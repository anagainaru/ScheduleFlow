#  Copyright (c) 2019-2020 by the Cabana authors
#   All rights reserved.

#   This file is part of the ScheduleFlow package. ScheduleFlow is
#   distributed under a BSD 3-clause license. For details see the
#   LICENSE file in the top-level directory.

#   SPDX-License-Identifier: BSD-3-Clause

import subprocess
import logging
import heapq
import os
from distutils.spawn import find_executable
from enum import IntEnum
import sys
if sys.version_info[0] < 3:
    import collections as col
else:
    import collections.abc as col


class JobChangeType(IntEnum):
    ''' Enumeration class to hold all the types of changes
    that can be applied to an Application properties '''

    SubmissionChange = 0
    RequestChange = 1
    RequestSequenceOverwrite = 2


class EventType(IntEnum):
    ''' Enumeration class to hold event types (distinct values are required)
    The values give the order in which the simulator parses the events
    (e.g. JobEnd has the higher priority and will be evaluated first) '''

    JobSubmission = 2
    JobStart = 1
    JobEnd = 0
    TriggerSchedule = 3


class EventQueue(object):
    ''' Class for storing the events used by the simulator '''

    def __init__(self):
        self.heap = []

    def __str__(self):
        return ' '.join([str(i) for i in self.heap])

    def size(self):
        return len(self.heap)

    def empty(self):
        return self.size() == 0

    def push(self, item):
        ''' The items that can be pushed in the EventQueue must be tuples
        of the form (timestamp, event) '''

        assert (isinstance(item, col.Sequence)
                ), 'EventQueue works only on tuples (time, values)'
        heapq.heappush(self.heap, item)

    def pop(self):
        return heapq.heappop(self.heap)

    def top(self):
        return self.heap[0]

    def pop_list(self):
        ''' Method for extracting all items in the queue that share
        the lowest timestamp '''

        if self.empty():
            raise IndexError()
        obj_list = [self.pop()]
        while not self.empty() and self.top()[0] == obj_list[0][0]:
            obj_list.append(self.pop())
        return obj_list


class Runtime(object):
    ''' Runtime class responsible for coordinating the submission and
    execution process for all the jobs in a workload '''

    def __init__(self, workload, logger=None):
        ''' Constructor method creates the job submission events for all
        jobs in the workload. It also requires a default facor value for
        increasing the request time of failed jobs (in case they do not
        contain a sequence of request walltimes '''

        self.__current_time = 0
        self.__reserved_jobs = {}  # reserved_job[job] = time_to_start
        self.__finished_jobs = {}  # finish_jobs[job] = [(start, end)]
        self.__events = EventQueue()
        self.__logger = logger or logging.getLogger(__name__)

        # create submit_job events for all the applications in the list
        for job in workload:
            self.__events.push(
                (job.submission_time, EventType.JobSubmission, job))

    def __call__(self, sch):
        ''' Method for execution the simulation on a given scheduler '''

        self.scheduler = sch

        while not self.__events.empty():
            # get next set of events
            current_events = self.__events.pop_list()
            self.__current_time = current_events[0][0]

            self.__logger.debug(r'[Timestamp %d] Receive events %s' % (
                self.__current_time, current_events))
            self.__logger.debug(r'[Timestamp %d] Reservations %s' % (
                self.__current_time, self.__reserved_jobs))

            trigger_schedule = -1
            for event in current_events:
                if event[1] == EventType.JobSubmission:
                    self.__job_subimssion_event(
                        event[2], EventType.TriggerSchedule not in [
                            i[1] for i in current_events])
                elif event[1] == EventType.JobStart:
                    self.__job_start_event(event[2])
                elif event[1] == EventType.JobEnd:
                    trigger_schedule = self.__job_end_event(event[2])
                elif event[1] == EventType.TriggerSchedule:
                    self.__trigger_schedule_event()

            # if there are no jobs reserved for execution and the current
            # events list does not include one, create a new schedule event
            if (len(self.__reserved_jobs) == 0 and EventType.TriggerSchedule
                    not in [i[1] for i in current_events]):
                self.__events.push((self.__current_time,
                                    EventType.TriggerSchedule,
                                    -1))
            elif trigger_schedule != -1:
                # a job end requests a new schedule
                self.__events.push((self.__current_time + trigger_schedule,
                                    EventType.TriggerSchedule, -1))

        # at the end of the simulation return default values for all the jobs
        for job in self.__finished_jobs:
            job.restore_default_values()

    def __job_subimssion_event(self, job, can_start):
        ''' Method for handling a job submission event. The method takes the
        job that is being submitted and if it is allowed to start it now
        inside an existing schedule '''

        tm = self.scheduler.fit_job_in_schedule(job, self.__reserved_jobs,
                                                self.__current_time)
        # check if the job can fit in the current reservations
        # if yes and if it is allowed, send it for execution
        if tm != -1 and can_start:
            self.__logger.debug(
                r'[Timestamp %d] Job submission %s fit at time %d' %
                (self.__current_time, job, tm))
            self.__reserved_jobs[job] = tm
            self.__events.push((tm, EventType.JobStart, job))
            return
        # if not submit it to the scheduler
        self.scheduler.submit_job(job)

    def __trigger_schedule_event(self):
        ''' Method for handling an event for triggering a new schedule. '''

        ret_schedule = self.scheduler.trigger_schedule()
        self.__logger.debug(r'[Timestamp %d] Trigger schedule %s' % (
            self.__current_time, ret_schedule))
        # create a start job event for each job selected by the scheduler
        for apl in ret_schedule:
            self.__reserved_jobs[apl[1]] = self.__current_time + apl[0]
            self.__events.push(
                (self.__current_time + apl[0], EventType.JobStart, apl[1]))

    def __job_end_event(self, job):
        ''' Method for handling a job end event '''

        self.__logger.info(r'[Timestamp %d] Stop job %s' % (
            self.__current_time, job))
        # check if the job finished successfully or it was a failure
        if job.walltime > job.request_walltime and job.resubmit:
            # resubmit failed job unless the job doesn't permit it
            job.update_submission(self.__current_time)
            self.__logger.debug(
                r'[Timestamp %d] Resubmit failed job %s' %
                (self.__current_time, job))
            self.__events.push((self.__current_time,
                                EventType.JobSubmission, job))

        # look for backfilling jobs if the reserved time > walltime
        elif job.walltime < job.request_walltime:
            backfill_schedule = self.scheduler.backfill_request(
                job, self.__reserved_jobs, self.__current_time)
            self.__logger.info(
                r'[Timestamp %d] Backfill for job %s; Reserved %s' %
                (self.__current_time, job,
                 self.__reserved_jobs))
            for apl in backfill_schedule:
                self.__reserved_jobs[apl[1]] = apl[0]
                self.__events.push((apl[0], EventType.JobStart, apl[1]))

        ret = self.scheduler.clear_job(job)
        self.__log_end(job)
        del self.__reserved_jobs[job]
        return ret

    def __job_start_event(self, job):
        ''' Method for handling a job start event '''

        self.__logger.info(r'[Timestamp %d] Start job %s' % (
            self.__current_time, job))
        self.scheduler.allocate_job(job)
        self.__log_start(job)
        # create a job end event for the started job for timestamp
        # current_time+execution_time
        execution = min(job.walltime, job.request_walltime)
        self.__events.push(
            (self.__current_time + execution, EventType.JobEnd, job))

    def __log_start(self, job):
        ''' Method for logging the information about a new job start '''

        if job not in self.__finished_jobs:
            self.__finished_jobs[job] = []
        self.__finished_jobs[job].append([self.__current_time, -1])

    def __log_end(self, job):
        ''' Method for logging the information about job end '''

        assert (job in self.__finished_jobs),\
            "Logging the end of a job that did not start"
        last_execution = len(self.__finished_jobs[job]) - 1
        self.__finished_jobs[job][last_execution][1] = self.__current_time

    def get_stats(self):
        ''' Method for returning the log containing every jon start and
        finish recorded during the simulation up to the current time '''

        return self.__finished_jobs


class TexGenerator():
    ''' Internal class used by the Visualization Engine to create the
    latex files that will be compiled into a GIF animation '''

    def __init__(self, execution_slices_list, execution_job_list,
                 scalex, scaley):
        ''' The constructor takes a division of the space into slices,
        the execution log for each job and the vertical and horizontal
        scale factors to fit the simulation to the figure size '''
        self.__scalex = scalex
        self.__scaley = scaley
        self.__slices = execution_slices_list
        self.__run_list = execution_job_list
        self.__total_runs = len(execution_job_list)

    def write_to_file(self, filename):
        ''' Method to create a file for each step of the simulation '''

        for i in range(self.__total_runs + 1):
            outf = open(os.environ["ScheduleFlow_PATH"]+'/draw/%s_%d.tex' % (
                filename, i), 'w')
            # write header
            outf.writelines([l for l in open(
                os.environ["ScheduleFlow_PATH"]+"/draw/tex_header").readlines()])
            self.__print_execution_list(i + 1, outf)
            if i < self.__total_runs:
                # write last job start and end times
                self.__print_current_execution_info(self.__run_list[i],
                                                    outf)
            else:
                # last step in the simulation
                self.__print_makespan(max([r[1] for r in self.__run_list]),
                                      outf)
            # write footer
            outf.writelines([l for l in open(
                os.environ["ScheduleFlow_PATH"]+"/draw/tex_footer").readlines()])
            outf.close()

    def __print_current_execution_info(self, execution, outf):
        ''' Method to plot the start time, duration and request time
        for the current step (showing a job instance) '''

        start = float(execution[0])
        end = float(execution[1]) - start
        request = float(execution[3])
        outf.write(r'\legend{17}{-0.5}{Start:\ %.1f}' % (start))
        outf.write('\n')
        outf.write(r'\legend{40}{-0.5}{Duration:\ %.1f}' % (end))
        outf.write('\n')
        outf.write(r'\legend{65}{-0.5}{Request:\ %.1f}' % (request))
        outf.write('\n')

    def __print_makespan(self, value, outf):
        ''' The last step of the simulation plots the total makespan
        instead of the job information plotting during each other step '''

        val = float(value)
        outf.write(r'\legend{%.1f}{-0.5}{%.1f}' % (val * self.__scalex, val))
        outf.write("\n")

    def __print_execution(self, execution, outf, last_frame):
        ''' Method for ploting a jobs execution represented by a rectagle.
        Yellow color represents a sucessfull execution, shades of orange
        consecutive failed instances '''

        start = float(execution[0]) * self.__scalex
        end = float(execution[1]) * self.__scalex
        procs = execution[2] * self.__scaley
        offset = execution[6] * self.__scaley
        job_id = execution[4]
        color = 2 * min(execution[5], 5)
        color_text = r"{rgb:red,%d;yellow,%d}" % (
            color, 10 - color)
        if last_frame and color != 0:
            color_text = "white"
            job_id = ' '
        if start != end:
            # walltime box
            outf.write(r'''\draw[-, thick,fill=%s] (%.1f,%d)
                       rectangle node{$\scriptstyle{%s}$} (%.1f, %d);
                        ''' % (color_text, start, offset,
                               job_id, end, offset + procs))
            outf.write("\n")

    def __print_reservation(self, execution, outf):
        ''' Method for plotting the dashed rectangle that shows the
        reserved time for a given execution '''

        start = float(execution[0]) * self.__scalex
        procs = execution[2] * self.__scaley
        offset = execution[6] * self.__scaley
        request = float(execution[3]) * self.__scalex
        if start < request:
            # requested walltime box
            outf.write(r'''\draw[-, thick, dashed] (%.1f,%d)
                       rectangle (%.1f,%d) ;''' % (
                start, offset, request, offset + procs))
            outf.write("\n")

    def __print_execution_list(self, step, outf):
        ''' Method for printing all job instances for a given step '''

        # check if it is the last frame
        last_frame = False
        if step == len(self.__slices) + 1:
            last_frame = True
            step = step - 1
        for i in range(step):
            execution_list = self.__slices[i]
            # print all slices of the current execution
            for execution in execution_list:
                self.__print_execution(execution, outf, last_frame)
            if not last_frame:
                self.__print_reservation(
                    execution_list[len(execution_list)-1], outf)


class VizualizationEngine():
    ''' Internal class responsible with creating the GIF animation '''

    def __init__(self, procs, execution_log=[], horizontal_ax_limit=0,
                 keep_intermediate_pdf=False):
        self.__scaley = 150 / procs
        self.__limitx = horizontal_ax_limit
        self.__execution_log = execution_log
        self.__keep_pdf = keep_intermediate_pdf
        self.__set_scalex(execution_log)

        # check if pdflatex and convert from ImageMagik are installed
        assert (find_executable('pdflatex')), \
            'Pdflatex needs to be installed to create GIFs'
        assert (find_executable('convert')), \
            'Convert from ImageMagik needs to be installed to create GIFs'

    def __set_scalex(self, execution_log):
        ''' Method for setting the scale for plotting the execution log
        on the given image size '''

        if len(execution_log) > 0:
            limitx = max([execution_log[job][len(execution_log[job]) - 1][1]
                          for job in execution_log])
            if limitx > self.__limitx:
                self.__limitx = limitx
            self.__scalex = 90 / self.__limitx

    def set_execution_log(self, execution_log):
        self.__execution_log = execution_log
        self.__set_scalex(execution_log)

    def set_horizontal_ax_limit(self, horizontal_ax_limit):
        ''' Method used to set the horizontal limit different than
        the end of the simulation '''

        self.__limitx = horizontal_ax_limit
        self.__scalex = 90/horizontal_ax_limit

    def generate_scenario_gif(self, name_scenario):
        ''' Method that generates the animation latex files, creates the
        PDF and calls convert from ImageMagik to convert the PDFs into a
        GIF file '''

        assert (len(self.__execution_log) > 0),\
            'ERR - Trying to create an animation for an empty execution log'

        self.__generate_animation_files(name_scenario)
        subprocess.call([
            os.environ["ScheduleFlow_PATH"]+"/draw/create_animation.sh",
            name_scenario,
            "delete"])
        return self.__limitx

    def __generate_animation_files(self, filename):
        ''' Generate a temp list of (start, end, procs,
        requested walltime, job_id, color) used to create
        the tex file '''

        run_list = []
        for job in self.__execution_log:
            run_list += self.__get_job_runs(self.__execution_log[job], job)
        run_list.sort()
        sliced_list = self.__get_sliced_list(run_list)

        tex_generator = TexGenerator(sliced_list, run_list,
                                     self.__scalex, self.__scaley)
        tex_generator.write_to_file(filename)

    def __find_running_jobs(self, run_list, start, end):
        ''' Given an execution log find all jobs that are included
        inside the schedule between start and end '''

        return [i for i in range(len(run_list)) if
                run_list[i][0] <= start and
                run_list[i][1] >= end]

    def __get_sliced_list(self, run_list):
        ''' Generate a list of (start, end, procs, request_end,
        job_id, failure_count, starty) for each job instance for
        each slice (a slice is a unit execution time not containing
        any job starts or ends)'''

        event_list = list(set([i[0] for i in run_list] +
                              [i[1] for i in run_list]))
        event_list.sort()
        sliced_list = [[] for i in run_list]
        for i in range(len(event_list)-1):
            idx_list = self.__find_running_jobs(
                run_list, event_list[i], event_list[i + 1])
            idx_list.sort()
            starty = 0
            for idx in idx_list:
                sliced_list[idx].append(
                    (event_list[i], event_list[i + 1],
                     run_list[idx][2], run_list[idx][3] +
                     run_list[idx][0], run_list[idx][4],
                     run_list[idx][5], starty))
                starty += run_list[idx][2]
        return sliced_list

    def __get_job_runs(self, execution_list, job):
        ''' Generate a list of (start, end, procs, request_time,
        job_id, failure_count) for each job instance run '''

        run_list = []
        requested_time = job.request_walltime
        for i in range(len(execution_list) - 1):
            # check failed executions
            start = execution_list[i][0]
            end = execution_list[i][1]
            run_list.append((start, end, job.nodes,
                             requested_time, job.job_id,
                             i + 1))
            requested_time = job.get_request_time(i + 1)

        # check succesful execution (last run)
        start = execution_list[len(execution_list) - 1][0]
        end = execution_list[len(execution_list) - 1][1]
        run_list.append((start, end, job.nodes,
                         requested_time, job.job_id, 0))
        return run_list


class StatsEngine():
    ''' Internal class used by the Simulator to generate the statistics
    related to a simulation (utilization, average makespan, etc) '''

    def __init__(self, total_nodes):
        self.__execution_log = {}
        self.__makespan = -1
        self.__total_nodes = total_nodes
        
        self.__metric_mapping = {
            "system makespan" : self.total_makespan,
            "system utilization" : self.system_utilization,
            "job utilization" : self.average_job_utilization,
            "job response time" : self.average_job_response_time,
            "job stretch" : self.average_job_stretch,
            "job wait time" : self.average_job_wait_time,
            "job failures" : self.total_failures}
        self.__metrics = [i for i in self.__metric_mapping]
        self.__metrics.sort()

    def __str__(self):
        if len(self.__execution_log) == 0:
            return 'Empty stats: Needs an execution log added'
        return 'Execution end %3.2f \nUtilization %3.2f \nAverage job ' \
               'utilization %3.2f \nAverage job response time %3.2f \n' \
               'Average job stretch %3.2f \nAverage wait time %3.2f\n' \
               'Average failures: %d' % (
                self.total_makespan(),
                self.system_utilization(),
                self.average_job_utilization(),
                self.average_job_response_time(),
                self.average_job_stretch(),
                self.average_job_wait_time(),
                self.total_failures())

    def set_execution_output(self, execution_log):
        ''' Add the execution log that will be used to generate stats '''

        assert (len(execution_log) > 0), "Simulation execution log is NULL"
        self.__execution_log = execution_log
        self.__makespan = max([max([i[1] for i in self.__execution_log[job]])
                               for job in self.__execution_log])
    
    def __add_metric_list(self, metric_name):
        self.__metrics |= set([metric for metric in self.__metric_mapping
                              if metric_name in metric])

    def set_metrics(self, metric_list):
        ''' Add the metrics of interest for the current simulation '''

        for metric in metric_list:
            if metric == "all":
                return self.__metrics
        self.__metrics = set()
        for metric in metric_list:
            if "all" in metric[:4]:
                self.__add_metric_list(metric[4:])
                continue
            if metric not in self.__metric_mapping:
                continue
            self.__metrics.add(metric)

        # set order is not deterministic when parsed
        self.__metrics = list(self.__metrics)
        self.__metrics.sort()
        return self.__metrics
    
    def total_makespan(self):
        ''' Time from simulation beginning last job end '''
        return self.__makespan

    def total_failures(self):
        ''' Total number of failures for all job instance runs '''
        total_failures = sum([len(self.__execution_log[job])-1 for job in
                              self.__execution_log])
        return total_failures

    def system_utilization(self):
        ''' The sum of execution time for successful runs multiplied by the
        processors used for each job divided by the simulation volume
        (makespan multiplied by number of nodes in the system) '''

        total_runtime = sum([job.walltime * job.nodes for job in
                             self.__execution_log])
        return total_runtime / (self.__makespan * self.__total_nodes)

    def average_job_wait_time(self):
        ''' Average time between submission and run for all instances '''

        total_wait = 0
        total_runs = 0
        for job in self.__execution_log:
            submission = job.submission_time
            apl_wait = 0
            for instance in self.__execution_log[job]:
                apl_wait += instance[0] - submission
                submission = instance[1]
            total_wait += apl_wait
            total_runs += len(self.__execution_log[job])
        return total_wait / max(1, total_runs)

    def average_job_utilization(self):
        ''' Average utilization of the machine for each job
        (ratio between time of successful run to the sum of all
        execution of every instance of the job)'''

        total = 0
        for job in self.__execution_log:
            apl_total = sum([self.__execution_log[job][i][1] -
                             self.__execution_log[job][i][0] for i
                             in range(len(self.__execution_log[job])-1)])
            request = job.get_request_time(
                          len(self.__execution_log[job]) - 1)
            apl_total = 1. * job.walltime / (apl_total + request)
            total += apl_total
        return total / max(1, len(self.__execution_log))

    def average_job_response_time(self):
        ''' Average time between last run and submission of jobs '''

        makespan = 0
        for job in self.__execution_log:
            runs = self.__execution_log[job]
            makespan += (runs[len(runs) - 1][1] - job.submission_time)
        return makespan / max(1, len(self.__execution_log))

    def average_job_stretch(self):
        ''' Average stretch for all jobs (ratio between response time
        and time of sucessful run)'''

        stretch = 0
        for job in self.__execution_log:
            runs = self.__execution_log[job]
            stretch += ((runs[len(runs) - 1][1] - job.submission_time) /
                        job.walltime)
        return stretch / max(1, len(self.__execution_log))

    def get_metric_values(self):
        if len(self.__execution_log) == 0:
            return {}
        ret = {}
        for metric in self.__metrics:
            ret[metric] = self.__metric_mapping[metric]()
        return ret

    def print_to_file(self, file_handler, scenario, loop_id):
        ''' Print all metrics to a file handler '''

        if len(self.__execution_log) == 0:
            return -1
        # if printing the first loop, print the header
        if loop_id == 0:
            file_handler.write("Scenario name : ")
            for metric in self.__metrics:
                file_handler.write("%s : " %(metric))
            file_handler.write("\n")

        # print metric values
        file_handler.write("%s : " %(scenario))
        for metric in self.__metrics:
            file_handler.write("%.2f : " %
            (self.__metric_mapping[metric]()))
        file_handler.write("\n")
