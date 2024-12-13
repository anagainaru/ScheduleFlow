#  Copyright (c) 2019-2020 by the ScheduleFlow authors
#   All rights reserved.

#   This file is part of the ScheduleFlow package. ScheduleFlow is
#   distributed under a BSD 3-clause license. For details see the
#   LICENSE file in the top-level directory.

#   SPDX-License-Identifier: BSD-3-Clause

import logging
import os
import numpy as np
import _intScheduleFlow
from _intScheduleFlow import JobChangeType
from enum import IntEnum


class SchedulingPriorityPolicy(IntEnum):
    ''' Enumeration class to hold the policy types 
    for scheduling '''

    LJF = 0
    SJF = 1
    FCFS = 2

class SchedulingBackfillPolicy(IntEnum):
    ''' Enumeration class to hold the policy types
    for scheduling '''

    Easy = 0
    Conservative = 1


class Simulator():
    ''' Main class of the simulation '''

    def __init__(self, loops=1, generate_gif=False, check_correctness=False,
                 output_file_handler=None):
        ''' Constructor defining the main properties of a simulation '''

        assert (loops > 0), "Number of loops has to be a positive integer"

        self.__loops = loops
        self.__generate_gif = generate_gif
        self.__check_correctness = check_correctness
        self.__execution_log = {}
        self.job_list = []
        self.logger = logging.getLogger(__name__)

        self.__fp = output_file_handler

        if generate_gif:
            self.horizontal_ax = -1
            if self.__loops != 1:
                self.logger.warning("Number of loops in the Simulator "
                                    "needs to be 1 if the generate_gif "
                                    "option is True. Updated number of "
                                    "loops to 1.")
            self.__loops = 1

        if "ScheduleFlow_PATH" not in os.environ:
            os.environ["ScheduleFlow_PATH"] = "."

    def __str__(self):
        return 'Simulator(GIF: %s, Check_correctness: %s, Loops: %s' \
                ', Output: %s, Jobs: %d)' % (
                        self.__generate_gif, self.__check_correctness,
                        self.__loops, self.__fp, len(self.job_list))

    def __repr__(self):
        return 'Simulator(Loops: %s, Output: %s, Jobs: %d)' % (
                        self.__loops, self.__fp, len(self.job_list))

    def run_scenario(self, scheduler, job_list, scenario_name="ScheduleFlow",
                     metrics=["all"]):
        ''' Method for directly runnning a scenario (includes creating
        the scenario and calling the run method'''

        assert (len(job_list) > 0), "The job list cannot be empty"
        self.create_scenario(scheduler, job_list=job_list,
                             scenario_name=scenario_name)
        return self.run(metrics=metrics)

    def create_scenario(self, scheduler, job_list=[],
                        scenario_name="ScheduleFlow"):
        ''' Method for setting the properties of the current scenario '''

        self.__scheduler = scheduler
        self.__system = scheduler.system
        if len(job_list) > 0:
            self.job_list = []
        self.__execution_log = {}
        self.__scenario_name = scenario_name

        self.stats = _intScheduleFlow.StatsEngine(
                self.__system.get_total_nodes())
        if self.__generate_gif:
            self.__viz_handler = _intScheduleFlow.VizualizationEngine(
                    self.__system.get_total_nodes())

        return self.add_applications(job_list)

    def get_execution_log(self):
        ''' Method that returns the  a copy of the execution log.
        The log is a dictionary, where log[job] = list of (start, end)
        for each running instance '''
        return self.__execution_log

    def add_application(self, job):
        ''' Method for adding one additional application to the simulation '''

        if job in self.job_list:
            self.logger.warning("Job %s is already included "
                                "in the simulation. Skipping." % (job))
            return job.job_id
        if job.job_id == -1 or job.job_id in self.job_list:
            newid = 0
            if len(self.job_list) > 0:
                newid = max([j.job_id for j in self.job_list]) + 1
            job.job_id = newid
        self.job_list.append(job)
        return job.job_id

    def add_applications(self, job_list):
        ''' Method for sending additional applications to the simulation '''

        for new_job in job_list:
            add_application(new_job)
        return len(self.job_list)

    def __sanity_check_job_execution(self, execution_list, job):
        ''' Sanity checks for one application's execution log '''

        # The execution list: [(st, end)]
        # check that first start is after the submission time
        if execution_list[0][0] < job.submission_time:
            return False
        requested_time = job.get_total_request_time(0)
        expected_time = 0
        for i in range(len(execution_list)-1):
            # check that resubmissions start after end of previous
            if execution_list[i][1] > execution_list[i + 1][0]:
                return False
            # check len of failed executions
            start = execution_list[i][0]
            end = execution_list[i][1]
            if not np.isclose(end-start, requested_time,
                              rtol=1e-3):
                return False
            if job.get_checkpoint_size(i) > 0:
                expected_time += job.get_request_time(i)
            requested_time = job.get_total_request_time(i + 1)

        # check len of last execution
        start = execution_list[len(execution_list)-1][0]
        end = execution_list[len(execution_list)-1][1]
        walltime_left = job.walltime - expected_time
        if job.get_request_time(len(execution_list) - 1) >= walltime_left:
            # if run was successful, exected time is the job walltime
            expected_time = walltime_left
            # in case the last submission was checkpointed, the expected
            # time must also include the checkpoint read time
            expected_time += job.get_checkpoint_read_time(
                step=len(execution_list) - 1)
        else:
            expected_time = requested_time
        if not np.isclose(end-start, expected_time,
                          rtol=1e-3):
            return False
        return True

    def __sainity_check_schedule(self, workload):
        ''' Basic sanity checks for a complete schedule '''

        check_fail = 0
        # check that scheduled applications do not exceed system size
        # only check executions and not reservations (backfill)
        event_list = []
        for job in workload:
            event_list += [i[0] for i in workload[job]]
            event_list += [i[1] for i in workload[job]]
        event_list = list(set(event_list))
        event_list.sort()

        for i in range(len(event_list) - 1):
            start = event_list[i]
            end = event_list[i + 1]
            # find all jobs running between event i and i + 1
            procs = 0
            for job in workload:
                run_jobs = len([1 for run in workload[job]
                                if run[0] <= start
                                and run[1] >= end])
                if run_jobs > 0:
                    procs += job.nodes

            if procs > self.__system.get_total_nodes():
                check_fail += 1
        return check_fail

    def test_correctness(self, execution_log=[]):
        ''' Method for checking the correctness of the execution of a
        given list of jobs. Job list contains the jobs with their initial
        information, workload contains execution information for each
        job '''
        if len(execution_log) == 0:
            execution_log = self.__execution_log

        if len(execution_log) == 0:
            self.logger.warning("Testing correctness on an empty execution log")
            return 0

        check_fail = 0
        for job in execution_log:
            pass_check = self.__sanity_check_job_execution(
                execution_log[job], job)
            if not pass_check:
                self.logger.error("%s did not pass the sanity check: %s" %
                                  (job, execution_log[job]))
                check_fail += 1

        schedule_fail = self.__sainity_check_schedule(execution_log)
        if schedule_fail > 0:
            self.logger.error("Full schedule did not pass sanity check")
        check_fail += schedule_fail
        return check_fail

    def get_stats_metrics(self, metrics, execution_log=[]):
        ''' Method for returning  values for the given set of metrics
        on the current execution log or on a different provided log'''
        if len(execution_log) == 0:
            execution_log = self.__execution_log
        assert (len(execution_log) > 0), \
            "ERR - Trying to test correctness on an empty execution log"

        stats = _intScheduleFlow.StatsEngine(
                self.__system.get_total_nodes())
        stats.set_execution_output(execution_log)
        stats.set_metrics(metrics)
        return stats.get_metric_values()

    def run(self, metrics=["all"], simulation_duration=-1):
        ''' Main method of the simulator that triggers the start of
        a given simulation scenario '''

        assert (len(self.job_list) > 0), "Cannot run an empty scenario"
        check = 0
        average_stats = {}
        for i in range(self.__loops):
            runtime = _intScheduleFlow.Runtime(
                    self.job_list, logger=self.logger,
                    simulation_duration=simulation_duration)
            runtime(self.__scheduler)

            self.__execution_log = runtime.get_stats()
            # if the simulation was stopped before it finished
            if simulation_duration > 0:
                # remove the jobs that did not finish
                # i.e. have end ts equal to -1
                execution_log = {}
                for job in self.__execution_log:
                    valid_runs = [run for run in self.__execution_log[job]
                                  if run[1] != -1]
                    if len(valid_runs) > 0:
                        execution_log[job] = valid_runs
                self.__execution_log = execution_log

            if self.__check_correctness:
                check_loop = self.test_correctness()
                check += check_loop
                if check_loop > 0:
                    self.logger.debug("FAIL correctness test (loop %d)" % (i))
                    continue

            self.stats.set_execution_output(self.__execution_log)
            self.stats.set_metrics(metrics)
            self.logger.info("Stats in loop %d: %s" %(i, self.stats))
            if self.__fp is not None:
                self.stats.print_to_file(self.__fp, self.__scenario_name, i)
            if len(average_stats) == 0:
                average_stats = self.stats.get_metric_values()
            else:
                tmp = self.stats.get_metric_values()
                average_stats = {i: average_stats[i] + tmp[i]
                                 for i in average_stats}

        if check == 0:
            self.logger.info("PASS correctness test")

        if self.__generate_gif and check == 0:
            if self.horizontal_ax != -1:
                self.__viz_handler.set_horizontal_ax_limit(
                    self.horizontal_ax)
            self.__viz_handler.set_execution_log(self.__execution_log)
            self.horizontal_ax = self.__viz_handler.generate_scenario_gif(
                self.__scenario_name)
            self.logger.info(r"GIF generated draw/%s" % (
                self.__scenario_name))

        if metrics == "execution_log":
            return self.__execution_log
        return {i: average_stats[i]/self.__loops for i in average_stats}


class Application(object):
    ''' Class containing the properties of an application and
    all its running instances '''

    def __init__(self, nodes, submission_time, walltime,
                 requested_walltimes, priority=0, resubmit_factor=-1,
                 name="NoName"):
        ''' Constructor method takes the number of nodes required by the job,
        the submission time, the actual walltime, the requested walltime, a
        job id and a sequence of request times in case the job fails '''

        assert (walltime > 0),\
            'Application walltime must be positive: received %3.1f' % (
            walltime)
        assert (len(requested_walltimes) > 0),\
            'Request time sequence cannot be empty'
        assert (all(i > 0 for i in requested_walltimes)),\
            'Job requested walltime must be > 0 : received %s' % (
                    requested_walltimes)
        assert (submission_time >= 0),\
            'Job submission time must be > 0 : received %3.1f' % (
            submission_time)
        assert (nodes > 0),\
            'Number of nodes for a job must be > 0 : received %d' % (
            nodes)

        self.name = name
        self.nodes = nodes
        self.submission_time = submission_time
        self.walltime = walltime
        self.request_walltime = requested_walltimes[0]
        self.job_id = -1
        self.request_sequence = requested_walltimes
        self.priority = priority
        # keep track of the number of submission
        self.submission_count = 0

        self.resubmit = True
        if resubmit_factor == -1:
            if len(self.request_sequence) == 0:
                self.resubmit = False
            self.resubmit_factor = 0
        else:
            assert (resubmit_factor > 0),\
                'Increase factor for an execution request time must be ' \
                'over 0: received %d' % (resubmit_factor)
            self.resubmit_factor = resubmit_factor

        # entries in the execution log: (JobChangeType, old_value)
        self.__execution_log = []

        # by default checkpointing is False
        self.checkpointing = False
        self.current_checkpoint = 0
        self.checkpoint_sequence = []
        # by default the job is not running on any system
        self.system = None

    def __str__(self):
        return 'Job(%s, Nodes: %d, Submission: %3.1f, Walltime: %3.1f' \
               ', Request: %3.1f, Priority %d)' % (
                       self.name, self.nodes, self.submission_time,
                       self.walltime, self.request_walltime, self.priority)

    def __repr__(self):
        return 'Job(%s:%d, pr %d)' %(self.name, self.job_id, self.priority)

    def __lt__(self, job):
        return self.job_id < job.job_id

    def assign_system(self, system):
        self.system = system

    def set_checkpointing(self, checkpoint_size_list):
        ''' Method for setting the checkpoint size for each submission
            Entries with 0 or negative values indicate no checkpoint
            for the respective submission. Submissions after the entries
            in the list will use the last value '''

        assert(len(checkpoint_size_list) > 0),\
            "Cannot set an empty checkpoint list"
        self.checkpointing = True
        # sequences containing negative values on the last position indicate
        # not to checkpoint for future resubmissions
        self.current_checkpoint = checkpoint_size_list[0]
        self.checkpoint_sequence = checkpoint_size_list

    def get_checkpoint_size(self, step):
        ''' Method for descoverying the checkpoint size that the job
        will use for its consecutive "step"-th submission. '''

        if step < 0 or len(self.checkpoint_sequence) == 0:
            return 0
        if step < len(self.checkpoint_sequence):
            return self.checkpoint_sequence[step]
        return self.checkpoint_sequence[-1]

    def get_checkpoint_read_time(self, system=None, step=None):
        ''' Method that returns the time to read the previous checkpoint '''
        if not self.checkpointing:
            return 0

        sel_system = system
        if sel_system is None:
            sel_system = self.system
        assert(sel_system is not None),\
            "Job must be running on a system to compute the request time"

        if step is None:
            step = self.submission_count
        read_checkpoint = 0
        if step > 0:
            # take the last checkpoint size (that is not negative)
            prev_check = [self.get_checkpoint_size(i) for i in range(step)
                          if self.get_checkpoint_size(i) > 0]
            if len(prev_check) > 0:
                read_checkpoint = sel_system.get_read_time(prev_check[-1])
        return read_checkpoint

    def get_current_total_request_time(self, system=None):
        ''' Method for getting the value for the current request time
        for a given system including the time to checkpoint at the end
        (if it's required) and the time to read the latest checkpoint
        (if necessary) '''

        if not self.checkpointing:
            return self.request_walltime

        sel_system = system
        if sel_system is None:
            sel_system = self.system
        assert(sel_system is not None),\
            "Job must be running on a system to compute the request time"

        write_checkpoint = sel_system.get_write_time(
            self.current_checkpoint)
        read_checkpoint = self.get_checkpoint_read_time(system=sel_system)
        return read_checkpoint + write_checkpoint + self.request_walltime

    def get_request_time(self, step):
        ''' Method for descovering the request time that the job will use
        for its consecutive "step"-th submission. First submission will use
        the provided request time. Following submissions will either use the
        values provided in the request sequence or will increase the last
        value by the job resubmission factor. Method used to compute stats'''

        assert (step >= 0), "Cannot compute the resuest time for step < 0"

        if step < len(self.request_sequence):
            return self.request_sequence[step]

        # if the resubmit factor is not set, there are no more submissions
        if self.resubmit_factor == 0:
            return -1

        seq_len = len(self.request_sequence)
        return self.request_sequence[-1] * pow(
            self.resubmit_factor, step - seq_len + 1)

    def get_total_request_time(self, step):
        ''' Method for getting the value for the request time used on
        the "stept"-th submission on a given system including the time
        to checkpoint at the end (if it's required) and the time to read
        the latest checkpoint (if necessary) '''

        if not self.checkpointing:
            return self.get_request_time(step)

        assert(self.system is not None),\
            "Job must be running on a system to compute the request time"
        request_walltime = self.get_request_time(step)
        write_checkpoint = self.system.get_write_time(
            self.get_checkpoint_size(step))
        read_checkpoint = 0
        if step > 0:
            read_checkpoint = self.get_checkpoint_read_time(step=step)
        return read_checkpoint + write_checkpoint + request_walltime

    def overwrite_request_sequence(self, request_sequence):
        ''' Method for overwriting the sequence of future walltime
        requests '''
        self.request_sequence = request_sequence

    def update_submission(self, submission_time):
        ''' Method to update submission information including
        the submission time, the walltime request and the checkpoint size '''

        assert (submission_time >= 0),\
            r'Negative submission time received: %d' % (
            submission_time)
        self.__execution_log.append((JobChangeType.SubmissionChange,
                                     self.submission_time))
        self.__execution_log.append((JobChangeType.WalltimeChange,
                                     self.walltime))
        self.submission_count += 1
        # update submission time
        self.submission_time = submission_time

        old_request_gap = self.walltime - self.request_walltime
        # in case of a checkpoint, update walltime
        if self.current_checkpoint > 0:
            self.walltime = self.walltime - self.request_walltime
        # update the requested time
        self.request_walltime = self.get_request_time(
            self.submission_count)
        assert (old_request_gap > (self.walltime - self.request_walltime)),\
            "The new request walltime has to be greater than the last one"

        if self.resubmit_factor == 0:
            if self.submission_count >= len(self.request_sequence) - 1:
                self.resubmit = False
        # update the checkpointing
        if self.checkpointing:
            self.current_checkpoint = self.get_checkpoint_size(
                self.submission_count)

    def restore_default_values(self):
        ''' Method for restoring the initial submission values '''
        # submission count goes back to 0
        self.submission_count = 0

        # restore submission and walltime
        restore = next((i for i in self.__execution_log if
                        i[0] == JobChangeType.SubmissionChange), None)
        if restore is not None:
            self.submission_time = restore[1]
        restore = next((i for i in self.__execution_log if
                        i[0] == JobChangeType.WalltimeChange), None)
        if restore is not None:
            self.walltime = restore[1]

        # restore the sequence of request times
        self.request_walltime = self.get_request_time(0)

        # restore first checkpoint
        self.current_checkpoint = self.get_checkpoint_size(0)

        self.resubmit = True
        if self.resubmit_factor == 0 and len(self.request_sequence) == 0:
            self.resubmit = False

        # clear the execution log
        self.__execution_log = []


class System(object):
    ''' System class containing available resources
        (default I/O bandwidth per core of 1 MB/s for both read/write) '''

    def __init__(self, total_nodes, io_write_bw=1, io_read_bw=1):
        assert (total_nodes > 0),\
            r'Number of nodes of a system must be > 0: received %d' % (
            total_nodes)

        self.__total_nodes = total_nodes
        self.__free_nodes = total_nodes
        self.__IO_write_bw = io_write_bw
        self.__IO_read_bw = io_read_bw

    def __str__(self):
        return 'System(Nodes: %d, Free: %d)' % (
            self.__total_nodes, self.__free_nodes)

    def __repr__(self):
        return 'System(Nodes: %d, Free: %d)' % (
            self.__total_nodes, self.__free_nodes)

    def get_free_nodes(self):
        return self.__free_nodes

    def get_total_nodes(self):
        return self.__total_nodes

    def get_write_time(self, dump_size):
        ''' Method for returning the write time for data of size dump_size '''
        if dump_size <= 0:
            return 0
        return int(dump_size / self.__IO_write_bw)

    def get_read_time(self, dump_size):
        ''' Method for returning the read time for data of size dump_size '''
        if dump_size <= 0:
            return 0
        return int(dump_size / self.__IO_read_bw)

    def start_job(self, nodes, jobid):
        ''' Method for aquiring resources in the system '''

        self.__free_nodes -= nodes
        assert (self.__free_nodes >= 0),\
            r'Not enough free nodes for the allocation of job %d' % (jobid)

    def end_job(self, nodes, jobid):
        ''' Method for releasing nodes in the system '''

        self.__free_nodes += nodes
        assert (self.__free_nodes <= self.__total_nodes),\
            r'Free more nodes than total system during end of job %d' % (
            jobid)


class Scheduler(object):
    ''' Class that implements the scheduler functionality (i.e. choosing
        what are the jobs scheduled to run and backfillinf jobs) '''

    def __init__(self, system, logger=None,
                 priority_policy=SchedulingPriorityPolicy.FCFS,
                 backfill_policy=SchedulingBackfillPolicy.Easy):
        self.system = system
        self.wait_list = set()
        self.running_jobs = {}
        self.scheduled_jobs = {}
        self.logger = logger or logging.getLogger(__name__)
        self.gaps_in_schedule = _intScheduleFlow.ScheduleGaps(
            system.get_total_nodes())
        self.priority_policy = priority_policy
        self.backfill_policy = backfill_policy

    def __str__(self):
        return "Scheduler(%d waiting; %d scheduled; %d running)" % (
            len(self.wait_list), len(self.scheduled_jobs),
            len(self.running_jobs))

    def __repr__(self):
        return 'Scheduler(Total jobs: %d; Running: %d)' % (
            len(self.running_jobs) + len(self.wait_list) +
            len(self.scheduled_jobs), len(self.running_jobs))

    def __sort_job_list(self, job_list):
        sort_list = [job for job in job_list]
        if self.priority_policy == SchedulingPriorityPolicy.FCFS:
            # sort based on priority, submission and job_id (ascending)
            sort_list = sorted(sort_list, key=lambda job:
                               (job.priority, job.submission_time,
                               job.job_id))
        elif self.priority_policy == SchedulingPriorityPolicy.LJF:
            # sort based on priority, volume (descending) and job_id
            sort_list = sorted(sort_list, key=lambda job:
                              (job.priority,
                               -job.nodes * job.get_current_total_request_time(),
                               job.job_id))
        elif self.priority_policy == SchedulingPriorityPolicy.SJF:
            # sort based on priority, volume and job_id (ascending)
            sort_list = sorted(sort_list, key=lambda job:
                              (job.priority,
                               job.nodes * job.get_current_total_request_time(),
                               job.job_id))
        return sort_list

    def __fit_in_schedule(self, job, current_schedule, current_time):
        # first, look inside the schedule for a spot
        start_time = max(current_time, job.submission_time)
        request_walltime = job.get_current_total_request_time()
        gap_list = current_schedule.get_gaps(
                start_time, request_walltime, job.nodes)
        if len(gap_list) > 0: # if one is found return it
            return max(gap_list[0][0], start_time)

       # check for the end of the schedule for a fit (the earliest
       # job end between all jobs at the end of the schedule)
        (start_last_gap, end_last_gap) = current_schedule.get_ending_gap(current_time)
        gap_list = current_schedule.get_gaps(
                max(job.submission_time, start_last_gap), 0, job.nodes)
        if len(gap_list) > 0:
            # return the earliest time found
            return min([gap[0] for gap in gap_list])
        return end_last_gap

    def __get_scheduled_jobs(self):
        active_jobs = {}
        for job in self.scheduled_jobs:
            active_jobs[job] = self.scheduled_jobs[job]
        for job in self.running_jobs:
            active_jobs[job] = self.running_jobs[job]
        return active_jobs

    def __create_curent_schedule(self):
        # start with an empty schedule
        current_schedule = _intScheduleFlow.ScheduleGaps(
            self.system.get_total_nodes())
        # get all the active jobs (scheduled and running)
        active_jobs = self.__get_scheduled_jobs()
        # add active jobs to the schedule
        current_schedule.add(active_jobs)
        return current_schedule

    def __update_schedule(self, current_time):
        ''' Update the start time for the scheduling jobs based
        on where jobs actually finished '''
        # create current schedule based only on running jobs
        current_schedule = _intScheduleFlow.ScheduleGaps(
            self.system.get_total_nodes())
        current_schedule.add(self.running_jobs)
        # start all jobs that can run at the current time
        start_jobs = []
        # reschedule all the jobs in the scheduled jobs
        # sort scheduled list based on policy
        job_list = self.__sort_job_list(self.scheduled_jobs)
        for job in job_list:
            ts = self.__fit_in_schedule(
                    job, current_schedule, current_time)
            if ts < self.scheduled_jobs[job]:
                # update time in the scheduled list
                self.scheduled_jobs[job] = ts
                current_schedule.add({job: ts})
            if ts == current_time:
                start_jobs.append((ts, job))
        return start_jobs

    def trigger_schedule(self, current_time):
        # create current schedule (ScheduleGaps object)
        current_schedule = self.__create_curent_schedule()
        # sort wait list based on policy
        job_list = self.__sort_job_list(self.wait_list)
        # keep track of all jobs ready to be executed
        start_list = []
        del_list = set()
        for job in job_list:
            # find earliest start time in the schedule
            ts = self.__fit_in_schedule(
                    job, current_schedule, current_time)
            # if the earliest start is the current time
            if ts == current_time:
                # mark job ready for execution
                start_list.append((ts, job))
                self.logger.debug(
                        "Found placement for job %s: %d" %(job, ts))
                # move from wait time to scheduled list
                self.scheduled_jobs[job] = ts
                del_list.add(job)
                # add job to the schedule
                current_schedule.add({job: ts})
            else:
                # if all jobs scheduled for execution will
                # be started now
                scheduled_start = max([self.scheduled_jobs[j]
                                       for j in self.scheduled_jobs])
                if scheduled_start == current_time:
                    # schedule the current job but do not mark it for start
                    self.scheduled_jobs[job] = ts
                    del_list.add(job)
                    current_schedule.add({job: ts})
                elif self.backfill_policy == SchedulingBackfillPolicy.Conservative:
                    # if conservativeBF we add the job to the schedule
                    # regardless if we execute the job now or not
                    current_schedule.add({job: ts})
        for job in del_list:
            self.wait_list.remove(job)
        return start_list

    def submit_job(self, current_time, job_list):
        ''' Mark as ready for execution the jobs that can be scheduled
         for running at the current time and save the rest in the
         waiting queue '''
        for job in job_list:
            assert (job.nodes <= self.system.get_total_nodes()),\
                "Submitted jobs cannot ask for more nodes that the system \
                 capacity"
            self.wait_list.add(job)
        return self.trigger_schedule(current_time)

    def stop_job(self, current_time, job_list):
        ''' Stop all jobs in the list, update the schedule based on
        the end time of each job and trigger a new schedule '''
        for job in job_list:
            # remove the job from the running list
            del self.running_jobs[job]
        # if the walltime for one job was less than the requested time
        # update the current schedule and trigger a new schedule to
        # cover the wholes created
        action_list = self.__update_schedule(current_time)
        action_list.extend(self.trigger_schedule(current_time))
        return action_list

    def start_job(self, current_time, job_list):
        ''' Move jobs from the scheduled list to running, create an
        end event for each started job and trigger a new schedule '''
        end_jobs = []
        for job in job_list:
            # move job from scheduled list to running
            self.running_jobs[job] = current_time
            del self.scheduled_jobs[job]
            # mark its ending event
            end_jobs.append((-1, job))
            # assign job to system
            job.assign_system(self.system)
        # trigger new schedule
        start_jobs = self.trigger_schedule(current_time)
        end_jobs.extend(start_jobs)
        return end_jobs
