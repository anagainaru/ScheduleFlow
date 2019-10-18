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
        return 'Simulator: Generate gif %s, Check correctness %s, Loops %s' \
                ', Output file %s, Number of jobs %d' % (
                        self.__generate_gif, self.__check_correctness,
                        self.__loops, self.__fp, len(self.job_list))

    def __repr__(self):
        return 'Simulator(GIF: %s, Check_correctness: %s, Loops: %s' \
                ', Output: %s, Jobs: %d)' % (
                        self.__generate_gif, self.__check_correctness,
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

    def add_applications(self, job_list):
        ''' Method for sending additional applications to the simulation '''

        for new_job in job_list:
            if new_job in self.job_list:
                self.logger.warning("Job %s is already included "
                                    "in the sumlation." % (new_job))
                continue
            job_id_list = [job.job_id for job in self.job_list]
            if new_job.job_id == -1 or new_job.job_id in job_id_list:
                newid = len(self.job_list)
                if len(job_id_list) > newid:
                    newid = max(job_id_list) + 1
                new_job.job_id = newid
            self.job_list.append(new_job)
        return len(self.job_list)

    def __sanity_check_job_execution(self, execution_list, job):
        ''' Sanity checks for one application's execution log '''

        # The execution list: [(st, end)]
        # check that first start is after the submission time
        if execution_list[0][0] < job.submission_time:
            return False
        requested_time = job.request_walltime
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
            requested_time = job.get_request_time(i + 1)

        # check len of last execution
        start = execution_list[len(execution_list)-1][0]
        end = execution_list[len(execution_list)-1][1]
        expected_time = requested_time
        if end - start >= job.walltime:
            # if run was successful, exected time is the job walltime
            expected_time = job.walltime
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

        assert (len(execution_log) > 0), \
            "ERR - Trying to test correctness on an empty execution log"

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

    def run(self, metrics=["all"]):
        ''' Main method of the simulator that triggers the start of
        a given simulation scenario '''

        assert (len(self.job_list) > 0), "Cannot run an empty scenario"
        check = 0
        average_stats = {}
        for i in range(self.__loops):
            runtime = _intScheduleFlow.Runtime(self.job_list)
            runtime(self.__scheduler)
            self.__execution_log = runtime.get_stats()

            if self.__check_correctness:
                check_loop = self.test_correctness()
                check += check_loop
                if check_loop > 0:
                    self.logger.debug("FAIL correctness test (loop %d)" % (i))
                    continue

            self.stats.set_execution_output(self.__execution_log)
            self.stats.set_metrics(metrics)
            self.logger.info(self.stats)
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
        return {i: average_stats[i]/self.__loops for i in average_stats}


class Application(object):
    ''' Class containing the properties of an application and
    all its running instances '''

    def __init__(self, nodes, submission_time, walltime,
                 requested_walltimes, resubmit_factor=-1):
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
        assert (all(requested_walltimes[i] < requested_walltimes[i + 1]
                for i in range(len(requested_walltimes) - 1))),\
            'Request time sequence is not sorted in increasing order'

        self.nodes = nodes
        self.submission_time = submission_time
        self.walltime = walltime
        self.request_walltime = requested_walltimes[0]
        self.job_id = -1
        self.request_sequence = requested_walltimes[1:]
        self.resubmit = True
        if resubmit_factor == -1:
            if len(self.request_sequence) == 0:
                self.resubmit = False
            self.resubmit_factor = 1
        else:
            assert (resubmit_factor > 1),\
                'Increase factor for an execution request time must be ' \
                'over 1: received %d' % (resubmit_factor)
            self.resubmit_factor = resubmit_factor

        # Entries in the execution log: (JobChangeType, old_value)
        self.__execution_log = []
        if len(self.request_sequence) > 0:
            self.__execution_log.append(
                    (JobChangeType.RequestSequenceOverwrite,
                     self.request_sequence[:]))

        # By default checkpointing is False
        self.current_checkpoint = 0
        self.checkpoint_sequence = []

    def __str__(self):
        return 'Job %d: %d nodes; %3.1f submission time; %3.1f total ' \
               'execution time (%3.1f requested)' % (
                       self.job_id, self.nodes, self.submission_time,
                       self.walltime, self.request_walltime)

    def __repr__(self):
        return 'Job(Nodes: %d, Submission: %3.1f, Walltime: %3.1f, ' \
               'Request: %3.1f)' % (self.nodes,
                                    self.submission_time,
                                    self.walltime,
                                    self.request_walltime)

    def __lt__(self, job):
        return self.job_id < job.job_id

    def set_checkpointing(self, checkpoint_size_list):
        ''' Method for setting the checkpoint/restart characteristics:
            (1) what is the checkpoint size for each submission;
            (2) is each submission checkpointed at the end;
            (3) resubmit_factor resubmissions are checkpointed '''
        # 0 or negative values indicate no checkpoint
        self.current_checkpoint = checkpoint_size_list[0]
        if len(checkpoint_size_list)>1:
            self.checkpoint_sequence = checkpoint_size_list[1:]
        # once the checkpoint_sequnce becomes empty, resubmissions will be
        # always or never checkpointed based on self.checkpointing

    def get_checkpoint_size(self, step):
        ''' Method for descoverying the checkpoint size that the job
        will use for its consecutive "step"-th submission. '''

        if step == 0 or len(self.checkpoint_sequence) == 0:
            return self.current_checkpoint
        if step < len(self.checkpoint_sequence):
            return self.checkpoint_sequence[step - 1]
        return self.checkpoint_sequence[-1]

    def get_request_time(self, step):
        ''' Method for descovering the request time that the job will use
        for its consecutive "step"-th submission. First submission will use
        the provided request time. Following submissions will either use the
        values provided in the request sequence or will increase the last
        value by the job resubmission factor. '''

        if step == 0:
            return self.request_walltime
        if len(self.request_sequence) == 0:
            return self.request_walltime * pow(self.resubmit_factor, step)

        if step < len(self.request_sequence):
            return self.request_sequence[step - 1]

        seq_len = len(self.request_sequence)
        return self.request_sequence[seq_len - 1] * pow(
            self.resubmit_factor, step - seq_len)

    def overwrite_request_sequence(self, request_sequence):
        ''' Method for overwriting the sequence of future walltime
        requests '''

        assert (all(request_sequence[i] <
                request_sequence[i + 1] for i in
                range(len(request_sequence) - 1))),\
            r'Request time sequence is not sorted in increasing order'
        if len(request_sequence) > 0:
            assert (request_sequence[0] > self.request_walltime),\
                r'Request time sequence is not sorted in increasing order'
        self.request_sequence = request_sequence[:]
        self.__execution_log.append((JobChangeType.RequestSequenceOverwrite,
                                     self.request_sequence[:]))

    def update_submission(self, submission_time):
        ''' Method to update submission information including
        the submission time, the walltime request and the checkpoint size '''

        assert (submission_time >= 0),\
            r'Negative submission time received: %d' % (
            submission_time)
        self.__execution_log.append((JobChangeType.SubmissionChange,
                                     self.submission_time))
        self.__execution_log.append((JobChangeType.RequestChange,
                                     self.request_walltime))

        self.submission_time = submission_time
        if len(self.request_sequence) > 0:
            self.request_walltime = self.request_sequence[0]
            del self.request_sequence[0]
        else:
            self.request_walltime = int(self.resubmit_factor *
                                        self.request_walltime)
        if self.resubmit_factor == 1 and len(self.request_sequence) == 0:
            self.resubmit = False

        if len(self.checkpoint_sequence) > 0:
            self.__execution_log.append((JobChangeType.CheckpointSizeChange,
                                         self.current_checkpoint))
            self.current_checkpoint = self.checkpoint_sequence[0]
            del self.checkpoint_sequence[0]

    def restore_default_values(self):
        ''' Method for restoring the initial submission values '''
        restore = next((i for i in self.__execution_log if
                        i[0] == JobChangeType.SubmissionChange), None)
        if restore is not None:
            self.submission_time = restore[1]

        # restore the sequence of request times
        restore = next((i for i in self.__execution_log if
                        i[0] == JobChangeType.RequestSequenceOverwrite), None)
        if restore is not None:
            self.request_sequence = restore[1][:]
        
        restore = next((i for i in self.__execution_log if
                        i[0] == JobChangeType.RequestChange), None)
        if restore is not None:
            self.request_walltime = restore[1]

        # restore the sequence of checkpointing size
        restore = [i[1] for i in self.__execution_log if
                   i[0] == JobChangeType.CheckpointSizeChange]
        if len(restore) > 0:
            restore_check = []
            if len(restore) > 1:
                restore_check = restore[1:]
            restore_check += [self.current_checkpoint]
            self.current_checkpoint = restore[0]
            self.checkpoint_sequence = restore_check + self.checkpoint_sequence

        self.resubmit = True
        if self.resubmit_factor == 1 and len(self.request_sequence) == 0:
            self.resubmit = False

        # clear the execution log
        self.__execution_log = []
        if len(self.request_sequence) > 0:
            self.__execution_log.append(
                    (JobChangeType.RequestSequenceOverwrite,
                     self.request_sequence[:]))


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
        return 'System: %d total nodes (%d currently free)' % (
            self.__total_nodes, self.__free_nodes)

    def __repr__(self):
        return 'System(Nodes: %d, Free: %d)' % (
            self.__total_nodes, self.__free_nodes)

    def get_free_nodes(self):
        return self.__free_nodes

    def get_total_nodes(self):
        return self.__total_nodes

    def get_write_time(self, dump_size):
        return dump_size * self.__IO_write_bw

    def get_read_time(self, dump_size):
        return dump_size * self.__IO_read_bw

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
    ''' Base class that needs to be extended by all Scheduler classes '''

    def __init__(self, system, logger=None, total_queues=1):
        ''' Base construnction method that takes a System object '''
        self.system = system
        self.waiting_queue = _intScheduleFlow.WaitingQueue(
                total_queues=total_queues)
        self.running_jobs = set()
        self.logger = logger or logging.getLogger(__name__)
        self.gaps_in_schedule = _intScheduleFlow.ScheduleGaps(
            system.get_total_nodes())

    def __str__(self):
        return 'Scheduler: %s; %s; %d jobs running' % (
            self.system, self.waiting_queue, len(self.running_jobs))

    def __repr__(self):
        return 'Scheduler(Queued jobs: %d; Running: %d)' % (
            len(self.waiting_queue.total_jobs()),
            len(self.running_jobs))

    def submit_job(self, job):
        ''' Base method to add a job in the waiting queue '''

        assert (job.nodes <= self.system.get_total_nodes()),\
            "Submitted jobs cannot ask for more nodes that the system"
        self.waiting_queue.add(job)

    def allocate_job(self, job):
        ''' Base method for allocating the job for running on the system '''

        self.system.start_job(job.nodes, job.job_id)
        self.running_jobs.add(job)

    def clear_job(self, job):
        ''' Base method for clearing a job that was running in the system.
        The method returns whether the scheduler requires a new scheduling
        cycle for chosing new jobs after this job end: -1 means no trigger
        is necessary; otherwise the relative timestamp is returned (e.g. a
        timestamp of 4 means trigger a schedule at current_timestamp + 4 '''

        assert (job in self.running_jobs),\
            r'Scheduler trying to stop a job that was not running %d' % (
            job.job_id)

        # clear system nodes, remove job from running queue
        self.system.end_job(job.nodes, job.job_id)
        self.running_jobs.remove(job)
        return -1

    def trigger_schedule(self, current_time):
        ''' Base method for triggering schedules guarantees the simulator
        will not crash if the child Schedulers do not implement it. The child
        methods implement different algorithms for choosing when/what jobs
        to run from the waiting queue at the current schedule cycle '''
        return []

    def fit_job_in_schedule(self, job, reserved_jobs, current_time=0):
        ''' Base method that fits a new job into an existing schedule.
        The `reserved_jobs` consists of a list of [start time, job].
        The base method assumes a reservation based scheduler: the end of
        the last job represents the end of the reservation window. All
        jobs will be fit into this strict window (the new job cannot exceed
        the end of the window)
        The method returns -1 if the job does not fit into the schedule
        and timestamp otherwise '''

        if len(reserved_jobs) == 0:
            return -1
        start_time = max(job.submission_time, current_time)
        gap_list = self.gaps_in_schedule.get_gaps(start_time,
                                                  job.request_walltime,
                                                  job.nodes)
        self.logger.debug(
            r'[Scheduler] Reservation list: %s; Gaps: %s' % (
                reserved_jobs, gap_list))
        if len(gap_list) == 0:
            return -1

        ts = max(gap_list[0][0], start_time)
        # there is room for the current job starting with ts
        self.logger.info(r'[Scheduler] Found space for %s: timestamp %d' % (
            job, ts))
        # update the gaps
        self.gaps_in_schedule.add({job: ts})
        return ts

    def backfill_request(self, stop_job, reserved_jobs, min_ts):
        ''' Base method for requesting a backfill phase. By default the
        base method does not use any backfilling.
        The child methods implement different algorithms for choosing
        jobs to use to fill the space left by the termination of stop_job '''

        self.gaps_in_schedule.remove({stop_job: reserved_jobs[stop_job]})
        return []


class BatchScheduler(Scheduler):
    ''' Reservation based scheduler (default LJF batch scheduler) '''

    def __init__(self, system, batch_size=100, total_queues=1, logger=None):
        ''' Constructor method extends the base to specify the batch size,
        i.e. number of jobs in the wait queue to examime to create the
        reservation. Jobs in the waiting queue are ordered based on their
        submission time '''

        super(BatchScheduler, self).__init__(system, logger,
                                             total_queues=total_queues)
        self.batch_size = batch_size

    def __str__(self):
        return "Batch "+super(BatchScheduler, self).__str__()

    def __repr__(self):
        return "Batch "+super(BatchScheduler, self).__repr__()

    def __get_jobs(self, queue, batch_size):
        ''' Method that returns the first batch_size jobs (by submission)
        in the queue ordered by their volume '''

        if len(queue) == 0:
            return []
        # get jobs in the queue ordered by their submission times
        batch_list = sorted(queue, key=lambda job: job.submission_time)
        # ger all jobs that share submission times that could be included
        # in the current batch
        max_submission = max([job.submission_time for job in
                              batch_list[:batch_size]])
        batch_list = [job for job in batch_list if
                      job.submission_time <= max_submission]

        # sort the list by the size of the job (nodes*request_walltime)
        batch_sorted = sorted(batch_list, key=lambda job:
                              job.nodes * job.request_walltime, reverse=True)
        # return the first batch_size entries
        return batch_sorted[:batch_size]

    def get_batch_jobs(self):
        ''' Method that returns the first batch_size jobs (by submission)
        in the main waiting queue ordered by their volume. If the waiting
        queue jobs from secondary queues will be updated to the main one '''

        # make sure the wait queue is empty only if there are no more jobs
        self.waiting_queue.fill_priority_queue()
        return self.__get_jobs(self.waiting_queue.get_priority_jobs(),
                               self.batch_size)

    def get_backfill_jobs(self):
        ''' Method that returns the first batch_size jobs from each waiting
        queue ordered by their volume '''

        batch_list = []
        batch_list += self.__get_jobs(self.waiting_queue.get_priority_jobs(),
                                      self.batch_size)
        for i in range(self.waiting_queue.num_queues):
            batch_list += self.__get_jobs(
                self.waiting_queue.get_secondary_jobs(i),
                self.batch_size)
        return batch_list

    def create_job_reservation(self, job, reserved_jobs):
        ''' Method that implements a greedy algotithm for finding a place
        for a new job into a reservation window that is given by the
        previously reserved jobs '''

        gap_list = self.gaps_in_schedule.get_gaps(job.submission_time,
                                                  job.request_walltime,
                                                  job.nodes)
        if len(gap_list) == 0:
            return -1
        ts = max(gap_list[0][0], job.submission_time)
        self.logger.info(
            r'[Scheduler] Found inside reservation for %s at ts %d'
            % (job, ts))
        return ts

    def build_schedule(self, job, reservations, current_time):
        ''' Method for extending the existing reservation to include
        a new job. All jobs have to fit in the schedule, thus the reservation
        will be increased if the job does not fit inside the current one '''

        if len(reservations) == 0:
            self.logger.info(
                r'[Scheduler] Found reservation slot for %s at beginning' %
                (job))
            return current_time

        ts = self.create_job_reservation(job, reservations)
        if ts != -1:
            return ts

        end_window = max([reservations[j] + j.request_walltime
                          for j in reservations])
        # check for the end of the schedule for a fit (after all jobs that do
        # not have other jobs starting in front)
        gap_list = self.gaps_in_schedule.get_gaps(job.submission_time, 0,
                                                  job.nodes)
        if len(gap_list) > 0:
            end_gaps = [gap for gap in gap_list if gap[1] == end_window]
            if len(end_gaps) > 0:
                self.logger.info(
                    r'[Scheduler] Found reservation for %s at timestamp %d'
                    % (job, end_gaps[0][0]))
                return end_gaps[0][0]

        # there is no fit for the job to start anywhere inside the schedule
        # start the current job after the last job
        self.logger.info(
            r'[Scheduler] Found reservation slot for %s at timestamp %d' %
            (job, end_window))
        return end_window

    def trigger_schedule(self, current_time):
        ''' Method for creating a schedule for the first `batch_size` jobs
        in the waiting queue ordered by their submission time.
        The implemented algorithm is greedyly fitting jobs in the batch list
         starting with the largest on the first available slot '''

        self.waiting_queue.update_priority(current_time)
        self.gaps_in_schedule.clear()
        batch_jobs = self.get_batch_jobs()
        selected_jobs = {}
        for job in batch_jobs:
            # find a place for the job in the current schedule
            ts = self.build_schedule(job, selected_jobs, current_time)
            selected_jobs[job] = ts
            self.gaps_in_schedule.add({job: ts})
            self.waiting_queue.remove(job)

        # try to fit any of the remaining jobs into the gaps created by the
        # schedule (for the next batch list)
        batch_jobs = self.get_backfill_jobs()
        for job in batch_jobs:
            ts = self.create_job_reservation(job, selected_jobs)
            if ts != -1:
                selected_jobs[job] = ts
                self.gaps_in_schedule.add({job: ts})
                self.waiting_queue.remove(job)

        # return (start_time, job) list in the current batch
        return [(selected_jobs[job], job) for job in selected_jobs]

    def backfill_request(self, stop_job, reservation, current_time):
        ''' Method that implements a greedy algorithm to find jobs from
        the waiting queue that fit in the space left after the end of the
        `stop_job`. Larger jobs are inspected first and first slot available
        is reserved '''

        self.gaps_in_schedule.trim(current_time)
        reserved_jobs = reservation.copy()
        self.gaps_in_schedule.remove({stop_job: reservation[stop_job]})
        self.logger.info(r'[Backfill for job %s] Reservations: %s' %
                         (stop_job, reserved_jobs))

        batch_jobs = self.get_backfill_jobs()
        selected_jobs = []
        for job in batch_jobs:
            tm = super(BatchScheduler, self).fit_job_in_schedule(
                job, reserved_jobs, current_time=current_time)
            if tm != -1:
                selected_jobs.append((tm, job))
                reserved_jobs[job] = tm
                self.waiting_queue.remove(job)
        return selected_jobs


class OnlineScheduler(Scheduler):
    ''' Online scheduler (default LJF completly online) '''

    def __init__(self, system, total_queues=1, logger=None):
        ''' Constructor method forces the baso to use only one
        waiting queue '''
        super(OnlineScheduler, self).__init__(system, logger,
                                              total_queues)

    def __str__(self):
        return "Online "+super(OnlineScheduler, self).__str__()

    def __repr__(self):
        return "Online "+super(OnlineScheduler, self).__repr__()

    def clear_job(self, job):
        ''' Method that overwrites the base one to indicate that a new
        schedule needs to be triggered after each job end '''

        super(OnlineScheduler, self).clear_job(job)
        self.gaps_in_schedule.completely_remove(job)
        return 0  # trigger a new schedule starting now (timestamp 0)

    def __get_next_queued_job(self, nodes, queue):
        ''' Method to extract the largest volume job from the queue
        in the waiting queue that fits the space given by the `nodes` '''

        try:
            max_volume = max([job.nodes * job.request_walltime for job
                              in queue if job.nodes <= nodes])
            largest_jobs = [
                job for job in queue if job.nodes *
                job.request_walltime == max_volume and job.nodes <= nodes]
        except BaseException:
            # there are no jobs that fit the given space
            return -1
        # out of the largest jobs get the one submitted first
        min_time = min([job.submission_time for job in largest_jobs])
        return [job for job in largest_jobs
                if job.submission_time == min_time][0]

    def get_next_job(self, nodes):
        ''' Method to extract the largest volume job (nodes * requested time)
        in the waiting queue that fits the space given by the `nodes`. The job
        searches the main and secondary waiting queues in order of their
        priority '''

        job = self.__get_next_queued_job(
            nodes, self.waiting_queue.get_priority_jobs())
        if job != -1:
            return job
        # there are no jobs in the main queue, start searching secondary queues
        for i in range(self.waiting_queue.num_queues):
            job = self.__get_next_queued_job(
                nodes, self.waiting_queue.get_secondary_jobs(i))
            if job != -1:
                return job
        return -1

    def trigger_schedule(self, current_time):
        ''' Method for chosing the next jobs to run. For the online
        scheduler, the method iteratively choses the largest job that fits
        in the available nodes in the system until no job fits or there
        are no more nodes left in the system '''

        self.waiting_queue.update_priority(current_time)
        self.gaps_in_schedule.trim(current_time)

        jobs_in_waiting_queue = self.waiting_queue.total_jobs()
        self.logger.info('Wait queue: %d' % (jobs_in_waiting_queue))
        selected_jobs = []
        if jobs_in_waiting_queue == 0:
            return []
        free_nodes = self.system.get_free_nodes()
        while free_nodes > 0:
            job = self.get_next_job(free_nodes)
            self.logger.info(
                r'Reserve next job: %s; Free procs %s' % (job, free_nodes))
            if job == -1:
                break
            selected_jobs.append((current_time, job))
            self.gaps_in_schedule.add({job: current_time})
            self.waiting_queue.remove(job)
            free_nodes -= job.nodes
        return selected_jobs

    def fit_job_in_schedule(self, job, reserved_jobs, current_time=0):
        ''' Method that overwrites the base class that implements a
        reservation based algorithm. For the base method all jobs
        need to be fitted in the reservation window and cannot exceed
        the end. Online methods do not have this limitation '''

        if len(reserved_jobs) == 0:
            return -1

        gap_list = self.gaps_in_schedule.get_gaps(job.submission_time,
                                                  0, job.nodes)
        if len(gap_list) == 0:
            return -1
        # keep only the gaps that can start the job from the submission time
        for gap in gap_list:
            if job.submission_time > gap[0]:
                self.logger.info(
                    r'[OnlineScheduler] Found space for %s: timestamp %d' %
                    (job, gap[0]))
                self.gaps_in_schedule.add({job: job.submission_time})
                return job.submission_time
        return -1
