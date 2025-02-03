#  Copyright (c) 2019-2020 by the ScheduleFlow authors
#   All rights reserved.

#   This file is part of the ScheduleFlow package. ScheduleFlow is
#   distributed under a BSD 3-clause license. For details see the
#   LICENSE file in the top-level directory.

#   SPDX-License-Identifier: BSD-3-Clause

import subprocess
import logging
import heapq
import os
import shutil
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
    WalltimeChange = 1


class EventType(IntEnum):
    ''' Enumeration class to hold event types (distinct values are required)
    The values give the order in which the simulator parses the events
    (e.g. JobEnd has the higher priority and will be evaluated first) '''

    Submit = 2
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


class WaitingQueue(object):
    ''' Class responsible with storing the priority queues used by the
    scheduler to hold the jobs in several waiting queues '''

    def __init__(self, total_queues=2):
        ''' Creates two waiting queues, one for large jobs having high
        priority and one for backfilling jobs '''

        assert (total_queues > 0), 'The waiting queue must contain'\
            'at least one queue (%d requested)' % (total_queues)
        self.num_queues = total_queues - 1
        self.volume_threshold = [36000 / i for i in
                                 range(1, self.num_queues + 1)]
        if self.num_queues == 0:
            self.volume_threshold = [0]

        self.main_queue = set()
        self.secondary_queues = [set() for i in range(self.num_queues)]
        self.__last_update = {}

    def __str__(self):
        return 'Wait queue: total of %d' % (
            len(self.secondary_queues) + 1)

    def __repr__(self):
        return 'WaitQueue(total %d queues)' % (
            len(self.secondary_queues) + 1)

    def add(self, job):
        ''' Method for adding a job into the waiting queues based on
        their total volume '''

        request_walltime = job.get_current_total_request_time()
        self.__last_update[job] = job.submission_time
        job_volume = request_walltime * job.nodes
        if job_volume > self.volume_threshold[0]:
            self.main_queue.add(job)
            return
        idx = max([i for i in range(len(self.volume_threshold)) if
                   self.volume_threshold[i] >= job_volume])
        self.secondary_queues[idx].add(job)

    def remove(self, job):
        ''' Method for removing a job from the waiting queues '''

        assert (job in self.__last_update), 'Atempting to remove'\
            'inexisting job from the waiting queues'
        del self.__last_update[job]
        if job in self.main_queue:
            self.main_queue.remove(job)
            return
        idx = [i for i in range(len(self.secondary_queues)) if
               job in self.secondary_queues[i]]
        self.secondary_queues[idx[0]].remove(job)

    def update_queue(self, low_queue, high_queue,
                     threshold, current_time):
        ''' Method for moving all the jobs from the low_queue to
        the high queue if the time since submission exceeds the
        threshold '''

        del_list = []
        for job in low_queue:
            if current_time - self.__last_update[job] > threshold:
                del_list.append(job)
        for job in del_list:
            self.__last_update[job] = current_time
            low_queue.remove(job)
            high_queue.add(job)

    def update_priority(self, current_time, time_threshold=1800):
        ''' Method for updating the priority of jobs that spent
        more time than the threshold in the backfill queue '''

        if len(self.secondary_queues) == 0:
            return
        for i in range(len(self.secondary_queues)-1, 0, -1):
            self.update_queue(self.secondary_queues[i],
                              self.secondary_queues[i-1],
                              time_threshold,
                              current_time)
        self.update_queue(self.secondary_queues[0], self.main_queue,
                          time_threshold, current_time)

    def fill_priority_queue(self):
        ''' Method called when the main queue is empty for bringing to it
        the highest priority largest job from the secondary queues '''

        if len(self.main_queue) == 0 and self.total_secondary_jobs() > 0:
            # get the first priority queue that has at least one job
            idx = min([i for i in range(len(self.secondary_queues)) if
                       len(self.secondary_queues[i]) > 0])
            # move the longest job from the backfill queue
            longest_job = max(self.secondary_queues[idx], key=lambda job:
                              job.nodes*job.get_current_total_request_time())
            self.secondary_queues[idx].remove(longest_job)
            self.main_queue.add(longest_job)

    def total_jobs(self):
        ''' Method for returning the total jobs in all queues '''
        return self.total_priority_jobs() + self.total_secondary_jobs()

    def total_priority_jobs(self):
        ''' Method for returning the total jobs in main queue '''
        return len(self.main_queue)

    def total_secondary_jobs(self):
        ''' Method for returning the total jobs in all the
        secondary queues '''
        if len(self.secondary_queues) == 0:
            return 0
        return sum([len(queue) for queue in self.secondary_queues])

    def get_priority_jobs(self):
        ''' Return all the high priority jobs '''
        return self.main_queue

    def get_secondary_jobs(self, index=0):
        ''' Return all the low priority jobs '''
        return self.secondary_queues[index]


class ScheduleGaps(object):
    ''' Class for storing and accessing the list of gaps within a
    reservation schedule '''

    def __init__(self, total_nodes):
        self.gaps_list = [] # (start, end, available_procs)
        self.__total_nodes = total_nodes
        self.__reserved_jobs = {}

    def __str__(self):
        return 'ScheduleGaps(%d nodes, %d jobs : %s)' % (
                self.__total_nodes, len(self.__reserved_jobs),
                self.gaps_list)

    def clear(self):
        ''' Clear all entries in the gap list '''
        self.gaps_list = []
        self.__reserved_jobs = {}

    def trim(self, current_time):
        ''' Delete all gaps that end before the current timestamp '''
        job_list = [job for job in self.__reserved_jobs if
                    (self.__reserved_jobs[job] +
                     job.get_current_total_request_time()) <
                    current_time]
        for job in job_list:
            del self.__reserved_jobs[job]
        if len(self.__reserved_jobs) == 0:
            self.gaps_list = []
            return []
        gaps_idx = [idx for idx in range(len(self.gaps_list)) if
                    self.gaps_list[idx][1] < current_time]
        gaps_idx.sort(reverse=True)
        for idx in gaps_idx:
            del self.gaps_list[idx]

        return self.gaps_list

    def __overflow_intersections(self, gaps_idx, start, end, procs, ops):
        ''' Method that creates new gaps if the new job exceeds the [min, max]
        of existing gaps. The start and end values of the new job are updated
        to reflect what is left of the job '''

        new_gaps = []
        min_start = min([self.gaps_list[idx][0] for idx in gaps_idx])
        max_end = max([self.gaps_list[idx][1] for idx in gaps_idx])
        available_procs = self.__total_nodes
        if ops > 0:
            available_procs = 0
        if start < min_start:
            if available_procs + procs * ops > 0:
                new_gaps.append([start, min_start,
                                 available_procs + procs * ops])
            start = min_start
        if end > max_end:
            if available_procs + procs * ops > 0:
                new_gaps.append([max_end, end,
                                 available_procs + procs * ops])
            end = max_end
        return (start, end, new_gaps)

    def __update_intersections(self, gaps_idx, start, end, procs, ops):
        ''' Update the gaps that intersect the new job that needs to be
        scheduled. Gaps_idx represent the list of gaps that intersect the
        job which is characterized by start, end and procs. Ops indicates
        if the job is being added (-1) or removed (1) '''

        if len(gaps_idx) == 0:
            return []
        (start, end, new_gaps) = self.__overflow_intersections(
            gaps_idx, start, end, procs, ops)
        intersect_gaps_idx = [idx for idx in gaps_idx if
                              self.gaps_list[idx][0] < start or
                              self.gaps_list[idx][1] > end]

        # check all gaps that still intersect the remaining of the job
        # (without edges outside existing gaps)
        intersect_gaps_idx.sort(reverse=True)
        for idx in intersect_gaps_idx:
            if self.gaps_list[idx][0] < start:
                new_gaps.append([self.gaps_list[idx][0], start,
                                 self.gaps_list[idx][2]])
                self.gaps_list[idx][0] = start
            if self.gaps_list[idx][1] > end:
                new_gaps.append([end, self.gaps_list[idx][1],
                                 self.gaps_list[idx][2]])
                self.gaps_list[idx][1] = end
            self.gaps_list[idx][2] = self.gaps_list[idx][2] + procs * ops
            #if (self.gaps_list[idx][2] > 0 and
            if self.gaps_list[idx][0] != self.gaps_list[idx][1]:#):
                new_gaps.append([self.gaps_list[idx][0],
                                self.gaps_list[idx][1],
                                self.gaps_list[idx][2]])
        return new_gaps

    def __update_included(self, gaps_idx, start, end, procs, ops):
        ''' Update the gaps that are completely included by the new job.
        Gaps_idx represent the list of gaps that intersect the
        job which is characterized by start, end and procs '''

        new_gaps = []
        include_gaps_idx = [idx for idx in gaps_idx if
                            self.gaps_list[idx][0] >= start and
                            self.gaps_list[idx][1] <= end]
        for idx in include_gaps_idx:
            new_gaps.append([self.gaps_list[idx][0],
                             self.gaps_list[idx][1],
                             self.gaps_list[idx][2] + procs * ops])
        return new_gaps

    def __consolidate(self, gaps_list):
        ''' Method for removing duplicate gaps and merging smaller gaps
        into more inclusive ones '''

        gaps_list.sort()
        remove_list = set()

        for i in range(len(gaps_list)):
            gap = gaps_list[i]
            # merge overlapping gaps (e.g. [[0, 10, 3], [5, 12, 5]]
            # becomes [[0, 12, 3], [5, 12, 5]]
            overlap = [idx for idx in range(len(gaps_list)) if
                       (gaps_list[idx][0] <= gap[1] and
                        gaps_list[idx][1] >= gap[0] and
                        idx not in remove_list) and idx != i]
            for idx in overlap:
                start = min(gaps_list[idx][0], gap[0])
                end = max(gaps_list[idx][1], gap[1])
                if gaps_list[idx][2] <= gap[2]:
                    gaps_list[idx][0] = start
                    gaps_list[idx][1] = end
                    if gaps_list[idx][2] == gap[2]:
                        remove_list.add(i)
                    continue

                if start == gaps_list[idx][0] and end == gaps_list[idx][1]:
                    remove_list.add(i)
                else:
                    gaps_list[i][0] = start
                    gaps_list[i][1] = end

        remove_list = sorted(list(remove_list), reverse=True)
        if len(remove_list) == 0:
            return 0
        for idx in remove_list:
            del gaps_list[idx]
        return -1

    def __fill_gap_to_neighbors(self, new_job):
        ''' Add neighbor space on the left and right of the new job '''

        new_gaps = []
        start = self.__reserved_jobs[new_job]
        left_gaps = [self.__reserved_jobs[job] +
                     job.get_current_total_request_time()
                     for job in self.__reserved_jobs if
                     self.__reserved_jobs[job] +
                     job.get_current_total_request_time() <= start]
        if len(left_gaps) > 0 and max(left_gaps) < start:
            new_gaps.append([max(left_gaps), start, self.__total_nodes])
        end = self.__reserved_jobs[new_job] \
            + new_job.get_current_total_request_time()
        right_gaps = [self.__reserved_jobs[job] for job in self.__reserved_jobs
                      if self.__reserved_jobs[job] >= end]
        if len(right_gaps) and min(right_gaps) > end:
            new_gaps.append([end, min(right_gaps), self.__total_nodes])
        return new_gaps

    def __update_reserved_list(self, job, start, ops):
        ''' Method that removes or stores information about the new job '''
        if ops < 0:
            self.__reserved_jobs[job] = start
        else:
            del self.__reserved_jobs[job]

    def __fill_voids(self, job, start, end, procs, ops):
        ''' Method called only for jobs that do not intersect any other gaps.
        The void space between the new job and end/beginning of the neighbor
        job needs to be represented by a gap '''

        new_gaps = []
        free_nodes = procs
        if ops < 0:
            new_gaps = self.__fill_gap_to_neighbors(job)
            free_nodes = self.__total_nodes - procs
        if free_nodes > 0:
            new_gaps.append([start, end, free_nodes])

        if len(new_gaps) > 0 and new_gaps[0][0] != start:
            start = new_gaps[0][0]
            end = new_gaps[0][1]
            procs = 0
        return (new_gaps, start, end, procs)

    def __free_hidden_gaps(self, affected_gaps_idx, start, end, procs):
        hidden_gaps = []
        # for each gap end
        for idx in affected_gaps_idx:
            gap_start = self.gaps_list[idx][1]
            # find all the gaps that start before this end
            gaps = [self.gaps_list[i][1] for i in affected_gaps_idx
                    if i != idx and
                    self.gaps_list[i][0] < gap_start]
            # if the max of these gaps does not exceed the gap end,
            # there might be a hidden gap with no available space
            if len(gaps) == 0 or max(gaps) <= gap_start:
                # find the end of the hidden gap as the min of the
                gaps = [self.gaps_list[i][0] for i in
                        affected_gaps_idx if
                        self.gaps_list[i][0] > gap_start]
                if len(gaps) == 0:
                    if gap_start != end:
                        hidden_gaps.append([gap_start, end, procs])
                else:
                    hidden_gaps.append([gap_start, min(gaps), procs])
        return hidden_gaps

    def update(self, reserved_jobs, ops):
        ''' Method for updating the gaps in a schedule when new jobs are
        included in the schedule or are ending and are creating backfill
        space. Reserved_jobs represents the list of new jobs and ops
        indicates if the job is being added (-1) or removed (1) '''

        # reserved_jobs[job] = time
        for job in reserved_jobs:
            assert(job not in self.__reserved_jobs, "ERR Cannot add"
                   "a job that is already part of a schedule")
            self.__update_reserved_list(job, reserved_jobs[job], ops)
            start = reserved_jobs[job]
            request_walltime = job.get_current_total_request_time()
            end = reserved_jobs[job] + request_walltime
            # for removing job backfills, the available space is between
            # when the job ends and how much time was reserved for the job
            if ops == 1:
                start += job.walltime
            if start == end:
                continue
            procs = job.nodes

            # identify the gaps that are affected by the new job
            # gap start < job end and gap end > job start
            affected_gaps_idx = [idx for idx in range(len(self.gaps_list))
                                 if self.gaps_list[idx][0] <= end and
                                 self.gaps_list[idx][1] >= start]
            new_gaps = []
            if len(affected_gaps_idx) == 0:
                # add empty gap between current job and the neighbors
                (new_gaps, start, end, procs) = self.__fill_voids(
                    job, start, end, procs, ops)
                if procs == 0:
                    affected_gaps_idx = [
                        idx for idx in range(len(self.gaps_list))
                        if self.gaps_list[idx][0] < end and
                        self.gaps_list[idx][1] > start]

            if len(new_gaps) == 0 and len(affected_gaps_idx) == 0:
                self.gaps_list.append([start, end, self.__total_nodes - procs])
                continue

            # update the amount of free processing units for all the gaps
            # that are completely included inside the new job
            new_gaps += self.__update_included(
                affected_gaps_idx, start, end, procs, ops)
            # update gaps that intersect the new job
            new_gaps += self.__update_intersections(
                affected_gaps_idx, start, end, procs, ops)
            if ops==1:
                new_gaps += self.__free_hidden_gaps(
                    affected_gaps_idx, start, end, procs)

            # consolidate the new gaps
            ret = -1
            while ret < 0:
                ret = self.__consolidate(new_gaps)

            # remove all affected gaps from the gap list and
            # add the consolidated new list of gaps
            affected_gaps_idx.sort(reverse=True)
            for idx in affected_gaps_idx:
                del self.gaps_list[idx]
            self.gaps_list += new_gaps
            self.gaps_list.sort()
        return self.gaps_list

    def add(self, job_list):
        ''' Method for adding jobs in the schedule '''
        return self.update(job_list, -1)

    def remove(self, job_list):
        ''' Method for removing the backfilling space of jobs from
        the schedule '''
        return self.update(job_list, 1)

    def completely_remove(self, job):
        ''' Method for removing the entore jobs from the schedule '''
        job_list = {}
        if job not in self.__reserved_jobs:
            return self.gaps_list
        job_list[job] = self.__reserved_jobs[job]
        return self.update(job_list, 2)

    #  _____________
    # |   |__
    # |      |__
    # |         |
    # t1 t2 t3 t4 The function returns (t2, t4, 1) and (t3 t4, 2)
    def get_ending_gaps(self, current_time):
        if len(self.gaps_list) == 0:
            return []
        # find the end of the last gap
        last_end = max([gap[1] for gap in self.gaps_list])
        # return all the gaps that extend to the end
        return [gap for gap in self.gaps_list if gap[1] == last_end]

    def fit_at_the_end(self, start_time, nodes):
        if len(self.gaps_list) == 0:
            return start_time
        end_gaps = self.get_ending_gaps(start_time)
        # find all gaps (start, end, procs) that have enough space
        gap_list = [gap for gap in end_gaps if gap[2] >= nodes]
        if len(gap_list) == 0:
            # return the latest end time
            return max([gap[1] for gap in self.gaps_list])
        earliest_gap = min([gap[0] for gap in gap_list])
        return max(earliest_gap, start_time)

    def get_gaps(self, start_time, length, nodes):
        ''' Return all the gaps that can fit a job using a given number of
        nodes, requiring a length walltime and that has to start the earliest
        at start_time '''
        return [gaps for gaps in self.gaps_list if (gaps[1] > start_time
                and gaps[1] - max(start_time, gaps[0]) >= length and
                gaps[2] >= nodes)]


class Runtime(object):
    ''' Runtime class responsible for coordinating the submission and
    execution process for all the jobs in a workload '''

    def __init__(self, workload, simulation_end_time=-1, logger=None):
        ''' Constructor method creates the job submission events for all
        jobs in the workload. It also requires a default facor value for
        increasing the request time of failed jobs (in case they do not
        contain a sequence of request walltimes '''

        self.__current_time = 0
        self.__sim_end_time = simulation_end_time
        self.__finished_jobs = {}  # finish_jobs[job] = [(start, end)]
        self.__events = EventQueue()
        self.__logger = logger or logging.getLogger(__name__)

        # create submit_job events for all the applications in the list
        for job in workload:
            self.__events.push(
                (job.submission_time, EventType.Submit, job))

        # initialize the progress bar if we don't print anything else
        if self.__logger == None:
            self.__progressbar_width = min(50, len(workload))
            self.total_jobs = len(workload)
            sys.stdout.write("[%s]" % ("." * self.__progressbar_width))
            sys.stdout.flush()
            sys.stdout.write("\b" * (self.__progressbar_width + 1))
            self.__progressbar_step = 1

    def update_progressbar(self):
        progress = int((self.total_jobs * self.__progressbar_step) /
                       self.__progressbar_width)
        if len(self.__finished_jobs) < progress:
            return
        sys.stdout.write("=")
        sys.stdout.flush()
        self.__progressbar_step += 1

    def __call__(self, sch):
        ''' Method for execution the simulation on a given scheduler '''

        self.scheduler = sch

        while not self.__events.empty():
            # get next set of events
            current_events = self.__events.pop_list()
            self.__current_time = current_events[0][0]
            if self.__sim_end_time > 0 and self.__current_time > self.__sim_end_time:
                break

            self.__logger.debug(r'[Timestamp %d] Receive events %s' % (
                self.__current_time, current_events))

            submitted_jobs = []
            start_jobs = []
            end_jobs = []
            for event in current_events:
                if event[1] == EventType.Submit:
                    submitted_jobs.append(event[2])
                elif event[1] == EventType.JobStart:
                    start_jobs.append(event[2])
                elif event[1] == EventType.JobEnd:
                    end_jobs.append(event[2])

            if len(end_jobs) > 0:
                self.__job_end_event(end_jobs)
            if len(start_jobs) > 0:
                self.__job_start_event(start_jobs)
            if len(submitted_jobs) > 0:
                self.__job_subimssion_event(submitted_jobs)
            # no new events were created in this step and the queue is empty
            if self.__events.empty():
                # make sure the scheduler wait list is also empty
                self.__trigger_schedule_event()

            self.__logger.debug("[Timestamp %d] Sceduler state: %s" %(
                self.__current_time, self.scheduler))

        # at the end of the simulation return default values for all the jobs
        for job in self.__finished_jobs:
            job.restore_default_values()

        # end the progress bar
        sys.stdout.write("]\n")

    def __handle_scheduler_actions(self, action_list):
        ''' Method for handling a list of instructions returned by the
        scheduler (in the form (time, job), where action could be a Start
        event (if tm != -1)  or an End event. The function changes the
        internal state of the event list '''

        for job_event in action_list:
            tm = job_event[0]
            job = job_event[1]
            if tm != -1:
                self.__logger.info(
                    r'[Timestamp %d] %s set to start at time %d' %
                    (self.__current_time, job, tm))
                self.__events.push((tm, EventType.JobStart, job))
            else:
                self.__log_start(job)
                # create a job end event for the started job
                # for timestamp current_time + execution_time
                execution = job.walltime + job.get_checkpoint_read_time()
                # execution time is the walltime + time to read the last checkpoint
                # in case of successful run or the total request time
                if job.walltime > job.request_walltime:
                    execution = job.get_current_total_request_time()
                self.__logger.info("[Timestamp %d] %s set to"
                                   " end at time %d" % (
                    self.__current_time, job, self.__current_time + execution))
                self.__events.push(
                    (self.__current_time + execution, EventType.JobEnd, job))

    def __job_subimssion_event(self, job_list):
        ''' Method for handling a job submission event. The method takes the
        job that is being submitted and if it is allowed to start it now
        inside an existing schedule '''

        # get the list of jobs that need a status change ret = (tm job)
        ret = self.scheduler.submit_job(self.__current_time, job_list)
        self.__handle_scheduler_actions(ret)

    def __trigger_schedule_event(self):
        ''' Method for handling an event for triggering a new schedule. '''

        ret_schedule = self.scheduler.trigger_schedule(self.__current_time)
        self.__logger.info(r'[Timestamp %d] Trigger schedule %s' % (
            self.__current_time, ret_schedule))
        self.__handle_scheduler_actions(ret_schedule)

    def __job_end_event(self, job_list):
        ''' Method for handling a job end event '''

        for job in job_list:
            # check if the job finished successfully or it was a failure
            if job.walltime > job.request_walltime and job.resubmit:
                # resubmit failed job
                job.update_submission(self.__current_time)
                self.__logger.info(
                    r'[Timestamp %d] Resubmit failed job %s' %
                    (self.__current_time, job))
                self.__events.push((self.__current_time,
                                    EventType.Submit, job))
            else:
                # if successful update the progress bar
                if self.__logger == None:
                    self.update_progressbar()
                self.__log_end(job)

        # handle backfilling jobs and new jobs ready to be executed
        ret = self.scheduler.stop_job(self.__current_time, job_list)
        self.__handle_scheduler_actions(ret)

    def __job_start_event(self, job_list):
        ''' Method for handling a job start event '''

        ret = self.scheduler.start_job(self.__current_time, job_list)
        self.__handle_scheduler_actions(ret)

    def __log_start(self, job):
        ''' Method for logging the information about a new job start '''

        if job not in self.__finished_jobs:
            self.__finished_jobs[job] = []
        self.__finished_jobs[job].append([self.__current_time, -1])

    def __log_end(self, job):
        ''' Method for logging the information about job end '''

        assert (job in self.__finished_jobs),\
            "Logging the end of a job that did not start"
        self.__finished_jobs[job][-1][1] = self.__current_time

    def get_stats(self):
        ''' Method for returning the log containing every jon start and
        finish recorded during the simulation up to the current time '''

        return self.__finished_jobs


class TexGenerator():
    ''' Internal class used by the Visualization Engine to create the
    latex files that will be compiled into a GIF animation '''

    def __init__(self, execution_slices_list, execution_job_list,
                 scalex, scaley, labely):
        ''' The constructor takes a division of the space into slices,
        the execution log for each job and the vertical and horizontal
        scale factors to fit the simulation to the figure size '''
        self.__scalex = scalex
        self.__scaley = scaley
        self.__labely = labely
        self.__slices = execution_slices_list
        self.__run_list = execution_job_list
        self.__total_runs = len(execution_job_list)

    def write_to_file(self, filename):
        ''' Method to create a file for each step of the simulation '''

        for i in range(self.__total_runs + 1):
            outf = open(os.environ["ScheduleFlow_PATH"]+'/draw/%s_%d.tex' % (
                filename, i), 'w')
            # write header
            outf.writelines(
                [l for l in open(os.environ["ScheduleFlow_PATH"] +
                                 "/draw/tex_header").readlines()])
            # add legend on the vertical axes, needs to be out of the header
            # since the total number of procs is not known in the tex file
            outf.write("\draw[-, very thick] (-1, 151) -- (1, 151) node [pos=0, left] {$\scriptstyle{%d}$};" %(self.__labely))
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
            outf.writelines(
                [l for l in open(os.environ["ScheduleFlow_PATH"] +
                                 "/draw/tex_footer").readlines()])
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
        consecutive failed instances (blue represents low priority) '''

        start = float(execution[0]) * self.__scalex
        end = float(execution[1]) * self.__scalex
        procs = execution[2] * self.__scaley
        offset = execution[6] * self.__scaley
        job_id = execution[4]
        priority = execution[7]
        color = 2 * min(execution[5], 5)
        color_text = r"{rgb:red,%d;yellow,%d}" % (
            color, 10 - color)
        # if successful execution of a low priority job
        if priority > 0 and execution[5]==0:
            color_text = "{rgb:blue,25;green,25}"
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
        self.__labely = procs
        self.__limitx = horizontal_ax_limit
        self.__execution_log = execution_log
        self.__keep_pdf = keep_intermediate_pdf
        self.__set_scalex(execution_log)

        # check if pdflatex and convert from ImageMagik are installed
        assert (shutil.which('pdflatex')), \
            'Pdflatex needs to be installed to create GIFs'
        assert (shutil.which('convert')), \
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

    def __order_jobs_for_gif(self, run_list):
        sort_list = sorted(
            run_list, key=lambda i: (-i[0], i[1]-i[0]), reverse=True)
        return sort_list

    def __generate_animation_files(self, filename):
        ''' Generate a temp list of (start, end, procs,
        requested walltime, job_id, color) used to create
        the tex file '''

        run_list = []
        for job in self.__execution_log:
            run_list += self.__get_job_runs(self.__execution_log[job], job)

        # order list of jobs by start time (ascending) and length (descending)
        run_list = self.__order_jobs_for_gif(run_list)

        sliced_list = self.__get_sliced_list(run_list)

        tex_generator = TexGenerator(sliced_list, run_list,
                                     self.__scalex, self.__scaley,
                                     self.__labely)
        tex_generator.write_to_file(filename)

    def __find_running_jobs(self, run_list, start, end):
        ''' Given an execution log find all jobs that are included
        inside the schedule between start and end '''

        return [i for i in range(len(run_list)) if
                run_list[i][0] <= start and
                run_list[i][1] >= end]

    def __merge_slices(self, sliced_list):
        ''' Merge slices for the same job if they are on the same
            horizontal line '''

        for job_slices in sliced_list:
            del_list = []
            last_keep_index = 0
            for i in range(1, len(job_slices)):
                # if this slice continues the previous
                if job_slices[i][0] == job_slices[i - 1][1]:
                    # if they are on the same horizontal line
                    if job_slices[i][6] == job_slices[i - 1][6]:
                        del_list.append(i)
                        job_slices[last_keep_index][1] = job_slices[i][1]
                        continue
                last_keep_index = i

            del_list.sort(reverse=True)
            for i in del_list:
                del job_slices[i]

        return sliced_list

    def __get_sliced_list(self, run_list):
        ''' Generate a list of (start, end, procs, request_end,
        job_id, failure_count, starty, priority) for each job
        instance for each slice (a slice is a unit execution time
        not containing any job starts or ends)'''

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
                    [event_list[i], event_list[i + 1],
                     run_list[idx][2], run_list[idx][3] +
                     run_list[idx][0], run_list[idx][4],
                     run_list[idx][5], starty, run_list[idx][6]])
                starty += run_list[idx][2]

        return self.__merge_slices(sliced_list)

    def __get_job_runs(self, execution_list, job):
        ''' Generate a list of (start, end, procs, request_time,
        job_id, failure_count) for each job instance run '''

        run_list = []
        requested_time = job.request_walltime
        for i in range(len(execution_list) - 1):
            # check failed executions
            start = execution_list[i][0]
            end = execution_list[i][1]
            priority = job.priority
            fig_text = job.name
            if fig_text == "Unname":
                fig_text = job.job_id
            run_list.append((start, end, job.nodes,
                             requested_time, fig_text,
                             i + 1, priority))
            requested_time = job.get_total_request_time(i + 1)

        # check succesful execution (last run)
        fig_text = job.name
        if fig_text == "Unname":
            fig_text = job.job_id
        start = execution_list[len(execution_list) - 1][0]
        end = execution_list[len(execution_list) - 1][1]
        priority = job.priority
        run_list.append((start, end, job.nodes,
                         requested_time, fig_text, 0,
                         priority))
        return run_list


class StatsEngine():
    ''' Internal class used by the Simulator to generate the statistics
    related to a simulation (utilization, average makespan, etc) '''

    def __init__(self, total_nodes):
        self.__execution_log = {}
        self.__makespan = -1
        self.__total_nodes = total_nodes

        self.__metric_mapping = {
            "system makespan": self.total_makespan,
            "system utilization": self.system_utilization,
            "job utilization": self.average_job_utilization,
            "job response time": self.average_job_response_time,
            "job stretch": self.average_job_stretch,
            "job wait time": self.average_job_wait_time,
            "job failures": self.total_failures}
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

    def set_metrics(self, metric_list):
        ''' Add the metrics of interest for the current simulation '''

        for metric in metric_list:
            if metric == "all":
                return self.__metrics
        self.__metrics = set()
        for metric in metric_list:
            self.__metrics |= set([m for m in self.__metric_mapping
                                   if metric in m])

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
                file_handler.write("%s : " % (metric))
            file_handler.write("\n")

        # print metric values
        file_handler.write("%s : " % (scenario))
        for metric in self.__metrics:
            file_handler.write("%.2f : " % (
                self.__metric_mapping[metric]()))
        file_handler.write("\n")
