import logging
from enum import IntEnum
from EventQueue import EventQueue
from EventQueue import EventType


class System(object):
    ''' System class containing available resources (for now just nodes) '''

    def __init__(self, total_nodes):
        assert (total_nodes > 0),\
            r'Number of nodes of a system must be > 0: received %d' % (
            total_nodes)

        self.__total_nodes = total_nodes
        self.__free_nodes = total_nodes

    def __str__(self):
        return r'System of %d nodes (%d currently free)' % (
            self.__total_nodes, self.__free_nodes)

    def get_free_nodes(self):
        return self.__free_nodes

    def get_total_nodes(self):
        return self.__total_nodes

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


class JobChangeType(IntEnum):
    ''' Enumeration class to hold  '''

    SubmissionChange = 0
    RequestChange = 1
    RequestSequenceOverwrite = 2


class ApplicationJob(object):
    ''' Job class containing the properties of the running instance '''

    def __init__(self, nodes, submission_time, walltime,
                 requested_walltimes, job_id, request_sequence=[],
                 resubmit_factor=-1):
        ''' Constructor method takes the number of nodes required by the job,
        the submission time, the actual walltime, the requested walltime, a
        job id and a sequence of request times in case the job fails '''

        assert (walltime > 0),\
            r'Application walltime must be positive: received %3.1f' % (
            walltime)
        assert (all(i > 0 for i in requested_walltimes)),\
            r'Job requested walltime must be > 0 : received %s' % (
                    requested_walltimes)
        assert (submission_time >= 0),\
            r'Job submission time must be > 0 : received %3.1f' % (
            submission_time)
        assert (nodes > 0),\
            r'Number of nodes for a job must be > 0 : received %d' % (
            nodes)
        assert (all(requested_walltimes[i] < requested_walltimes[i + 1] for i in
                range(len(requested_walltimes) - 1))),\
            r'Request time sequence is not sorted in increasing order'

        self.nodes = nodes
        self.submission_time = submission_time
        self.walltime = walltime
        self.request_walltime = requested_walltimes[0]
        self.job_id = job_id
        self.request_sequence = requested_walltimes[1:]
        if resubmit_factor == -1:
            self.resubmit = False
            self.resubmit_factor = 1
        else:
            assert (resubmit_factor > 1),\
                r"""Increase factor for an execution request time must be
                over 1: received %d""" % (resubmit_factor)
            self.resubmit = True
            self.resubmit_factor = resubmit_factor

        # Entries in the execution log: (JobChangeType, old_value)
        self.__execution_log = []
        if len(self.request_sequence) > 0:
            self.__execution_log.append(
                    (JobChangeType.RequestSequenceOverwrite,
                     self.request_sequence[:]))

    def __str__(self):
        return r"""Job %d: %d nodes; %3.1f submission time; %3.1f total
               execution time (%3.1f requested)""" % (
               self.job_id, self.nodes, self.submission_time, self.walltime,
               self.request_walltime)

    def __repr__(self):
        return r'Job(%d, %3.1f, %3.1f, %3.1f, %d)' % (self.nodes,
                                                      self.submission_time,
                                                      self.walltime,
                                                      self.request_walltime,
                                                      self.job_id)

    def __lt__(self, apl):
        return self.job_id < apl.job_id

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

        if len(self.request_sequence) > step-1:
            return self.request_sequence[step-1]

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
        the submission time and the walltime request '''

        assert (submission_time >= 0),\
            r'Negative submission time received: %d' % (
            submission_time)
        self.__execution_log.append((JobChangeType.SubmissionChange,
                                     self.submission_time))
        self.__execution_log.append((JobChangeType.RequestChange,
                                     self.request_walltime))

        self.submission_time = submission_time
        # if factor is a series of request values, just use the first value
        if len(self.request_sequence) > 0:
            self.request_walltime = self.request_sequence[0]
            del self.request_sequence[0]
        else:
            self.request_walltime = int(self.resubmit_factor *
                                        self.request_walltime)

    def free_wasted_space(self):
        ''' Method for marking that the job finished leaving a gap equal to
        the difference between the requested time and the walltime '''
        self.__execution_log.append((JobChangeType.RequestChange,
                                     self.request_walltime))
        self.request_walltime = self.walltime

    def restore_default_values(self):
        ''' Method for restoring the initial submission values '''
        restore = next((i for i in self.__execution_log if
                        i[0] == JobChangeType.RequestSequenceOverwrite), None)
        if restore is not None:
            self.request_sequence = restore[1][:]
        restore = next((i for i in self.__execution_log if
                        i[0] == JobChangeType.SubmissionChange), None)
        if restore is not None:
            self.submission_time = restore[1]
        restore = next((i for i in self.__execution_log if
                        i[0] == JobChangeType.RequestChange), None)
        if restore is not None:
            self.request_walltime = restore[1]


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
        if job.walltime > job.request_walltime:
            # resubmit failed job unless the job doesn't permit it
            if not job.resubmit:
                return

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
