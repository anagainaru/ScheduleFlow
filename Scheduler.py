import logging


class Scheduler(object):
    ''' Base class that needs to be extended by all Scheduler classes '''

    def __init__(self, system, logger=None):
        ''' Base construnction method that takes a System object '''
        self.system = system
        self.wait_queue = set()
        self.running_jobs = set()
        self.logger = logger or logging.getLogger(__name__)

    def __str__(self):
        return r'Scheduler: %s; %d jobs in queue; %d jobs running' % (
            self.system, len(self.wait_queue), len(self.running_jobs))

    def submit_job(self, job):
        ''' Base method to add a job in the waiting queue '''

        assert (job.nodes <= self.system.get_total_nodes()),\
            "Submitted jobs cannot ask for more nodes that the system"
        self.wait_queue.add(job)

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

    def trigger_schedule(self):
        ''' Base method for triggering schedules guarantees the simulator
        will not crash if the child Schedulers do not implement it. The child
        methods implement different algorithms for choosing when/what jobs
        to run from the waiting queue at the current schedule cycle '''
        return []

    def create_gaps_list(self, reserved_jobs, min_ts):
        ''' Base method that extracts the gaps between the jobs in the given
        reservation. '''

        reservations = []
        # array of (job start, job end) for all jobs in the reservation list
        for entry in reserved_jobs:
            reservations.append((reserved_jobs[entry], 1, entry))
            reservations.append(
                (reserved_jobs[entry] + entry.request_walltime, 0, entry))
        order_reservations = sorted(reservations)

        # parse each job begin/end in the reservation and mark space left
        gap_list = []
        nodes = 0
        ts = -1
        for event in order_reservations:
            if event[0] > ts and ts != -1:
                gap_list.append(
                    [ts, event[0], self.system.get_total_nodes() - nodes])
                # gap end is the same as the beginning of the current gap
                prev = [gap for gap in gap_list if gap[1] == ts]
                for gap in prev:
                    gap_list.append([gap[0], event[0], min(
                        gap[2], self.system.get_total_nodes() - nodes)])
            if event[1] == 1:  # job start
                nodes += event[2].nodes
            else:  # job_end
                nodes -= event[2].nodes
            ts = max(event[0], min_ts)
        gap_list.sort()
        return [i for i in gap_list if i[2]>0]

    def fit_job_in_schedule(self, job, reserved_jobs, min_ts):
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
        gap_list = self.create_gaps_list(reserved_jobs, min_ts)
        if len(gap_list) == 0:
            return -1

        self.logger.debug(
            r'[Scheduler] Reservation list: %s; Gaps: %s' % (
                reserved_jobs, gap_list))

        # check every gap and return the first one where the new job fits
        for gap in gap_list:
            if gap[1] <= job.submission_time:
                continue
            ts = max(gap[0], job.submission_time)
            if job.nodes <= gap[2] and job.request_walltime <= (gap[1] - ts):
                # there is room for the current job starting with ts
                self.logger.info(
                    r'[Scheduler] Found space for %s: timestamp %d' %
                    (job, ts))
                return ts
        return -1

    def backfill_request(self, stop_job, reserved_jobs, min_ts):
        ''' Base method for requesting a backfill phase. By default the
        base method does not use any backfilling.
        The child methods implement different algorithms for choosing
        jobs to use to fill the space left by the termination of stop_job '''
        return []


class BatchScheduler(Scheduler):
    ''' Reservation based scheduler (default LJF batch scheduler) '''

    def __init__(self, system, batch_size=100, logger=None):
        ''' Constructor method extends the base to specify the batch size,
        i.e. number of jobs in the wait queue to examime to create the
        reservation. Jobs in the waiting queue are ordered based on their
        submission time '''

        super(BatchScheduler, self).__init__(system, logger)
        self.batch_size = batch_size

    def __get_batch_jobs(self):
        ''' Method that returns the first batch_size jobs in the waiting
        queue ordered by their submission time '''

        batch_list = []
        # get all submission times sorted in an increasing order
        submission_times = sorted(
            set([job.submission_time for job in self.wait_queue]))
        for st in submission_times[:self.batch_size]:
            # get all jobs from wait queue with submission time == st
            batch_list += [job for job in self.wait_queue
                           if job.submission_time == st]

        # sort the list by the size of the job (nodes*request_walltime)
        size_list = list(
            set([job.nodes * job.request_walltime for job in batch_list]))
        size_list.sort(reverse=True)
        batch_sorted = []
        for sl in size_list:
            batch_sorted += [job for job in batch_list if job.nodes *
                             job.request_walltime == sl]
        return batch_sorted[:self.batch_size]

    def __create_job_reservation(self, job, reserved_jobs):
        ''' Method that implements a greedy algotithm for finding a place
        for a new job into a reservation window that is given by the
        previously reserved jobs '''

        gap_list = super(BatchScheduler, self).create_gaps_list(
            reserved_jobs, 0)
        if len(gap_list) == 0:
            return -1
        # check every gap and return the first one where the new job fits
        for gap in gap_list:
            ts = gap[0]
            if job.nodes <= gap[2] and job.request_walltime <= (gap[1] - ts):
                # there is room for the current job starting with ts
                self.logger.info(
                    r'[Scheduler] Found inside reservation for %s at ts %d'
                    % (job, ts))
                return ts
        return -1

    def __build_schedule(self, job, reservations):
        ''' Method for extending the existing reservation to include
        a new job. All jobs have to fit in the schedule, thus the reservation
        will be increased if the job does not fit inside the current one '''

        if len(reservations) == 0:
            self.logger.info(
                r'[Scheduler] Found reservation slot for %s at beginning' %
                (job))
            return 0

        end_window = max([reservations[job] + job.request_walltime
                          for job in reservations])
        ts = self.__create_job_reservation(job, reservations)
        if ts != -1:
            return ts
        # check for the end of the schedule for a fit (after all jobs that do
        # not have other jobs starting in front)
        gap_list = super(BatchScheduler, self).create_gaps_list(
            reservations, 0)
        if len(gap_list) > 0:
            end_gaps = [gap for gap in gap_list if gap[1] == end_window]
            for gap in end_gaps:
                if job.nodes <= gap[2]:
                    self.logger.info(
                        r'[Scheduler] Found reservation for %s at timestamp %d'
                        % (job, gap[0]))
                    return gap[0]

        # there is no fit for the job to start anywhere inside the schedule
        # start job after the last job
        self.logger.info(
            r'[Scheduler] Found reservation slot for %s at timestamp %d' %
            (job, end_window))
        return end_window

    def trigger_schedule(self):
        ''' Method for creating a schedule for the first `batch_size` jobs
        in the waiting queue ordered by their submission time.
        The implemented algorithm is greedyly fitting jobs in the batch list
         starting with the largest on the first available slot '''

        batch_jobs = self.__get_batch_jobs()
        selected_jobs = {}
        for job in batch_jobs:
            # find a place for the job in the current schedule
            ts = self.__build_schedule(job, selected_jobs)
            selected_jobs[job] = ts
            self.wait_queue.remove(job)

        # try to fit any of the remaining jobs into the gaps created by the
        # schedule (for the next batch list)
        batch_jobs = self.__get_batch_jobs()
        for job in batch_jobs:
            ts = self.__create_job_reservation(job, selected_jobs)
            if ts != -1:
                selected_jobs[job] = ts
                self.wait_queue.remove(job)

        # return (start_time, job) list in the current batch
        return [(selected_jobs[job], job) for job in selected_jobs]

    def backfill_request(self, stop_job, reservation, min_ts):
        ''' Method that implements a greedy algorithm to find jobs from
        the waiting queue that fit in the space left after the end of the
        `stop_job`. Larger jobs are inspected first and first slot available
        is reserved '''

        reserved_jobs = reservation.copy()
        for job in reserved_jobs:
            if job == stop_job:
                job.free_wasted_space()
        self.logger.info(r'[Backfill %d] Reservations: %s' %
                         (min_ts, reserved_jobs))

        batch_jobs = self.__get_batch_jobs()
        selected_jobs = []
        for job in batch_jobs:
            tm = super(BatchScheduler, self).fit_job_in_schedule(
                job, reserved_jobs, min_ts)
            if tm != -1:
                selected_jobs.append((tm, job))
                reserved_jobs[job] = tm
                self.wait_queue.remove(job)
        return selected_jobs


class OnlineScheduler(Scheduler):
    ''' Online scheduler (default LJF completly online) '''

    def clear_job(self, job):
        ''' Method that overwrites the base one to indicate that a new
        schedule needs to be triggered after each job end '''

        super(OnlineScheduler, self).clear_job(job)
        return 0  # trigger a new schedule starting now (timestamp 0)

    def __get_next_job(self, nodes):
        ''' Method to extract the largest volume job (nodes * requested time)
        in the waiting queue that fits the space given by the `nodes` '''

        try:
            max_volume = max([job.nodes * job.request_walltime for job
                              in self.wait_queue if job.nodes <= nodes])
            largest_jobs = [
                job for job in self.wait_queue if job.nodes *
                job.request_walltime == max_volume and job.nodes <= nodes]
        except BaseException:
            # there are no jobs that fit the given space
            return -1
        # out of the largest jobs get the one submitted first
        min_time = min([job.submission_time for job in largest_jobs])
        return [job for job in largest_jobs
                if job.submission_time == min_time][0]

    def trigger_schedule(self):
        ''' Method for chosing the next jobs to run. For the online
        scheduler, the method iteratively choses the largest job that fits
        in the available nodes in the system until no job fits or there
        are no more nodes left in the system '''

        selected_jobs = []
        if len(self.wait_queue) == 0:
            return []
        free_nodes = self.system.get_free_nodes()
        while free_nodes > 0:
            job = self.__get_next_job(free_nodes)
            self.logger.info(
                r'Reserve next job: %s; Free procs %s' % (job, free_nodes))
            if job == -1:
                break
            selected_jobs.append((0, job))
            self.wait_queue.remove(job)
            free_nodes -= job.nodes
        return selected_jobs

    def fit_job_in_schedule(self, job, reserved_jobs, min_ts):
        ''' Method that overwrites the base class that implements a
        reservation based algorithm. For the base method all jobs
        need to be fitted in the reservation window and cannot exceed
        the end. Online methods do not have this limitation '''

        ts = super(OnlineScheduler, self).fit_job_in_schedule(
            job, reserved_jobs, min_ts)
        if ts != -1:
            return ts
        # check for the end of the schedule for a fit regardless of how long
        # the job might run
        if len(reserved_jobs) == 0:
            return -1
        gap_list = super(OnlineScheduler, self).create_gaps_list(
            reserved_jobs, min_ts)
        if len(gap_list) == 0:
            return -1
        # look only at the gaps at the end of the reservation window (i.e. end
        # of the gap is max)
        end_window = max([gap[1] for gap in gap_list])
        if job.submission_time >= end_window:
            return -1
        end_gaps = [gap for gap in gap_list if gap[1] == end_window]
        for gap in end_gaps:
            if job.nodes <= gap[2]:
                # there is room for the current job starting with ts
                self.logger.info(
                    r'[OnlineScheduler] Found space for %s: timestamp %d' %
                    (job, max(
                        gap[0], job.submission_time)))
                return max(gap[0], job.submission_time)
        return -1
