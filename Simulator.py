import logging
import math
import VizEngine
import Runtime


class StatsEngine():
    def __init__(self, total_nodes):
        self.__execution_log = {}
        self.__makespan = -1
        self.__total_nodes = total_nodes

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
        assert (len(execution_log)>0), "Simulation execution log is NULL"
        self.__execution_log = execution_log
        self.__makespan = max([max([i[1] for i in self.__execution_log[job]])
                               for job in self.__execution_log])

    def total_makespan(self):
        return self.__makespan

    def total_failures(self):
        total_failures = sum([len(self.__execution_log[job])-1 for job in
                              self.__execution_log])
        return total_failures

    def system_utilization(self):
        total_runtime = sum([job.walltime * job.nodes for job in
                             self.__execution_log])
        return total_runtime / (self.__makespan * self.__total_nodes)

    def average_job_wait_time(self):
        total_wait = 0
        total_runs = 0
        for job in self.__execution_log:
            submission = 0
            apl_wait = 0
            for instance in self.__execution_log[job]:
                apl_wait += instance[0] - submission
                submission = instance[1]
            total_wait += apl_wait
            total_runs += len(self.__execution_log[job])
        return total_wait / max(1, total_runs)

    def average_job_utilization(self):
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
        makespan = 0
        for job in self.__execution_log:
            runs = self.__execution_log[job]
            makespan += (runs[len(runs) - 1][1] - job.submission_time)
        return makespan / max(1, len(self.__execution_log))

    def average_job_stretch(self):
        stretch = 0
        for job in self.__execution_log:
            runs = self.__execution_log[job]
            stretch += ((runs[len(runs) - 1][1] - job.submission_time) /
                        job.walltime)
        return stretch / max(1, len(self.__execution_log))

    def print_to_file(self, file_handler, scenario):
        if len(self.__execution_log) == 0:
            return -1
        file_handler.write(
            "%s : %.2f : %.2f : %.2f : %.2f : %.2f : %.2f : %d\n" %
            (scenario, self.total_makespan(),
             self.system_utilization(),
             self.average_job_utilization(),
             self.average_job_response_time(),
             self.average_job_stretch(),
             self.average_job_wait_time(),
             self.total_failures()))


class Simulator():
    def __init__(self, loops=1, generate_gif=False, check_correctness=False,
                 output_file_handler=None):
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

    def create_scenario(self, scenario_name, scheduler, job_list=[]):
        self.__scheduler = scheduler
        self.__system = scheduler.system
        self.job_list = []
        self.__execution_log = {}
        self.__scenario_name = scenario_name

        self.stats = StatsEngine(self.__system.get_total_nodes())
        if self.__generate_gif:
            self.__viz_handler = VizEngine.VizualizationEngine(
                    self.__system.get_total_nodes())

        return self.add_applications(job_list)

    def get_execution_log(self):
        return self.__execution_log

    def add_applications(self, job_list):
        change_log = []
        for new_job in job_list:
            if new_job in self.job_list:
                self.logger.warning("Job %s is already included "
                                    "in the sumlation." %(new_job))
                continue
            job_id_list = [job.job_id for job in self.job_list]
            if new_job.job_id in job_id_list:
                new_id = max(job_id_list) + 1 #len(change_log) + 1
                self.logger.warning("Jobs cannot share the same ID. "
                                    "Updated job %d with ID %d." %
                                    (new_job.job_id, new_id))
                change_log.append((new_job.job_id, new_id))
                new_job.job_id = new_id
            self.job_list.append(new_job)
        return change_log

    def __sanity_check_job_execution(self, execution_list, job):
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
            if not math.isclose(end-start, requested_time,
                                rel_tol=1e-3):
                return False
            requested_time = job.get_request_time(i + 1)

        # check len of succesful execution (last)
        start = execution_list[len(execution_list)-1][0]
        end = execution_list[len(execution_list)-1][1]
        if not math.isclose(end-start, job.walltime,
                            rel_tol=1e-3):
            return False
        return True

    def __sainity_check_schedule(self, workload):
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

    def test_correctness(self):
        ''' Method for checking the correctness of the execution of a
        given list of jobs. Job list contains the jobs with their initial
        information, workload contains execution information for each
        job '''
        assert (len(self.__execution_log) > 0), \
            "ERR - Trying to test correctness on an empty execution log"

        check_fail = 0
        for job in self.__execution_log:
            pass_check = self.__sanity_check_job_execution(
                self.__execution_log[job], job)
            if not pass_check:
                self.logger.error("%s did not pass the sanity check: %s" %
                                  (job, self.__execution_log[job]))
                check_fail += 1
                continue

        check_fail += self.__sainity_check_schedule(self.__execution_log)
        return check_fail

    def run(self):
        assert (len(self.job_list)>0), "Cannot run an empty scenario"
        check = 0
        for i in range(self.__loops):
            runtime = Runtime.Runtime(self.job_list)
            runtime(self.__scheduler)
            self.__execution_log = runtime.get_stats()

            if self.__check_correctness:
                check += self.test_correctness()
                if check > 0:
                    self.logger.debug("FAIL correctness test (loop %d)" % (i))
                    continue

            self.stats.set_execution_output(self.__execution_log)
            self.logger.info(self.stats)
            if self.__fp is not None:
                self.stats.print_to_file(self.__fp, self.__scenario_name)

        if check == 0:
            self.logger.info("PASS correctness test")

        if self.__generate_gif and check == 0:
            if self.horizontal_ax != -1:
                self.__viz_handler.set_horizontal_ax_limit(
                    self.horizontal_ax)
            self.__viz_handler.set_execution_log(self.__execution_log)
            self.horizontal_ax = self.__viz_handler.generate_scenario_gif(
                self.__scenario_name)
            self.logger.info(r"GIF generated draw/%s" % (self.__scenario_name))
        return check
