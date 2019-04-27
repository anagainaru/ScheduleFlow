import unittest
import numpy as np
import logging
from EventQueue import EventQueue
from Runtime import System
from Runtime import ApplicationJob
from Runtime import Runtime
from Scheduler import Scheduler
from Scheduler import OnlineScheduler
from Scheduler import BatchScheduler
import Simulator

# test the event priority queue
class TestEventQueue(unittest.TestCase):
    def test_empty(self):
        q = EventQueue()
        self.assertEqual(q.size(), 0)
        self.assertEqual(q.empty(), True)

    def test_not_tuple(self):
        q = EventQueue()
        with self.assertRaises(AssertionError):
            q.push(10)

    def test_pop_from_empty(self):
        q = EventQueue()
        with self.assertRaises(IndexError):
            q.pop()
        with self.assertRaises(IndexError):
            q.pop_list()

    def test_top_from_empty(self):
        q = EventQueue()
        with self.assertRaises(IndexError):
            q.top()

    def test_pop_list_complete(self):
        q = EventQueue()
        q.push((7, 4))
        q.push((7, 4))
        q.push((7, 5))
        self.assertEqual(len(q.pop_list()), 3)

    def test_pop_list_correct(self):
        q = EventQueue()
        np.random.seed(0)
        for i in range(100):
            q.push((np.random.randint(0, 10), np.random.randint(1, 100)))
        # check that all returned events have the same timestamp
        ts = set([i[0] for i in q.pop_list()])
        self.assertEqual(len(ts), 1)


# test the system class
class TestSystem(unittest.TestCase):
    def test_null_system(self):
        with self.assertRaises(AssertionError):
            System(0)

    def test_fail_start(self):
        s = System(10)
        with self.assertRaises(AssertionError):
            s.start_job(11, 0)

    def test_fail_end(self):
        s = System(10)
        with self.assertRaises(AssertionError):
            s.end_job(1, 0)

    def test_success_start(self):
        s = System(10)
        s.start_job(10, 0)
        self.assertEqual(s.get_free_nodes(), 0)

    def test_success_end(self):
        s = System(10)
        s.start_job(10, 0)
        s.end_job(10, 0)
        self.assertEqual(s.get_free_nodes(), 10)


# test the application class
class TestApplicationJob(unittest.TestCase):
    def test_get_request_time(self):
        apl = ApplicationJob(10, 0, 200, [100], resubmit_factor=1.5)
        self.assertEqual(apl.get_request_time(0), 100)
        self.assertEqual(apl.get_request_time(2), 225)

    def test_get_request_complex(self):
        apl = ApplicationJob(10, 0, 200, [100, 200, 300],
                             resubmit_factor=1.5)
        self.assertEqual(apl.get_request_time(1), 200)
        self.assertEqual(apl.get_request_time(4), 675)
        apl = ApplicationJob(10, 0, 200, [100, 200, 300])
        self.assertEqual(apl.get_request_time(4), 300)

    def test_sequence_overwrite(self):
        apl = ApplicationJob(10, 0, 200, [100],
                             resubmit_factor=1.5)
        with self.assertRaises(AssertionError):
            apl.overwrite_request_sequence([100, 150])
        with self.assertRaises(AssertionError):
            apl.overwrite_request_sequence([200, 150])
        apl.overwrite_request_sequence([190, 200])
        self.assertEqual(apl.get_request_time(1), 190)

    def test_invalid_job(self):
        with self.assertRaises(AssertionError):
            ApplicationJob(0, 0, 10, [11])
        with self.assertRaises(AssertionError):
            ApplicationJob(10, 0, 0, [11])
        with self.assertRaises(AssertionError):
            ApplicationJob(10, 0, 10, [0])
        with self.assertRaises(AssertionError):
            ApplicationJob(10, -2, 10, [11])
        with self.assertRaises(AssertionError):
            ApplicationJob(10, 0, 10, [7, 12, 10])

    def test_invalid_factor(self):
        with self.assertRaises(AssertionError):
            ap = ApplicationJob(7, 5, 10, [11], resubmit_factor=0.2)

    def test_request_list_factor(self):
        ap = ApplicationJob(7, 5, 100, [70, 80, 90], resubmit_factor=2)
        ap.update_submission(0)
        self.assertEqual(ap.request_walltime, 80)
        ap.update_submission(0)
        self.assertEqual(ap.request_walltime, 90)
        ap.update_submission(0)
        self.assertEqual(ap.request_walltime, 180)


# test the basic scheduler class
class TestScheduler(unittest.TestCase):
    def test_invalid_stop(self):
        sch = Scheduler(System(10))
        with self.assertRaises(AssertionError):
            sch.clear_job(ApplicationJob(7, 5, 10, [11]))

    def test_invalid_jobs(self):
        sch = Scheduler(System(10))
        with self.assertRaises(AssertionError):
            sch.submit_job(ApplicationJob(20, 5, 10, [11]))

    def test_empty_schedule(self):
        for SchedulerType in Scheduler.__subclasses__():
            sch = SchedulerType(System(10))
            ap_list = sch.trigger_schedule()
            self.assertEqual(len(ap_list), 0)

    def test_gaps_full(self):
        sch = Scheduler(System(10))
        reservation = {}
        for i in range(5):
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
        gap_list = sch.create_gaps_list(reservation, 0)
        free_nodes = max([gap[2] for gap in gap_list])
        self.assertEqual(free_nodes, 0)

    def test_gaps_random(self):
        sch = Scheduler(System(10))
        np.random.seed(0)
        reservation = {}
        for i in list(range(5)) + list(range(6, 10)):
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
        # add random number of one node jobs with execution times <= 50
        num_jobs = np.random.randint(1, 10)
        for i in range(num_jobs):
            reservation[ApplicationJob(1, 0, 50 - i, [50 - i])] = 250
        gap_list = sch.create_gaps_list(reservation, 0)
        for i in range(num_jobs):
            self.assertEqual(
                i + 1, len([gap for gap in gap_list if gap[2] == (9 - i)]))


# test the online scheduler class
class TestOnlineScheduler(unittest.TestCase):
    def test_longest_job(self):
        sch = OnlineScheduler(System(100))
        max_exec = 0
        np.random.seed(0)
        for i in range(10):
            exect = np.random.randint(10, 100)
            max_exec = exect if max_exec < exect else max_exec
            sch.submit_job(ApplicationJob(51, 0, exect, [exect]))
        ap_list = sch.trigger_schedule()
        self.assertNotEqual(len(ap_list), 0)
        self.assertEqual(ap_list[0][1].request_walltime, max_exec)

    def test_largest_job(self):
        sch = OnlineScheduler(System(100))
        max_procs = 0
        np.random.seed(0)
        for i in range(10):
            procs = np.random.randint(10, 100)
            max_procs = procs if max_procs < procs else max_procs
            sch.submit_job(ApplicationJob(procs, 0, 10, [10]))

        ap_list = sch.trigger_schedule()
        self.assertNotEqual(len(ap_list), 0)
        self.assertEqual(ap_list[0][1].nodes, max_procs)

    def test_one_node_jobs(self):
        sch = OnlineScheduler(System(100))
        np.random.seed(0)
        num_jobs = np.random.randint(1, 101)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            sch.submit_job(ApplicationJob(
                1, np.random.randint(0, 100), exect, [exect]))
        ap_list = sch.trigger_schedule()
        self.assertEqual(len(ap_list), num_jobs)

    def test_nofit_in_schedule(self):
        sch = OnlineScheduler(System(10))
        reservation = {}
        for i in range(5):
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
        ret = sch.fit_job_in_schedule(
            ApplicationJob(1, 0, 50, [50]), reservation, 0)
        self.assertEqual(ret, -1)

    def test_fit_middle(self):
        sch = OnlineScheduler(System(10))
        reservation = {}
        for i in list(range(5)) + list(range(6, 10)):
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
        # add random number of one node jobs ( < 8 total jobs )
        # with execution times <= 50
        num_jobs = np.random.randint(1, 8)
        for i in range(num_jobs):
            reservation[ApplicationJob(1, 0, 50 - i, [50 - i])] = 250
        # try to fit an application of execution time = 40 and 2 nodes whose
        # submission time is 5 over when the gap is available
        ret = sch.fit_job_in_schedule(
            ApplicationJob(2, 255, 40, [40]), reservation, 0)
        self.assertEqual(ret, 255)

    def test_fit_end(self):
        sch = OnlineScheduler(System(10))
        reservation = {}
        for i in range(5):
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
        reservation[ApplicationJob(4, 0, 10, [10])] = 250
        # fit a job that is larger than the gap at the end
        ret = sch.fit_job_in_schedule(
            ApplicationJob(6, 255, 50, [50]), reservation, 0)
        self.assertEqual(ret, 255)
        ret = sch.fit_job_in_schedule(
            ApplicationJob(6, 265, 50, [50]), reservation, 0)
        self.assertEqual(ret, -1)


# test the online scheduler class
class TestBatchScheduler(unittest.TestCase):
    def test_nofit_end(self):
        sch = BatchScheduler(System(10), 15)
        reservation = {}
        for i in range(5):
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
        reservation[ApplicationJob(4, 0, 10, [10])] = 250
        # fit a job that is larger than the gap at the end
        ret = sch.fit_job_in_schedule(
            ApplicationJob(6, 0, 50, [50]), reservation, 0)
        self.assertEqual(ret, -1)

    def test_fit_end(self):
        sch = BatchScheduler(System(10), 15)
        reservation = {}
        for i in range(5):
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
            reservation[ApplicationJob(5, 0, 50, [50])] = i * 50
        reservation[ApplicationJob(4, 0, 10, [10])] = 250
        # fit a job that is larger than the gap at the end (submission time is
        # 5 over when the gap is available)
        ret = sch.fit_job_in_schedule(
            ApplicationJob(6, 255, 5, [5]), reservation, 0)
        self.assertEqual(ret, 255)

    def test_one_node_jobs(self):
        sch = BatchScheduler(System(100), 105)
        np.random.seed(0)
        num_jobs = np.random.randint(1, 101)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            sch.submit_job(ApplicationJob(
                1, np.random.randint(0, 100), exect, [exect]))
        ap_list = sch.trigger_schedule()
        self.assertEqual(len(ap_list), num_jobs)
        # all jobs start at timestamp 0
        self.assertEqual(max([job[0] for job in ap_list]), 0)

    def test_system_wide_jobs(self):
        sch = BatchScheduler(System(10), 105)
        np.random.seed(0)
        num_jobs = np.random.randint(1, 101)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            procs = np.random.randint(6, 10)
            sch.submit_job(ApplicationJob(
                procs, 0, exect, [exect]))
        ap_list = sch.trigger_schedule()  # ap_list = (ts, job)
        ap_list.sort()
        exec_order = [
            job[1].nodes *
            job[1].request_walltime for job in ap_list]
        # jobs executed in order of their volume
        self.assertTrue(all(exec_order[i] >= exec_order[i + 1]
                            for i in range(len(exec_order) - 1)))
        # jobs reserved to execute one after the other
        ts = 0
        for job in ap_list:
            self.assertEqual(ts, job[0])
            ts += job[1].request_walltime

    def test_batch_size(self):
        sch = BatchScheduler(System(10), 10)
        np.random.seed(0)
        num_jobs = np.random.randint(11, 20)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            procs = np.random.randint(6, 10)
            sch.submit_job(ApplicationJob(
                procs, np.random.randint(0, 100), exect, [exect]))
        ap_list = sch.trigger_schedule()
        self.assertTrue(len(ap_list), 10)

    def test_batch_small(self):
        sch = BatchScheduler(System(10), 10)
        np.random.seed(0)
        num_jobs = np.random.randint(11, 20)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            procs = np.random.randint(6, 9)
            sch.submit_job(ApplicationJob(
                procs, np.random.randint(0, 100), exect, [exect]))
        # add a small job that will fit inside the schedule of the first 10
        # jobs
        sch.submit_job(ApplicationJob(1, np.random.randint(0, 100),
                                      10, [10]))
        ap_list = sch.trigger_schedule()
        self.assertTrue(len(ap_list), 11)


# test the runtime class
class TestRuntime(unittest.TestCase):
    def test_restore_job_default(self):
        for SchedulerType in Scheduler.__subclasses__():
            sch = SchedulerType(System(10))
            runtime = Runtime([ApplicationJob(
                10, 0, 100, [80, 100, 120],
                resubmit_factor=1.5)])
            runtime(sch)
            workload = runtime.get_stats()
            for job in workload:
                self.assertEqual(job.submission_time, 0)
                self.assertEqual(job.request_walltime, 80)
                self.assertEqual(job.request_sequence[0], 100)

    def test_empty_workload(self):
        for SchedulerType in Scheduler.__subclasses__():
            sch = SchedulerType(System(10))
            runtime = Runtime(set())
            runtime(sch)
            workload = runtime.get_stats()
            self.assertEqual(len(workload), 0)

    def test_one_job_success(self):
        for SchedulerType in Scheduler.__subclasses__():
            sch = SchedulerType(System(10))
            np.random.seed(0)
            start_time = np.random.randint(1, 100)
            execution_time = np.random.randint(1, 100)
            runtime = Runtime([ApplicationJob(
                7,
                start_time,
                execution_time,
                [execution_time])])
            runtime(sch)
            workload = runtime.get_stats()
            self.assertEqual(len(workload), 1)
            # assert job did not fail, it started when it was
            # submitted and ended after the execution time
            for job in workload:
                self.assertEqual(len(workload[job]), 1)
                self.assertEqual(workload[job][0][0], start_time)
                self.assertEqual(workload[job][0][1],
                                 start_time + execution_time)

    def test_one_job_failed(self):
        for SchedulerType in Scheduler.__subclasses__():
            sch = SchedulerType(System(10))
            np.random.seed(0)
            start_time = np.random.randint(1, 100)
            execution_time = np.random.randint(11, 100)
            runtime = Runtime([ApplicationJob(
                7,
                start_time,
                execution_time,
                [execution_time - 10],
                resubmit_factor=1.5)])
            runtime(sch)
            workload = runtime.get_stats()
            self.assertEqual(len(workload), 1)
            for job in workload:
                self.assertGreater(len(workload[job]), 1)
                # all runs of the job except last should be failures
                for i in range(len(workload[job]) - 1):
                    self.assertGreater(
                        workload[job][i][0] + execution_time,
                        workload[job][i][1])
                i += 1
                self.assertLessEqual(
                    workload[job][i][0] + execution_time, workload[job][i][1])

    def test_many_one_node_jobs(self):
        for SchedulerType in Scheduler.__subclasses__():
            sch = SchedulerType(System(100))
            apl = set()
            for i in range(100):
                apl.add(
                    ApplicationJob(
                        1,
                        i * 10,
                        1000 - i * 10,
                        [1000 - i * 10]))
            runtime = Runtime(apl)
            runtime(sch)
            workload = runtime.get_stats()
            exec_order = sorted([workload[job][0][0] for job in workload])
            self.assertListEqual(exec_order, [i * 10 for i in range(100)])

    def test_same_start(self):
        for SchedulerType in Scheduler.__subclasses__():
            sch = SchedulerType(System(100))
            np.random.seed(0)
            apl = set()
            for i in range(100):
                execution_time = np.random.randint(11, 100)
                apl.add(ApplicationJob(
                    1, 1 + i * 10, execution_time, [execution_time]))
            # aplication in the beginning that will make all other be in the
            # wait queue for the next schedule trigger
            apl.add(ApplicationJob(100, 0, 1000, [1000]))
            runtime = Runtime(apl)
            runtime(sch)
            workload = runtime.get_stats()
            self.assertEqual(
                len([job for job in workload if workload[job][0][0] == 0]),
                1)
            self.assertEqual(
                len([job for job in workload if workload[job][0][0] == 1000]),
                100)

    def test_nofail_random(self):
        sch = OnlineScheduler(System(100))
        apl = set()
        np.random.seed(0)
        for i in range(100):
            execution_time = np.random.randint(11, 100)
            apl.add(ApplicationJob(np.random.randint(51, 100),
                                   0, execution_time, [execution_time]))
        runtime = Runtime(apl)
        runtime(sch)
        workload = runtime.get_stats()
        # get (stat_time, volume) for each job and sort by start time
        exec_order = [(workload[job][0][0], job.nodes * job.request_walltime)
                      for job in workload]
        exec_order.sort()
        exec_order = [i[1] for i in exec_order]
        self.assertTrue(all(exec_order[i] >= exec_order[i + 1]
                            for i in range(len(exec_order) - 1)))

    def test_fail_job_online(self):
        apl = set()
        np.random.seed(0)
        for i in range(10):
            execution_time = np.random.randint(11, 100)
            apl.add(ApplicationJob(np.random.randint(6, 11),
                                   0, execution_time, [execution_time],
                                   resubmit_factor=1.5))
        fail_job = ApplicationJob(np.random.randint(1, 11), 0, 100, [90],
                                  resubmit_factor=1.5)
        apl.add(fail_job)

        sch = OnlineScheduler(System(10))
        runtime = Runtime(apl)
        runtime(sch)
        workload = runtime.get_stats()
        # failed job runs after its previous instance
        self.assertEqual(workload[fail_job][0][1], workload[fail_job][1][0])

    def test_fail_job_batch(self):
        apl = set()
        np.random.seed(0)
        for i in range(10):
            execution_time = np.random.randint(11, 100)
            apl.add(ApplicationJob(np.random.randint(1, 11),
                                   0, execution_time, [execution_time]))
        fail_job = ApplicationJob(np.random.randint(9, 11), 0, 100, [90],
                                  resubmit_factor=1.5)
        apl.add(fail_job)

        sch = BatchScheduler(System(10))
        runtime = Runtime(apl)
        runtime(sch)
        workload = runtime.get_stats()
        # failed job ran last
        last_run = max([workload[job][0][1] for job in workload])
        self.assertEqual(workload[fail_job][len(
            workload[fail_job]) - 1][0], last_run)

    def test_backfill_nojob(self):
        for SchedulerType in Scheduler.__subclasses__():
            apl = set()
            for i in range(10):
                if i == 7:
                    apl.add(ApplicationJob(5, 0, 50, [50]))
                    seljob = ApplicationJob(5, 0, 50, [60])
                    apl.add(seljob)
                apl.add(ApplicationJob(5, 0, 50, [50]))
                apl.add(ApplicationJob(5, 0, 50, [50]))

            sch = SchedulerType(System(10))
            runtime = Runtime(apl)
            runtime(sch)
            workload = runtime.get_stats()
            # check that first job ran is id 15
            first_job = [job for job in workload if workload[job][0][0] == 0]

            self.assertIn(seljob, first_job)
            # online covers the gap, batch does not
            cnt = 0
            if SchedulerType == BatchScheduler:
                cnt = 1
            self.assertEqual(
                len([job for job in workload if workload[job][0][0] == 60]),
                cnt)

    def test_backfill_jobs(self):
        # two backfill jobs running over a gap created by one job
        sch = BatchScheduler(System(10), 9)
        apl = set()
        for i in range(5):
            time = 50
            if i == 3:
                time = 30
            apl.add(ApplicationJob(5, 0, 50, [50]))
            apl.add(ApplicationJob(5, 0, time, [50]))
        apl.add(ApplicationJob(5, 0, 10, [10]))
        apl.add(ApplicationJob(5, 0, 10, [10]))
        runtime = Runtime(apl)
        runtime(sch)
        workload = runtime.get_stats()
        # check that all jobs are executed before timestamp 250
        self.assertTrue(max([workload[job][len(workload[job]) - 1][1]
                             for job in workload]) <= 250)

    def test_backfill_gaps(self):
        # one backfill jobs running over gaps created by two jobs
        sch = BatchScheduler(System(10), 10)
        apl = set()
        for i in range(5):
            if i == 2:
                continue
            apl.add(ApplicationJob(5, 0, 50, [50]))
            apl.add(ApplicationJob(5, 0, 50, [50]))
        apl.add(ApplicationJob(5, 0, 50, [60]))
        apl.add(ApplicationJob(5, 0, 50, [70]))
        apl.add(ApplicationJob(10, 0, 10, [10]))
        runtime = Runtime(apl)
        runtime(sch)
        workload = runtime.get_stats()
        # check that all jobs are executed before timestamp 250
        self.assertTrue(max([workload[job][len(workload[job]) - 1][1]
                             for job in workload]) <= 270)

    def test_starvation_online(self):
        sch = OnlineScheduler(System(10))
        apl = set()
        apl.add(ApplicationJob(5, 0, 100, [100]))
        apl.add(ApplicationJob(5, 0, 50, [50]))
        for i in range(1, 5):
            apl.add(ApplicationJob(5, 1, 100, [100]))
            apl.add(ApplicationJob(5, 1, 100, [100]))
        apl.add(ApplicationJob(10, 1, 100, [100]))
        apl.add(ApplicationJob(5, 10, 40, [40]))
        runtime = Runtime(apl)
        runtime(sch)
        workload = runtime.get_stats()
        for job in workload:
            if job.job_id == 30:
                # small job is not chosen for execution
                # until the end of the batch
                self.assertEqual(workload[job][0][0], 450)
            if job.job_id == 20:
                # large job does not have space to run until all jobs finish
                self.assertEqual(workload[job][0][0], 500)

    def test_starvation_batch(self):
        sch = BatchScheduler(System(10))
        apl = set()
        apl.add(ApplicationJob(5, 0, 100, [100]))
        apl.add(ApplicationJob(5, 0, 50, [50]))
        for i in range(1, 5):
            apl.add(ApplicationJob(5, 1, 100, [100]))
            apl.add(ApplicationJob(5, 1, 100, [100]))
        apl.add(ApplicationJob(10, 1, 100, [100]))
        apl.add(ApplicationJob(5, 10, 40, [40]))
        runtime = Runtime(apl)
        runtime(sch)
        workload = runtime.get_stats()
        for job in workload:
            if job.job_id == 30:
                # small job is squeezed inside the first batch
                self.assertEqual(workload[job][0][0], 50)
            if job.job_id == 20:
                # large job is scheduled at the beginning of the second batch
                self.assertEqual(workload[job][0][0], 100)

    def test_no_fit_in_schedule(self):
        sch = BatchScheduler(System(10))
        apl = set()
        selap = ApplicationJob(3, 5402, 600, [600])
        apl.add(ApplicationJob(2, 0, 13000, [13000]))
        apl.add(ApplicationJob(3, 0, 6800, [7200]))
        apl.add(ApplicationJob(5, 0, 5500, [5500]))
        apl.add(ApplicationJob(3, 5400, 7200, [7200]))
        apl.add(selap)
        apl.add(ApplicationJob(5, 5401, 5800, [5800]))
        runtime = Runtime(apl)
        runtime(sch)
        workload = runtime.get_stats()
        workload_job4 = workload[selap]
        self.assertTrue(workload_job4[0][0] >= 13000)

    def test_cascading_failures(self):
        sch = BatchScheduler(System(10))
        runtime = Runtime([ApplicationJob(5, 0, 500, [100],
                                          resubmit_factor=1.5)])
        runtime(sch)
        workload = runtime.get_stats()
        for job in workload:
            self.assertEqual(len(workload[job]), 5)
            expected_start = job.submission_time
            for run in workload[job]:
                self.assertEqual(run[0], expected_start)
                expected_start = run[1]

    def test_reservation_build(self):
        expected_start_job2 = [1200, 3650]
        for i in range(2):
            sch = BatchScheduler(System(10))
            seljob = ApplicationJob(5, 0, 700, [650], resubmit_factor=1.5)
            runtime = Runtime([ApplicationJob(5, 0, 1400, [1200+i],
                                              resubmit_factor=1.5),
                               ApplicationJob(5, 0, 1400, [1200],
                                              resubmit_factor=1.5),
                               seljob])
            runtime(sch)
            workload = runtime.get_stats()
            for job in workload:
                if job != seljob:
                    continue
                self.assertEqual(len(workload[job]), 2)
                for i in range(len(expected_start_job2)):
                    self.assertEqual(workload[job][i][0],
                                     expected_start_job2[i])

# test the simulator class
class TestSimulator(unittest.TestCase):
    def test_stats_engine(self):
        sch = BatchScheduler(System(10))
        runtime = Runtime([ApplicationJob(6, 0, 500, [1000]),
                           ApplicationJob(6, 0, 1000, [2000])])
        runtime(sch)
        workload = runtime.get_stats()
        stats = Simulator.StatsEngine(10)
        stats.set_execution_output(workload)
        self.assertEqual(stats.total_makespan(), 2500)
        self.assertEqual(stats.system_utilization(), 0.36)
        self.assertEqual(stats.average_job_utilization(), 0.5)
        self.assertEqual(stats.average_job_wait_time(), 1000)
        self.assertEqual(stats.average_job_response_time(), 1750)
        self.assertEqual(stats.average_job_stretch(), 3)
        self.assertEqual(stats.total_failures(), 0)

    def test_create_scenario(self):
        sim = Simulator.Simulator()
        sim.logger.setLevel(logging.CRITICAL)
        ret = sim.create_scenario("test", BatchScheduler(System(10)))
        self.assertEqual(len(sim.job_list), 0)
        self.assertEqual(ret, 0)
        apl = [ApplicationJob(6, 0, 500, [1000]),
               ApplicationJob(6, 0, 500, [1000])]
        ret = sim.create_scenario("test", BatchScheduler(System(10)),
                                  job_list=apl)
        self.assertEqual(len(sim.job_list), 2)
        self.assertEqual(ret, 2)

    def test_additional_jobs(self):
        sim = Simulator.Simulator()
        sim.logger.setLevel(logging.CRITICAL)
        sim.create_scenario("test", BatchScheduler(System(10)))
        apl = [ApplicationJob(6, 0, 500, [1000]),
               ApplicationJob(6, 0, 500, [1000])]
        ret = sim.add_applications(apl)
        ret = sim.add_applications(apl)
        self.assertEqual(ret, 2)
        apl = [ApplicationJob(6, 0, 500, [1000]),
               ApplicationJob(6, 0, 500, [1000])]
        ret = sim.add_applications(apl)
        self.assertEqual(ret, 4)

    def test_run_scenario(self):
        sim = Simulator.Simulator()
        with self.assertRaises(AssertionError):
            ret = sim.run()
        apl = [ApplicationJob(6, 0, 500, [1000]),
               ApplicationJob(6, 0, 500, [1000])]
        sim.create_scenario("test", BatchScheduler(System(10)),
                            job_list=apl)
        ret = sim.run()
        self.assertEqual(ret, 0)

    def test_simulation_correctness(self):
        sim = Simulator.Simulator(check_correctness=True)
        apl = [ApplicationJob(6, 0, 500, [200], resubmit_factor=1.5),
               ApplicationJob(6, 0, 500, [1000])]
        sim.create_scenario("test", BatchScheduler(System(10)),
                            job_list=apl)
        ret = sim.run()
        self.assertEqual(sim.test_correctness(), 0)


if __name__ == '__main__':
    unittest.main()
