import unittest
import numpy as np
import logging
from _intScheduleFlow import EventQueue
from _intScheduleFlow import Runtime
from _intScheduleFlow import StatsEngine
from _intScheduleFlow import WaitingQueue
import ScheduleFlow


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


# test the waiting queue class
class TestWaitingQueue(unittest.TestCase):
    def test_add(self):
        wq = WaitingQueue()
        self.assertEqual(wq.total_jobs(), 0)
        wq.add(ScheduleFlow.Application(10, 0, 1800, [4500]))
        wq.add(ScheduleFlow.Application(10, 0, 1800, [1800]))
        wq.add(ScheduleFlow.Application(2, 0, 800, [500]))
        self.assertEqual(wq.total_priority_jobs(), 1)
        self.assertEqual(wq.total_secondary_jobs(), 2)
    
    def test_invalid_wq(self):
        with self.assertRaises(AssertionError):
            wq = WaitingQueue(total_queues=0)

    def test_remove_fail(self):
        wq = WaitingQueue()
        self.assertEqual(len(wq.get_priority_jobs()), 0)
        with self.assertRaises(AssertionError):
            wq.remove(ScheduleFlow.Application(10, 0, 1800, [4500]))

    def test_remove(self):
        wq = WaitingQueue()
        job_list = [] 
        job_list.append(ScheduleFlow.Application(10, 0, 1800, [4500]))
        job_list.append(ScheduleFlow.Application(10, 0, 1800, [1800]))
        job_list.append(ScheduleFlow.Application(2, 0, 800, [500]))
        for job in job_list:
            wq.add(job)
        wq.remove(job_list[0])
        self.assertEqual(wq.total_priority_jobs(),0)
        with self.assertRaises(AssertionError):
            wq.remove(job_list[0])
        wq.remove(job_list[1])
        self.assertEqual(wq.total_secondary_jobs(),1)

    def test_update_empty(self):
        wq = WaitingQueue()
        wq.add(ScheduleFlow.Application(10, 0, 1800, [1800]))
        wq.add(ScheduleFlow.Application(2, 0, 800, [500]))
        wq.update_priority(0)
        self.assertEqual(len(wq.get_priority_jobs()),0)
        wq.fill_priority_queue() 
        self.assertEqual(len(wq.get_priority_jobs()),1)

    def test_update_priority(self):
        wq = WaitingQueue()
        wq.add(ScheduleFlow.Application(10, 0, 1800, [4500]))
        wq.add(ScheduleFlow.Application(10, 100, 1800, [1800]))
        wq.add(ScheduleFlow.Application(2, 0, 800, [500]))
        wq.update_priority(0) 
        self.assertEqual(len(wq.get_priority_jobs()),1)
        wq.update_priority(1900) 
        self.assertEqual(len(wq.get_priority_jobs()),2)

    def test_one_queue(self):
        wq = WaitingQueue(total_queues=1)
        wq.add(ScheduleFlow.Application(10, 0, 3600, [4500]))
        wq.add(ScheduleFlow.Application(10, 0, 180, [180]))
        self.assertEqual(wq.total_priority_jobs(), 2)
        wq.update_priority(3600)
        self.assertEqual(wq.total_priority_jobs(), 2)

    def test_multiple_queues(self):
        wq = WaitingQueue(total_queues=3)
        wq.add(ScheduleFlow.Application(10, 0, 4300, [4500]))
        wq.add(ScheduleFlow.Application(10, 0, 1200, [1800]))
        wq.add(ScheduleFlow.Application(10, 1900, 3800, [3500]))
        self.assertEqual(wq.total_priority_jobs(), 1)
        self.assertEqual(wq.total_secondary_jobs(), 2)
        self.assertEqual(len(wq.get_secondary_jobs()), 1)
        wq.update_priority(1900)
        self.assertEqual(len(wq.get_secondary_jobs()), 2)
        wq.update_priority(3800)
        self.assertEqual(wq.total_priority_jobs(), 3)


# test the system class
class TestSystem(unittest.TestCase):
    def test_null_system(self):
        with self.assertRaises(AssertionError):
            ScheduleFlow.System(0)

    def test_fail_start(self):
        s = ScheduleFlow.System(10)
        with self.assertRaises(AssertionError):
            s.start_job(11, 0)

    def test_fail_end(self):
        s = ScheduleFlow.System(10)
        with self.assertRaises(AssertionError):
            s.end_job(1, 0)

    def test_success_start(self):
        s = ScheduleFlow.System(10)
        s.start_job(10, 0)
        self.assertEqual(s.get_free_nodes(), 0)

    def test_success_end(self):
        s = ScheduleFlow.System(10)
        s.start_job(10, 0)
        s.end_job(10, 0)
        self.assertEqual(s.get_free_nodes(), 10)


# test the application class
class TestApplication(unittest.TestCase):
    def test_get_request_time(self):
        job_list = ScheduleFlow.Application(10, 0, 200, [100],
                                            resubmit_factor=1.5)
        self.assertEqual(job_list.get_request_time(0), 100)
        self.assertEqual(job_list.get_request_time(2), 225)

    def test_get_request_complex(self):
        job_list = ScheduleFlow.Application(10, 0, 200, [100, 200, 300],
                                            resubmit_factor=1.5)
        self.assertEqual(job_list.get_request_time(1), 200)
        self.assertEqual(job_list.get_request_time(4), 675)
        job_list = ScheduleFlow.Application(10, 0, 200, [100, 200, 300])
        self.assertEqual(job_list.get_request_time(4), 300)

    def test_sequence_overwrite(self):
        job_list = ScheduleFlow.Application(10, 0, 200, [100],
                                            resubmit_factor=1.5)
        with self.assertRaises(AssertionError):
            job_list.overwrite_request_sequence([100, 150])
        with self.assertRaises(AssertionError):
            job_list.overwrite_request_sequence([200, 150])
        job_list.overwrite_request_sequence([190, 200])
        self.assertEqual(job_list.get_request_time(1), 190)

    def test_invalid_job(self):
        with self.assertRaises(AssertionError):
            ScheduleFlow.Application(0, 0, 10, [11])
        with self.assertRaises(AssertionError):
            ScheduleFlow.Application(10, 0, 0, [11])
        with self.assertRaises(AssertionError):
            ScheduleFlow.Application(10, 0, 10, [0])
        with self.assertRaises(AssertionError):
            ScheduleFlow.Application(10, -2, 10, [11])
        with self.assertRaises(AssertionError):
            ScheduleFlow.Application(10, 0, 10, [7, 12, 10])
        with self.assertRaises(AssertionError):
            ScheduleFlow.Application(10, 0, 10, [])

    def test_invalid_factor(self):
        with self.assertRaises(AssertionError):
            ScheduleFlow.Application(7, 5, 10, [11],
                                     resubmit_factor=0.2)

    def test_request_list_factor(self):
        ap = ScheduleFlow.Application(7, 5, 100, [70, 80, 90],
                                      resubmit_factor=2)
        ap.update_submission(0)
        self.assertEqual(ap.request_walltime, 80)
        ap.update_submission(0)
        self.assertEqual(ap.request_walltime, 90)
        ap.update_submission(0)
        self.assertEqual(ap.request_walltime, 180)


# test the ScheduleGaps class
class TestScheduleGaps(unittest.TestCase):
    def test_add_jobs(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        reservations = {}
        reservations[ScheduleFlow.Application(5, 0, 2, [5])] = 0
        reservations[ScheduleFlow.Application(3, 0, 1, [2])] = 3
        reservations[ScheduleFlow.Application(9, 0, 3, [5])] = 5
        reservations[ScheduleFlow.Application(1, 0, 2, [5])] = 5
        reservations[ScheduleFlow.Application(10, 0, 3, [5])] = 10
        gaps = sch.gaps_list.update(reservations, -1)
        self.assertTrue(len(gaps) == 2)
        self.assertEqual(set([i[2] for i in gaps]), set([2, 5]))
        gaps = sch.gaps_list.update({ScheduleFlow.Application(2, 0, 1, [1]): 4}, -1)
        self.assertEqual(max([i[1]-i[0] for i in gaps]), 4)

    def test_add_job_over(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        reservations = {}
        reservations[ScheduleFlow.Application(3, 0, 5, [5])] = 3
        reservations[ScheduleFlow.Application(7, 0, 10, [10])] = 0
        gaps = sch.gaps_list.update(reservations, -1)
        self.assertTrue(len(gaps) == 2)
        self.assertEqual(min([i[0] for i in gaps]), 0)
        self.assertEqual(max([i[1] for i in gaps]), 10)

    def test_remove_middle_jobs(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        reservations = {}
        reservations[ScheduleFlow.Application(5, 0, 2, [5])] = 0
        reservations[ScheduleFlow.Application(3, 0, 1, [2])] = 3
        reservations[ScheduleFlow.Application(9, 0, 3, [5])] = 5
        reservations[ScheduleFlow.Application(1, 0, 2, [5])] = 5
        reservations[ScheduleFlow.Application(10, 0, 3, [5])] = 10
        gaps = sch.gaps_list.update(reservations, -1)

        gaps = sch.gaps_list.update({ScheduleFlow.Application(1, 0, 2, [5]): 5}, 1)
        self.assertTrue(len(gaps) == 3)
        self.assertEqual([i for i in gaps if i[0]==7][0], [7, 10, 1])

    def test_reservation_batch(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        reservations = {}
        reservations[ScheduleFlow.Application(5, 0, 2, [5])] = 0
        reservations[ScheduleFlow.Application(9, 0, 3, [5])] = 5
        reservations[ScheduleFlow.Application(3, 0, 1, [2])] = 3
        reservations[ScheduleFlow.Application(1, 0, 4, [10])] = 0
        reservations[ScheduleFlow.Application(10, 0, 3, [5])] = 10
        reservations[ScheduleFlow.Application(10, 0, 2, [5])] = 15
        gaps = sch.gaps_list.add(reservations)
        gaps = sch.gaps_list.remove(reservations)
        # result is [[0, 3, 4], [2, 3, 9], [2, 5, 6], [4, 5, 10],
        # [8, 10, 10], [13, 15, 10], [17, 20, 10]]
        self.assertTrue(len([i for i in gaps if i[2] == 10]) == 4)
        self.assertTrue(len([i for i in gaps if i[0] == 2]) == 2)
        self.assertTrue(len([i for i in gaps if i[0] == 0]) == 1)

    def test_trim(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        reservations = {}
        reservations[ScheduleFlow.Application(5, 0, 2, [5])] = 0
        reservations[ScheduleFlow.Application(9, 0, 3, [5])] = 5
        reservations[ScheduleFlow.Application(3, 0, 1, [2])] = 3
        reservations[ScheduleFlow.Application(1, 0, 4, [10])] = 0
        reservations[ScheduleFlow.Application(10, 0, 3, [5])] = 10
        reservations[ScheduleFlow.Application(10, 0, 2, [5])] = 15
        gaps = sch.gaps_list.add(reservations)
        gaps = sch.gaps_list.trim(4)
        self.assertTrue(len(gaps) == 1)

    def test_clear(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        reservations = {}
        reservations[ScheduleFlow.Application(5, 0, 2, [5])] = 0
        reservations[ScheduleFlow.Application(9, 0, 3, [5])] = 5
        reservations[ScheduleFlow.Application(3, 0, 1, [2])] = 3
        gaps = sch.gaps_list.add(reservations)
        reservations = {}
        reservations[ScheduleFlow.Application(1, 0, 4, [10])] = 0
        reservations[ScheduleFlow.Application(10, 0, 3, [5])] = 10
        reservations[ScheduleFlow.Application(10, 0, 2, [5])] = 15
        sch.gaps_list.clear()
        gaps = sch.gaps_list.add(reservations)
        self.assertEqual(len(gaps), 1)


# test the basic scheduler class
class TestScheduler(unittest.TestCase):
    def test_invalid_stop(self):
        sch = ScheduleFlow.Scheduler(ScheduleFlow.System(10))
        with self.assertRaises(AssertionError):
            sch.clear_job(ScheduleFlow.Application(7, 5, 10, [11]))

    def test_invalid_jobs(self):
        sch = ScheduleFlow.Scheduler(ScheduleFlow.System(10))
        with self.assertRaises(AssertionError):
            sch.submit_job(ScheduleFlow.Application(20, 5, 10, [11]))


# test the online scheduler class
class TestOnlineScheduler(unittest.TestCase):
    def test_longest_job(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(100))
        max_exec = 0
        np.random.seed(0)
        for i in range(10):
            exect = np.random.randint(10, 100)
            max_exec = exect if max_exec < exect else max_exec
            sch.submit_job(ScheduleFlow.Application(51, 0, exect, [exect]))
        ap_list = sch.trigger_schedule()
        self.assertNotEqual(len(ap_list), 0)
        self.assertEqual(ap_list[0][1].request_walltime, max_exec)

    def test_multiple_wait_queues(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(100),
                                           total_queues=10)
        max_procs = 0
        np.random.seed(0)
        # all jobs will be included in the lowest priority queue
        for i in range(10):
            procs = np.random.randint(10, 100)
            max_procs = procs if max_procs < procs else max_procs
            sch.submit_job(ScheduleFlow.Application(procs, 0, 10, [10]))
        ap_list = sch.trigger_schedule()
        self.assertNotEqual(len(ap_list), 0)
        self.assertEqual(ap_list[0][1].nodes, max_procs)

    def test_largest_job(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(100))
        max_procs = 0
        np.random.seed(0)
        for i in range(10):
            procs = np.random.randint(10, 100)
            max_procs = procs if max_procs < procs else max_procs
            sch.submit_job(ScheduleFlow.Application(procs, 0, 10, [10]))

        ap_list = sch.trigger_schedule()
        self.assertNotEqual(len(ap_list), 0)
        self.assertEqual(ap_list[0][1].nodes, max_procs)

    def test_one_node_jobs(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(100))
        np.random.seed(0)
        num_jobs = np.random.randint(1, 101)
        for i in range(num_jobs):
            exect = np.random.randint(36000, 360000)
            sch.submit_job(ScheduleFlow.Application(
                1, np.random.randint(0, 100), exect, [exect]))
        ap_list = sch.trigger_schedule()
        self.assertEqual(len(ap_list), num_jobs)

    def test_nofit_in_schedule(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(10))
        reservation = {}
        for i in range(5):
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
        ret = sch.fit_job_in_schedule(
            ScheduleFlow.Application(1, 0, 50, [50]), reservation, 0)
        self.assertEqual(ret, -1)

    def test_fit_middle(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(10))
        reservation = {}
        for i in list(range(5)) + list(range(6, 10)):
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
        # add random number of one node jobs ( < 8 total jobs )
        # with execution times <= 50
        num_jobs = np.random.randint(1, 8)
        for i in range(num_jobs):
            reservation[ScheduleFlow.Application(
                1, 0, 50 - i, [50 - i])] = 250
        # try to fit an application of execution time = 40 and 2 nodes whose
        # submission time is 5 over when the gap is available
        ret = sch.fit_job_in_schedule(
            ScheduleFlow.Application(2, 255, 40, [40]), reservation, 255)
        self.assertEqual(ret, 255)

    def test_fit_end(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(10))
        reservation = {}
        for i in range(5):
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
        reservation[ScheduleFlow.Application(4, 0, 10, [10])] = 250
        # fit a job that is larger than the gap at the end
        ret = sch.fit_job_in_schedule(
            ScheduleFlow.Application(6, 255, 50, [50]), reservation, 255)
        self.assertEqual(ret, 255)
        ret = sch.fit_job_in_schedule(
            ScheduleFlow.Application(6, 265, 50, [50]), reservation, 255)
        self.assertEqual(ret, -1)


# test the online scheduler class
class TestBatchScheduler(unittest.TestCase):
    def test_nofit_end(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10), 15)
        reservation = {}
        for i in range(5):
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
        reservation[ScheduleFlow.Application(4, 0, 10, [10])] = 250
        # fit a job that is larger than the gap at the end
        ret = sch.fit_job_in_schedule(
            ScheduleFlow.Application(6, 0, 50, [50]), reservation, 0)
        self.assertEqual(ret, -1)

    def test_fit_end(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10), 15)
        reservation = {}
        for i in range(5):
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
            reservation[ScheduleFlow.Application(5, 0, 50, [50])] = i * 50
        reservation[ScheduleFlow.Application(4, 0, 10, [10])] = 250
        # fit a job that is larger than the gap at the end (submission time is
        # 5 over when the gap is available)
        ret = sch.fit_job_in_schedule(
            ScheduleFlow.Application(6, 255, 5, [5]), reservation, 0)
        self.assertEqual(ret, 255)

    def test_one_node_jobs(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(100), 105)
        np.random.seed(0)
        num_jobs = np.random.randint(1, 101)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            sch.submit_job(ScheduleFlow.Application(
                1, np.random.randint(0, 100), exect, [exect]))
        ap_list = sch.trigger_schedule()
        self.assertEqual(len(ap_list), num_jobs)
        # all jobs start at timestamp 0
        self.assertEqual(max([job[0] for job in ap_list]), 0)

    def test_system_wide_jobs(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10), 105)
        np.random.seed(0)
        num_jobs = np.random.randint(1, 101)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            procs = np.random.randint(6, 10)
            sch.submit_job(ScheduleFlow.Application(
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
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10), 10)
        np.random.seed(0)
        num_jobs = np.random.randint(11, 20)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            procs = np.random.randint(6, 10)
            sch.submit_job(ScheduleFlow.Application(
                procs, np.random.randint(0, 100), exect, [exect]))
        ap_list = sch.trigger_schedule()
        self.assertTrue(len(ap_list), 10)

    def test_batch_small(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10), 10)
        np.random.seed(0)
        num_jobs = np.random.randint(11, 20)
        for i in range(num_jobs):
            exect = np.random.randint(10, 100)
            procs = np.random.randint(6, 9)
            sch.submit_job(ScheduleFlow.Application(
                procs, np.random.randint(0, 100), exect, [exect]))
        # add a small job that will fit inside the schedule
        # of the first 10 jobs
        sch.submit_job(ScheduleFlow.Application(
            1, np.random.randint(0, 100), 10, [10]))
        ap_list = sch.trigger_schedule()
        self.assertTrue(len(ap_list), 11)


# test the runtime class
class TestRuntime(unittest.TestCase):
    def test_restore_job_default(self):
        for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
            sch = SchedulerType(ScheduleFlow.System(10))
            runtime = Runtime([ScheduleFlow.Application(
                10, 0, 100, [80, 100, 120],
                resubmit_factor=1.5)])
            runtime(sch)
            workload = runtime.get_stats()
            for job in workload:
                self.assertEqual(job.submission_time, 0)
                self.assertEqual(job.request_walltime, 80)
                self.assertEqual(job.request_sequence[0], 100)

    def test_empty_workload(self):
        for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
            sch = SchedulerType(ScheduleFlow.System(10))
            runtime = Runtime(set())
            runtime(sch)
            workload = runtime.get_stats()
            self.assertEqual(len(workload), 0)

    def test_one_job_success(self):
        for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
            sch = SchedulerType(ScheduleFlow.System(10))
            np.random.seed(0)
            start_time = np.random.randint(1, 100)
            execution_time = np.random.randint(1, 100)
            runtime = Runtime([ScheduleFlow.Application(
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
        for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
            sch = SchedulerType(ScheduleFlow.System(10))
            np.random.seed(0)
            start_time = np.random.randint(1, 100)
            execution_time = np.random.randint(11, 100)
            runtime = Runtime([ScheduleFlow.Application(
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
        for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
            sch = SchedulerType(ScheduleFlow.System(100))
            job_list = set()
            for i in range(100):
                job_list.add(
                    ScheduleFlow.Application(
                        1,
                        i * 10,
                        1000 - i * 10,
                        [1000 - i * 10]))
            runtime = Runtime(job_list)
            runtime(sch)
            workload = runtime.get_stats()
            exec_order = sorted([workload[job][0][0] for job in workload])
            self.assertListEqual(exec_order, [i * 10 for i in range(100)])

    def test_multiple_queues_order(self):
        for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
            sch = SchedulerType(ScheduleFlow.System(10),
                                total_queues=2)
            job_list = set()
            job_list.add(ScheduleFlow.Application(10, 0, 7000, [7000]))
            job_list.add(ScheduleFlow.Application(10, 1, 300, [300]))
            job_list.add(ScheduleFlow.Application(10, 6000, 1000, [1000]))
            runtime = Runtime(job_list)
            runtime(sch)
            workload = runtime.get_stats()
            exec_order = sorted(
                    [(job.walltime, workload[job][0][0]) for job in workload],
                    key=lambda job:job[1])
            self.assertListEqual([i[0] for i in exec_order], [7000, 300, 1000])
            sch = SchedulerType(ScheduleFlow.System(10),
                                total_queues=1)
            job_list = set()
            job_list.add(ScheduleFlow.Application(10, 0, 7000, [7000]))
            job_list.add(ScheduleFlow.Application(10, 1, 300, [300]))
            job_list.add(ScheduleFlow.Application(10, 6000, 1000, [1000]))
            runtime = Runtime(job_list)
            runtime(sch)
            workload = runtime.get_stats()
            exec_order = sorted(
                    [(job.walltime, workload[job][0][0]) for job in workload],
                    key=lambda job:job[1])
            self.assertListEqual([i[0] for i in exec_order], [7000, 1000, 300])

    def test_batch_wq_backfill(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10),
                                          total_queues=2)
        job_list = set()
        job_list.add(ScheduleFlow.Application(10, 0, 5000, [7000]))
        job_list.add(ScheduleFlow.Application(5, 0, 9000, [10000]))
        job_list.add(ScheduleFlow.Application(10, 0, 100, [100]))
        job_list.add(ScheduleFlow.Application(10, 0, 900, [1000]))
        job_list.add(ScheduleFlow.Application(10, 0, 900, [900]))
        runtime = Runtime(job_list)
        runtime(sch)
        workload = runtime.get_stats()
        self.assertEqual(
            max([workload[i][len(workload[i])-1][1] for i in workload]),
            16000)
        # using 2 priority queues uses the small jobs for backfill
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10),
                                          total_queues=1)
        job_list = set()
        job_list.add(ScheduleFlow.Application(10, 0, 5000, [7000]))
        job_list.add(ScheduleFlow.Application(5, 0, 9000, [10000]))
        job_list.add(ScheduleFlow.Application(10, 0, 100, [100]))
        job_list.add(ScheduleFlow.Application(10, 0, 900, [1000]))
        job_list.add(ScheduleFlow.Application(10, 0, 900, [900]))
        runtime = Runtime(job_list)
        runtime(sch)
        workload = runtime.get_stats()
        self.assertEqual(
            max([workload[i][len(workload[i])-1][1] for i in workload]),
            19000)

    def test_space_out_jobs(self):
        for num_queues in range(1,3):
            for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
                sch = SchedulerType(ScheduleFlow.System(10),
                                    total_queues=num_queues)
                job_list = set()
                job_list.add(ScheduleFlow.Application(10, 0, 7000, [7000]))
                job_list.add(ScheduleFlow.Application(1, 16000, 1000, [1000]))
                runtime = Runtime(job_list)
                runtime(sch)
                workload = runtime.get_stats()
                self.assertEqual(
                    max([workload[i][len(workload[i])-1][1] for i in workload]),
                    17000)


    def test_same_start(self):
        for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
            sch = SchedulerType(ScheduleFlow.System(100))
            np.random.seed(0)
            job_list = set()
            for i in range(100):
                execution_time = np.random.randint(11, 100)
                job_list.add(ScheduleFlow.Application(
                    1, 1 + i * 10, execution_time, [execution_time]))
            # job_listication in the beginning that will make all other be
            # in the wait queue for the next schedule trigger
            job_list.add(ScheduleFlow.Application(100, 0, 1000, [1000]))
            runtime = Runtime(job_list)
            runtime(sch)
            workload = runtime.get_stats()
            self.assertEqual(
                len([job for job in workload if workload[job][0][0] == 0]),
                1)
            self.assertEqual(
                len([job for job in workload if workload[job][0][0] == 1000]),
                100)

    def test_nofail_random(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(100))
        job_list = set()
        np.random.seed(0)
        for i in range(100):
            execution_time = np.random.randint(11, 100)
            job_list.add(ScheduleFlow.Application(
                np.random.randint(51, 100),
                0, execution_time, [execution_time]))
        runtime = Runtime(job_list)
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
        job_list = set()
        np.random.seed(0)
        for i in range(10):
            execution_time = np.random.randint(11, 100)
            job_list.add(ScheduleFlow.Application(
                np.random.randint(6, 11),
                0, execution_time, [execution_time],
                resubmit_factor=1.5))
        fail_job = ScheduleFlow.Application(
            np.random.randint(1, 11), 0, 100, [90], resubmit_factor=1.5)
        job_list.add(fail_job)

        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(10))
        runtime = Runtime(job_list)
        runtime(sch)
        workload = runtime.get_stats()
        # failed job runs after its previous instance
        self.assertEqual(workload[fail_job][0][1], workload[fail_job][1][0])

    def test_fail_job_batch(self):
        job_list = set()
        np.random.seed(0)
        for i in range(10):
            execution_time = np.random.randint(11, 100)
            job_list.add(ScheduleFlow.Application(
                np.random.randint(1, 11), 0, execution_time, [execution_time]))
        fail_job = ScheduleFlow.Application(
            np.random.randint(9, 11), 0, 100, [90], resubmit_factor=1.5)
        job_list.add(fail_job)

        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        runtime = Runtime(job_list)
        runtime(sch)
        workload = runtime.get_stats()
        # failed job ran last
        last_run = max([workload[job][0][1] for job in workload])
        self.assertEqual(workload[fail_job][
            len(workload[fail_job]) - 1][0], last_run)

    def test_backfill_nojob(self):
        for SchedulerType in ScheduleFlow.Scheduler.__subclasses__():
            job_list = set()
            for i in range(10):
                if i == 7:
                    job_list.add(ScheduleFlow.Application(5, 0, 50, [50]))
                    seljob = ScheduleFlow.Application(5, 0, 50, [60])
                    job_list.add(seljob)
                job_list.add(ScheduleFlow.Application(5, 0, 50, [50]))
                job_list.add(ScheduleFlow.Application(5, 0, 50, [50]))

            sch = SchedulerType(ScheduleFlow.System(10))
            runtime = Runtime(job_list)
            runtime(sch)
            workload = runtime.get_stats()
            # check that first job ran is id 15
            first_job = [job for job in workload if workload[job][0][0] == 0]

            self.assertIn(seljob, first_job)
            # online covers the gap, batch does not
            cnt = 0
            if SchedulerType == ScheduleFlow.BatchScheduler:
                cnt = 1
            self.assertEqual(
                len([job for job in workload if workload[job][0][0] == 60]),
                cnt)

    def test_backfill_jobs(self):
        # two backfill jobs running over a gap created by one job
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10), 9)
        job_list = set()
        for i in range(5):
            time = 50
            if i == 3:
                time = 30
            job_list.add(ScheduleFlow.Application(5, 0, 50, [50]))
            job_list.add(ScheduleFlow.Application(5, 0, time, [50]))
        job_list.add(ScheduleFlow.Application(5, 0, 10, [10]))
        job_list.add(ScheduleFlow.Application(5, 0, 10, [10]))
        runtime = Runtime(job_list)
        runtime(sch)
        workload = runtime.get_stats()
        # check that all jobs are executed before timestamp 250
        self.assertTrue(max([workload[job][len(workload[job]) - 1][1]
                             for job in workload]) <= 250)

    def test_backfill_gaps(self):
        # one backfill jobs running over gaps created by two jobs
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10), 10)
        job_list = set()
        for i in range(5):
            if i == 2:
                continue
            job_list.add(ScheduleFlow.Application(5, 0, 50, [50]))
            job_list.add(ScheduleFlow.Application(5, 0, 50, [50]))
        job_list.add(ScheduleFlow.Application(5, 0, 50, [60]))
        job_list.add(ScheduleFlow.Application(5, 0, 50, [70]))
        job_list.add(ScheduleFlow.Application(10, 0, 10, [10]))
        runtime = Runtime(job_list)
        runtime(sch)
        workload = runtime.get_stats()
        # check that all jobs are executed before timestamp 250
        self.assertTrue(max([workload[job][len(workload[job]) - 1][1]
                             for job in workload]) <= 270)

    def test_starvation_online(self):
        sch = ScheduleFlow.OnlineScheduler(ScheduleFlow.System(10))
        job_list = set()
        job_list.add(ScheduleFlow.Application(5, 0, 100, [100]))
        job_list.add(ScheduleFlow.Application(5, 0, 50, [50]))
        for i in range(1, 5):
            job_list.add(ScheduleFlow.Application(5, 1, 100, [100]))
            job_list.add(ScheduleFlow.Application(5, 1, 100, [100]))
        job_list.add(ScheduleFlow.Application(10, 1, 100, [100]))
        job_list.add(ScheduleFlow.Application(5, 10, 40, [40]))
        runtime = Runtime(job_list)
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
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        job_list = set()
        job_list.add(ScheduleFlow.Application(5, 0, 100, [100]))
        job_list.add(ScheduleFlow.Application(5, 0, 50, [50]))
        for i in range(1, 5):
            job_list.add(ScheduleFlow.Application(5, 1, 100, [100]))
            job_list.add(ScheduleFlow.Application(5, 1, 100, [100]))
        job_list.add(ScheduleFlow.Application(10, 1, 100, [100]))
        job_list.add(ScheduleFlow.Application(5, 10, 40, [40]))
        runtime = Runtime(job_list)
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
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        job_list = set()
        selap = ScheduleFlow.Application(3, 5402, 600, [600])
        job_list.add(ScheduleFlow.Application(2, 0, 13000, [13000]))
        job_list.add(ScheduleFlow.Application(3, 0, 6800, [7200]))
        job_list.add(ScheduleFlow.Application(5, 0, 5500, [5500]))
        job_list.add(ScheduleFlow.Application(3, 5400, 7200, [7200]))
        job_list.add(selap)
        job_list.add(ScheduleFlow.Application(5, 5401, 5800, [5800]))
        runtime = Runtime(job_list)
        runtime(sch)
        workload = runtime.get_stats()
        workload_job4 = workload[selap]
        self.assertTrue(workload_job4[0][0] >= 13000)

    def test_cascading_failures(self):
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        runtime = Runtime([ScheduleFlow.Application(
            5, 0, 500, [100], resubmit_factor=1.5)])
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
            sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
            seljob = ScheduleFlow.Application(5, 0, 700, [650],
                                              resubmit_factor=1.5)
            runtime = Runtime([ScheduleFlow.Application(
                5, 0, 1400, [1200+i], resubmit_factor=1.5),
                               ScheduleFlow.Application(
                5, 0, 1400, [1200], resubmit_factor=1.5),
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
        sch = ScheduleFlow.BatchScheduler(ScheduleFlow.System(10))
        runtime = Runtime([ScheduleFlow.Application(6, 0, 500, [1000]),
                           ScheduleFlow.Application(6, 0, 1000, [2000])])
        runtime(sch)
        workload = runtime.get_stats()
        stats = StatsEngine(10)
        stats.set_execution_output(workload)
        self.assertEqual(stats.total_makespan(), 2500)
        self.assertEqual(stats.system_utilization(), 0.36)
        self.assertEqual(stats.average_job_utilization(), 0.5)
        self.assertEqual(stats.average_job_wait_time(), 1000)
        self.assertEqual(stats.average_job_response_time(), 1750)
        self.assertEqual(stats.average_job_stretch(), 3)
        self.assertEqual(stats.total_failures(), 0)

    def test_create_scenario(self):
        sim = ScheduleFlow.Simulator()
        sim.logger.setLevel(logging.CRITICAL)
        ret = sim.create_scenario(
            ScheduleFlow.BatchScheduler(ScheduleFlow.System(10)))
        self.assertEqual(len(sim.job_list), 0)
        self.assertEqual(ret, 0)
        job_list = [ScheduleFlow.Application(6, 0, 500, [1000]),
                    ScheduleFlow.Application(6, 0, 500, [1000])]
        ret = sim.create_scenario(
            ScheduleFlow.BatchScheduler(ScheduleFlow.System(10)),
            job_list=job_list)
        self.assertEqual(len(sim.job_list), 2)
        self.assertEqual(ret, 2)

    def test_additional_jobs(self):
        sim = ScheduleFlow.Simulator()
        sim.logger.setLevel(logging.CRITICAL)
        sim.create_scenario(
            ScheduleFlow.BatchScheduler(ScheduleFlow.System(10)))
        job_list = [ScheduleFlow.Application(6, 0, 500, [1000]),
                    ScheduleFlow.Application(6, 0, 500, [1000])]
        ret = sim.add_applications(job_list)
        ret = sim.add_applications(job_list)
        self.assertEqual(ret, 2)
        job_list = [ScheduleFlow.Application(6, 0, 500, [1000]),
                    ScheduleFlow.Application(6, 0, 500, [1000])]
        ret = sim.add_applications(job_list)
        self.assertEqual(ret, 4)

    def test_run_scenario(self):
        sim = ScheduleFlow.Simulator()
        with self.assertRaises(AssertionError):
            ret = sim.run()
        job_list = [ScheduleFlow.Application(6, 0, 500, [1000]),
                    ScheduleFlow.Application(6, 0, 500, [1000])]
        sim.create_scenario(
            ScheduleFlow.BatchScheduler(ScheduleFlow.System(10)),
            job_list=job_list)
        sim.run()
        self.assertEqual(sim.test_correctness(), 0)

    def test_simulation_correctness(self):
        sim = ScheduleFlow.Simulator(check_correctness=True)
        job_list = [ScheduleFlow.Application(10, 0, 500, [200],
                                             resubmit_factor=1.5),
                    ScheduleFlow.Application(10, 0, 500, [600]),
                    ScheduleFlow.Application(10, 0, 500, [200, 600]),
                    ScheduleFlow.Application(10, 0, 500, [300, 400]),
                    ScheduleFlow.Application(10, 0, 500, [400, 450],
                                             resubmit_factor=1.5)]
        sim.create_scenario(
            ScheduleFlow.BatchScheduler(ScheduleFlow.System(10)),
            job_list=job_list)
        sim.run()
        self.assertEqual(sim.test_correctness(), 0)


if __name__ == '__main__':
    unittest.main()
