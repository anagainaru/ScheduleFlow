from Runtime import System
from Runtime import ApplicationJob
import Scheduler
import Simulator
import sys
import numpy as np


def run_scenario(num_procssing_units, job_list):
    simulator = Simulator.Simulator(check_correctness=True,
                                    generate_gif=True,
                                    output_file_handler=sys.stdout)
    sch = Scheduler.BatchScheduler(System(num_processing_units))
    simulator.create_scenario("test_batch", sch, 1.5, job_list=job_list)
    simulator.run()

    sch = Scheduler.OnlineScheduler(System(num_processing_units))
    simulator.create_scenario("test_online", sch, 1.5, job_list=job_list)
    simulator.run()


if __name__ == '__main__':
    num_processing_units = 10
    job_list = set()
    for i in range(10):
        execution_time = np.random.randint(11, 100)
        request_time = execution_time + int(i / 2) * 10
        processing_units = np.random.randint(
            1, num_processing_units + 1)
        submission_time = 0
        job_list.add(ApplicationJob(processing_units,
                                    submission_time,
                                    execution_time,
                                    request_time,
                                    i))
    job_list.add(ApplicationJob(np.random.randint(9, 11), 0, 100, 90, 10,
                                resubmit_factor=1.5))

    print("Scenario : makespan : utilization : average_job_utilization : "
          "average_job_response_time : average_job_stretch : "
          "average_job_wait_time : failures")

    run_scenario(num_processing_units, job_list)
