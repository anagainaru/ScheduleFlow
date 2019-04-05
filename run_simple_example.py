from Runtime import System
from Runtime import ApplicationJob
import Scheduler
import Simulator
import sys
import numpy as np


def run_scenario(procs, apl):
    simulator = Simulator.Simulator(check_correctness=True,
                                    generate_gif=True,
                                    output_file_handler=sys.stdout)
    sch = Scheduler.BatchScheduler(System(procs))
    simulator.create_scenario("test_batch", sch, 1.5, job_list=apl)
    simulator.run()

    sch = Scheduler.OnlineScheduler(System(procs))
    simulator.create_scenario("test_online", sch, 1.5, job_list=apl)
    simulator.run()

if __name__ == '__main__':
    procs = 10
    apl = set()
    for i in range(10):
        execution_time = np.random.randint(11, 100)
        apl.add(ApplicationJob(np.random.randint(1, 11),
                               0,
                               execution_time,
                               execution_time + int(i / 2) * 10,
                               i))
    apl.add(ApplicationJob(np.random.randint(9, 11), 0, 100, 90, 10))

    print("Scenario : makespan : utilization : average_job_utilization : " \
          "average_job_response_time : average_job_stretch : " \
          "average_job_wait_time : failures")

    execution_log = run_scenario(procs, apl)

