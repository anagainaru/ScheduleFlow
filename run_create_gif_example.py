from Runtime import System
from Runtime import ApplicationJob
from Scheduler import BatchScheduler
import Simulator


def run_scenario(procs):
    sch = BatchScheduler(System(procs))
    apl = set()
    apl.add(ApplicationJob(2, 0, 130, 130, 5))
    apl.add(ApplicationJob(3, 0, 68, 72, 1))
    apl.add(ApplicationJob(5, 0, 55, 55, 2))
    apl.add(ApplicationJob(3, 50, 72, 72, 3))
    apl.add(ApplicationJob(3, 52, 6, 6, 4))
    apl.add(ApplicationJob(5, 51, 58, 58, 6))
    simulator = Simulator.Simulator(generate_gif=True)
    simulator.create_scenario("test", sch, job_list=apl)
    simulator.run()

if __name__ == '__main__':
    procs = 10
    execution_log = run_scenario(procs)

