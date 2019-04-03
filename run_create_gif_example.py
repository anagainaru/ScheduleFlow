from Runtime import System
from Runtime import ApplicationJob
from Runtime import Runtime
from Scheduler import BatchScheduler
import VizEngine
import subprocess
import generate_schedule_tex

def run_scenario(procs):
        sch = BatchScheduler(System(procs))
        apl = set()
        apl.add(ApplicationJob(2, 0, 130, 130, 5))
        apl.add(ApplicationJob(3, 0, 68, 72, 1))
        apl.add(ApplicationJob(5, 0, 55, 55, 2))
        apl.add(ApplicationJob(3, 50, 72, 72, 3))
        apl.add(ApplicationJob(3, 52, 6, 6, 4))
        apl.add(ApplicationJob(5, 51, 58, 58, 6))
        runtime = Runtime(apl, 1.5)
        runtime(sch)
        return runtime.get_stats()

if __name__ == '__main__':
    procs = 10
    execution_log = run_scenario(procs)
    viz_handler = VizEngine.VizualizationEngine(procs,
                                                execution_log=execution_log)
    viz_handler.set_horizontal_ax_limit(200)
    viz_handler.generate_scenario_gif("test")

