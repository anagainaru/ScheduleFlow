import sys
sys.path.append("..")
import ScheduleFlow
import numpy as np
import os
import logging

def priority_analysis_list():
    job_list = {}
    # job_list[name] = [priority, procs, requested_time]
    job_list["A0"] = [1, 4, 100]
    job_list["A1"] = [1, 1, 100]
    job_list["A2"] = [1, 4, 80]
    job_list["A3"] = [0, 1, 50]
    job_list["A4"] = [0, 3, 50]
    job_list["A5"] = [0, 2, 120]
    job_list["A6"] = [0, 4, 30]
    return job_list

# create scheduler jobs from a priority analysis list
def add_jobs(analysis_list, current_time, sim):
    for job in analysis_list:
        processing_units = analysis_list[job][1]
        submission_time = current_time
        execution_time = analysis_list[job][2]
        request_time = analysis_list[job][2] + 10
        priority = analysis_list[job][0]
        job = ScheduleFlow.Application(
                processing_units,
                submission_time,
                execution_time,
                [request_time],
                priority=priority, name=job)
        print("ADD job", job)
        sim.add_application(job)
    return sim

def update_priority(analysis_list, execution_log):
    return analysis_list

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: %s frequency/priority" % (sys.argv[0]))
        exit()

    type_run = sys.argv[1]
    if type_run != "frequency" and type_run != "priority":
        print("ERROR Unknown argument")
        print("Usage: %s frequency/priority" % (sys.argv[0]))
        exit()

    os.environ["ScheduleFlow_PATH"] = ".."
    number_loops = 2
    loop_duration = 250
    num_processing_units = 4

    # create the analysis jobs that need to be scheduled
    # during each simulation loop
    analysis_list = priority_analysis_list()

    # create the simulator
    simulator = ScheduleFlow.Simulator(check_correctness=True,
                                       generate_gif=False,
                                       output_file_handler=sys.stdout,
                                       loops = 1)
    # create the scheduler
    sch = ScheduleFlow.Scheduler(
            ScheduleFlow.System(num_processing_units),
            priority_policy=ScheduleFlow.PriorityPolicy.FCFS,
            backfill_policy=ScheduleFlow.BackfillPolicy.Easy,
            logger=logging.getLogger(__name__))
    logging.basicConfig(level=logging.DEBUG)

    simulator.create_scenario(sch)
    execution_log = {}
    for loop in range(number_loops):
        simulator = add_jobs(analysis_list, loop * loop_duration, simulator)
        print("Loop %d: List of jobs %s" %(loop, simulator.job_list))

        # run the scenario for the duration of the loop
        loop_exec_log = simulator.run(
                simulation_duration=loop_duration,
                discard_policy=ScheduleFlow.DiscardPolicy.NONE,
                metrics="execution_log")
        execution_log.update(loop_exec_log)
        print("Execution log at loop", loop, loop_exec_log)
        analysis_list = update_priority(analysis_list, loop_exec_log)

    simulator.generate_gif(execution_log, "test")
