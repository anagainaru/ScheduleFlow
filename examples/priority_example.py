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
def create_job_list(analysis_list, sim):
    for job in analysis_list:
        processing_units = analysis_list[job][1]
        submission_time = 0
        execution_time = analysis_list[job][2]
        request_time = analysis_list[job][2] + 10
        priority = analysis_list[job][0]
        job = ScheduleFlow.Application(
                processing_units,
                submission_time,
                execution_time,
                [request_time],
                priority=priority, name=job)
        sim.add_application(job)
    return sim

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
    analysis_list = priority_analysis_list()

    number_loops = 1
    loop_duration = 250
    num_processing_units = 4
    for loop in range(number_loops):
        # create the simulator
        simulator = ScheduleFlow.Simulator(check_correctness=True,
                                           generate_gif=True,
                                           output_file_handler=sys.stdout,
                                           loops = 1)
        simulator = create_job_list(analysis_list, simulator)
        print("List of jobs", simulator.job_list)

        sch = ScheduleFlow.Scheduler(
                ScheduleFlow.System(num_processing_units),
                priority_policy=ScheduleFlow.SchedulingPriorityPolicy.FCFS,
                backfill_policy=ScheduleFlow.SchedulingBackfillPolicy.Easy)
                logger=logging.getLogger(__name__))
        logging.basicConfig(level=logging.INFO)
        simulator.create_scenario(sch)
        execution_log = simulator.run(metrics="execution_log")
        success = []
        for job in execution_log:
            execution = execution_log[job]
            # check that the last execution window did not exceed the loop time
            if execution[-1][1] < loop_duration:
                success.append(job.name)
        print("Jobs finishing in loop %d: %s" % (loop, success))
