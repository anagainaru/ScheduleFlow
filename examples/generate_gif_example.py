import sys
sys.path.append("..")
import ScheduleFlow
import numpy as np
import os

def run_scenario(num_processing_units, num_jobs):
    job_list = set()
    # create the list of applications
    for i in range(num_jobs):
        execution_time = np.random.randint(1800, 10000)
        request_time = execution_time + int(i / 2) * 1500
        processing_units = np.random.randint(
            1, num_processing_units + 1)
        submission_time = 0
        job_list.add(ScheduleFlow.Application(
            processing_units,
            submission_time,
            execution_time,
            [request_time], name="J"+str(i)))
    # add a job that request less time than required for its first run
    job_list.add(ScheduleFlow.Application(np.random.randint(9, 11), 0,
                                          5000, [4000, 5500],
                                          name="J"+str(i)))
    generate_gif = False
    generate_jpg = False
    if num_jobs < 20:
        generate_gif = True
    if num_jobs > 90:
        generate_jpg = True
    simulator = ScheduleFlow.Simulator(check_correctness=True,
                                       generate_gif=generate_gif,
                                       generate_jpg=generate_jpg,
                                       output_file_handler=sys.stdout)
    sch = ScheduleFlow.Scheduler(
        ScheduleFlow.System(num_processing_units))
    simulator.create_scenario(sch, job_list=job_list,
                              scenario_name="test_jpg")
    execution = simulator.run(metrics="execution_log")
    # print(execution)

if __name__ == '__main__':
    os.environ["ScheduleFlow_PATH"] = ".."
    num_processing_units = 10
    run_scenario(num_processing_units, 10)
    run_scenario(num_processing_units, 100)
