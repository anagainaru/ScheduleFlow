#  Copyright (c) 2019-2020 by the Cabana authors
#   All rights reserved.

#   This file is part of the ScheduleFlow package. ScheduleFlow is
#   distributed under a BSD 3-clause license. For details see the
#   LICENSE file in the top-level directory.

#   SPDX-License-Identifier: BSD-3-Clause

import ScheduleFlow
import sys
import numpy as np


def run_scenario(num_processing_units, job_list):
    simulator = ScheduleFlow.Simulator(check_correctness=True,
                                       generate_gif=True,
                                       output_file_handler=sys.stdout)
    sch = ScheduleFlow.BatchScheduler(
        ScheduleFlow.System(num_processing_units))
    simulator.create_scenario("test_batch", sch, job_list=job_list)
    simulator.run()

    sch = ScheduleFlow.OnlineScheduler(
        ScheduleFlow.System(num_processing_units))
    simulator.create_scenario("test_online", sch, job_list=job_list)
    simulator.run()


if __name__ == '__main__':
    num_processing_units = 10
    job_list = set()
    # create the list of applications
    for i in range(10):
        execution_time = np.random.randint(11, 100)
        request_time = execution_time + int(i / 2) * 10
        processing_units = np.random.randint(
            1, num_processing_units + 1)
        submission_time = 0
        job_list.add(ScheduleFlow.Application(
            processing_units,
            submission_time,
            execution_time,
            [request_time]))
    # add a job that request less time than required for its first run
    job_list.add(ScheduleFlow.Application(np.random.randint(9, 11), 0,
                                          100, [90, 135]))

    print("Scenario : makespan : utilization : average_job_utilization : "
          "average_job_response_time : average_job_stretch : "
          "average_job_wait_time : failures")

    run_scenario(num_processing_units, job_list)
