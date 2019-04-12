
<img src="./docs/logo.png" align="right" alt="Logo" width="200"/>

# Simulator for HPC schedulers

[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/anagainaru_marketplace/anagainaru%2FSchedulerSimulator%2FSchedulerSimulator?type=cf-1)]( https://g.codefresh.io/public/accounts/anagainaru_marketplace/pipelines/anagainaru/SchedulerSimulator/SchedulerSimulator)
[![codecov](https://codecov.io/gh/anagainaru/SchedulerSimulator/branch/master/graph/badge.svg)](https://codecov.io/gh/anagainaru/SchedulerSimulator)

### Usage

`python run_simple_example.py`

<sup>* Tested with python 3</sup>

**Requirements** 

Running the script requires **numpy**, **scipy**.

For the GIF generation, **pdflatex** and **convert** from ImageMagick are required.

**Details**

The script simulates the execution of 11 jobs submitted to 
a reservation-based scheduler and an online scheduler.

The simulation assumes a system of 10 processing units.

The workload used consists of:

- 10 jobs with randon execution times
  (requesting larger execution times than needed) and random
  processing unit requirements. 
- one large job running on almost the entire machine and for
  large units of time (equesting less time than it needs to 
  complete successfully)

The simulation can check for correctness, generate an animation
of the scheduling process and output the results to a file or 
to stdout. Declaring a new Simulation object:

```python
simulator = Simulator.Simulator(check_correctness=True,
                                generate_gif=True,
                                output_file_handler=sys.stdout)

```

To start a simulation a scenario needs to be created for a 
given simulation:

```python
scheduler = Scheduler.BatchScheduler(system)
simulator.create_scenario(scenario_name,
                          scheduler,
                          resubmit_factor=1.5,
                          job_list=job_list)
simulator.run()
```

The scenario uses a scheduler and a list of jobs that need to be
simulated. In addition, jobs can be added to a scenario by using
the `simulator.add_jobs(job_list)` method.

The scenario name is used to create the animation GIF filenames
and for debugging purposes. The resubmit factor indicates that 
failed jobs need to be resubmitted with an execution time increase
given by the facor. By default, failed jobs are not resubmitted.

The GIF generation is controled by the VizEngine class. It uses the
`tex_header` and `tex_footer` files in the ./draw directory to 
generate tex files for every step of the animation. Pdflatex is used
fo create PDF files which are used by ImageMagick to generate the GIF.

**Output**

If successfuly ran, the `run_simple_example.py` script will output:

> Scenario : makespan : utilization : average_job_utilization : average_job_response_time : average_job_stretch : average_job_wait_time : failures
>
> test_batch : 526.00 : 0.54 : 0.70 : 291.55 : 8.76 : 209.50 : 1
>
> test_online : 421.00 : 0.67 : 0.70 : 285.91 : 8.96 : 204.33 : 1
>
> GIFs generated in ./draw/test_{batch, online}.gif

Example GIFs generated:

Reservation-based scheduler simulation

![Batch scheduler](./docs/batch.png)

Reservation-based scheduler simulation

![Batch scheduler](./docs/online.png)

<sup>* Depeding on the requirements of the jobs, the scheduler will give
another execution order for all submitted jobs.</sup>

