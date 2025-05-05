
# A simple simulator for HPC schedulers

<img src="./docs/logo.png" align="left" alt="Logo" width="250"/>

The ScheduleFlow software consists of a series of scripts and classes that offer an API allowing users to create simulation scenarios for online and reservation-based batch schedulers.

### Citation

If you use the package available here in your work, please refer to the latest release.

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15318879.svg)](https://zenodo.org/records/15318879)

Or cite using APA style:

*[1] A. Gainaru. ScheduleFlow: A Simple simulator for HPC schedulers. (2020) [Online]. Available: https:// github.com/anagainaru/ScheduleFlow*

## Documentation

For details on the API or the internals of the Simulator, visit the [wiki](https://github.com/anagainaru/SchedulerSimulator/wiki)

### Getting Started

Clone the repository or download the latest release:

`git clone https://github.com/anagainaru/ScheduleFlow.git ScheduleFlow`

In the python code import the ScheduleFlow package:
```python
sys.path.append("path/to/ScheduleFlow")
import ScheduleFlow
```

For generating GIF animations, the path to the ScheduleFlow root directory needs to be saved in the ScheduleFlow_PATH environmental variable, either in the python code:
```python
import os
os.environ["ScheduleFlow_PATH"] = "path/to/ScheduleFlow"
```
or as linux command:

`export ScheduleFlow_PATH="path/to/ScheduleFlow" `

**Requirements** 

The ScheduleFlow package was build for python 3 and requires numpy. For running on python 2, enum34 needs to be installed.

<pre>
Generating GIFs for simulations requires <b>pdflatex</b> and <b>convert</b> from ImageMagick.
</pre>


### Example

`python run_simple_example.py`

<sup>* Tested with python 3.7</sup>

**Crash course**

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
scheduler = Scheduler.Scheduler(system)
simulator.create_scenario(scenario_name,
                          scheduler,
                          job_list=job_list)
simulator.run()
```

The scenario uses a scheduler and a list of jobs that need to be
simulated. In addition, jobs can be added to a scenario by using
the `simulator.add_applications(job_list)` method. Alternatively,
the run_scenario method can be called directly on the simulator:

```python
simulator.run_scenario(scenario_name, scheduler, job_list)
```

The scenario name is used to create the animation GIF filenames
and for debugging purposes. The resubmit factor indicates that 
failed jobs need to be resubmitted with an execution time increase
given by the facor. By default, failed jobs are not resubmitted.

The GIF generation is controled by the VizEngine class. It uses the
`tex_header` and `tex_footer` files in the ./draw directory to 
generate tex files for every step of the animation. Pdflatex is used
fo create PDF files which are used by ImageMagick to generate the GIF.

**Output**

If successfuly ran, the `run_simple_example.py` script will output<sup>*</sup>:

<pre>
Scenario name : job failures : job response time : job stretch : job utilization : job wait time : system makespan : system utilization :
test_batch : 1.00 : 361.64 : 5.78 : 0.73 : 259.08 : 606.00 : 0.67 :
test_online : 1.00 : 374.91 : 6.86 : 0.73 : 271.25 : 553.00 : 0.74 :
GIFs generated in ./draw/test_{batch, online}.gif
</pre>

Example GIFs generated:

Reservation-based scheduler simulation<sup>*</sup>

![Batch scheduler](./docs/batch.png)

Online scheduler simulation<sup>*</sup>

![Online scheduler](./docs/online.png)

<sup>* Depeding on the requirements of the jobs, the scheduler might give
other execution orders for the submitted jobs and thus slightly different performance values</sup>

