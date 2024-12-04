# ScheduleFlow

Mini documentation. There are 4/5 main steps of an application using ScheduleFlow:

1. Create Application objects and keep them in a container (which will represent the workload that needs to be simulated)
```python
ScheduleFlow.Application(
       processing_units,
       submission_time,
       execution_time,
       [request_time], name=job)
```
2. Create a Scheduler object that defines the underlying system (defined by the number of available processing units), the number of priority queues used and the priority policy and the backfilling policy.
   - The priority policy defines the order in which the scheduler chooses the jobs for scheduling (first come first served FCFS, largest job first LJF, shortest job first SJF)
   - The backfilling policy defines the order in which the scheduler chooses jobs for backfilling (easy and conservative)
```python
ScheduleFlow.BatchScheduler(
       ScheduleFlow.System(num_processing_units),
       total_queues=1,
       priority_policy=ScheduleFlow.SchedulingPolicy.FCFS,
       backfill_policy=ScheduleFlow.SchedulingPolicy.Easy)
```
3. Create a Simulator object that defines the parameters of the whole simulation
```python
ScheduleFlow.Simulator(
       check_correctness=True,
       generate_gif=True,
       output_file_handler=sys.stdout,
       loops = 1)
```
4. Runs a scenario in the simulation for a given scheduler and workload
   - Either by creating the scenario first and then running it
   - Directly running the scenario (that internal creates the scenario and runs it)
```python
simulator.create_scenario(sch, job_list=job_list)
execution_log = simulator.run(metrics="execution_log")
```

![API](https://github.com/anagainaru/ScheduleFlow/blob/master/docs/wiki/api.png)

Running the scenario returns either the execution log {(job, execution_times)} or average metrics about the run:
```
job failures : job response time : job stretch : job utilization : job wait time : system makespan : system utilization
0.00 : 268.43 : 4.94 : 1.00 : 189.86 : 470.00 : 0.72
```

# Internals

Runtime class responsible for coordinating the submission and execution process for all the jobs in a workload

Runtime object is created at the beginning of the execution.

Constructor method creates the job submission events for all the jobs in the workload. 


