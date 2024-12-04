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

**Simulation object**
  - Creating the scenario: links a scheduler and a workload to a simulation, as well as creates and initializes the Stats and the Viz engines.
  - Running a scenario: goes over as many loops as given in the simulation. In each loop it creates a Runtime object with the workload and executes it over the scheduler.

**Runtime object**

The Runtime class uses an event based approach and is responsible for coordinating the submission and execution process for all the jobs in a workload
  - Keeps track of scheluded and finished jobs
  - Uses an EventQueue object to track events in the simulation {(time, event)}
  - Creates in the constructor an event of type `JobSubmission` for all applications in a workload with the submission time

During the execution over a scheduler, it loops over the EventQueue (4 types of events: JobSubmission, JobStart, JobEnd, TriggerSchedule):
  - It extracts all items in the queue that share the lowest timestamp
  - Moves the current timestamp to the one given by the new events and inspects all the events in the list
    - For `JobSubmission` events: check if the job can fit in the current schedule (decided by the Scheduler) and if yes, create a `JobStart` event for the job
    - For `JobStart` events: allocate the job on the system and create a `JobEnd` event with the timestamp given by the walltime/requested time
    - For `TriggerSchedule` events: ask the scheduler to trigger a new scheduling and add a `JobStart` events for all the jobs chosen by the scheduler for execution
    - For `JobEnd` events: if the walltime was over the requested time (and resubmit is on) add a new `JobSubmission` event with the current timestamp. If the walltime is lower than the request time, look for backfilling jobs and create `JobStart` events for all returned by the scheduler
   
**Scheduler object**

