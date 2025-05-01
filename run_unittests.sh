#!/bin/bash

# bash run_unittest.sh -v (for verbose)

echo "Test Queues"
python -m unittest test_unittest.TestEventQueue $1
python -m unittest test_unittest.TestWaitingQueue $1

echo "---------------------------------"
echo "Test System"
python -m unittest test_unittest.TestSystem $1

echo "---------------------------------"
echo "Test Applications"
python -m unittest test_unittest.TestApplication $1
python -m unittest test_unittest.TestCheckpointing $1 > /dev/null

echo "---------------------------------"
echo "Test Basic Scheduler"
python -m unittest test_unittest.TestScheduleGaps $1
python -m unittest test_unittest.TestScheduler $1

echo "---------------------------------"
echo "Test Batch Scheduler"
python -m unittest test_unittest.TestScheduler $1

echo "---------------------------------"
echo "Test Runtime"
python -m unittest test_unittest.TestRuntime $1 > /dev/null

echo "---------------------------------"
echo "Test Simulator"
python -m unittest test_unittest.TestSimulator $1 > /dev/null

