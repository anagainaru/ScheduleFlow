#!/bin/bash

# bash run_unittest.sh -v (for verbose)

echo "Test Event Queue"
python -m unittest test_unittest.TestEventQueue $1

echo "---------------------------------"
echo "Test System"
python -m unittest test_unittest.TestSystem $1

echo "---------------------------------"
echo "Test Applications"
python -m unittest test_unittest.TestApplication $1

echo "---------------------------------"
echo "Test Basic Scheduler"
python -m unittest test_unittest.TestScheduler $1

echo "---------------------------------"
echo "Test Online Scheduler"
python -m unittest test_unittest.TestOnlineScheduler $1

echo "---------------------------------"
echo "Test Batch Scheduler"
python -m unittest test_unittest.TestBatchScheduler $1

echo "---------------------------------"
echo "Test Runtime"
python -m unittest test_unittest.TestRuntime $1

echo "---------------------------------"
echo "Test Simulator"
python -m unittest test_unittest.TestSimulator $1

