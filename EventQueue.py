import heapq
import collections
from enum import IntEnum


class EventType(IntEnum):
    ''' Enumeration class to hold event types (distinct values are required)
    The values give the order in which the simulator parses the events
    (e.g. JobEnd has the higher priority and will be evaluated first) '''

    JobSubmission = 2
    JobStart = 1
    JobEnd = 0
    TriggerSchedule = 3


class EventQueue(object):
    ''' Class for storing the events used by the simulator '''

    def __init__(self):
        self.heap = []

    def __str__(self):
        return ' '.join([str(i) for i in self.heap])

    def size(self):
        return len(self.heap)

    def empty(self):
        return self.size() == 0

    def push(self, item):
        ''' The items that can be pushed in the EventQueue must be tuples
        of the form (timestamp, event) '''

        assert (isinstance(item, collections.abc.Sequence)
                ), 'EventQueue works only on tuples (time, values)'
        heapq.heappush(self.heap, item)

    def pop(self):
        return heapq.heappop(self.heap)

    def top(self):
        return self.heap[0]

    def pop_list(self):
        ''' Method for extracting all items in the queue that share
        the lowest timestamp '''

        if self.empty():
            raise IndexError()
        obj_list = [self.pop()]
        while not self.empty() and self.top()[0] == obj_list[0][0]:
            obj_list.append(self.pop())
        return obj_list
