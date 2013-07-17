import math
import random
import sys
import heapq

from time import time

from atomic import Atomic
from threading import RLock
from metrology.stats.snapshot import Snapshot


class UniformSample(object):
    def __init__(self, reservoir_size):
        self.counter = Atomic(0)
        self.values = [0] * reservoir_size

    def clear(self):
        self.values = [0] * len(self.values)
        self.counter.value = 0

    def size(self):
        count = self.counter.value
        if count > len(self.values):
            return len(self.values)
        return count

    def __len__(self):
        return self.size

    def snapshot(self):
        return Snapshot(self.values[0:self.size()])

    def update(self, value):
        new_count = self.counter.update(lambda v: v + 1)

        if new_count <= len(self.values):
            self.values[new_count - 1] = value
        else:
            index = random.uniform(0, new_count)
            if index < len(self.values):
                self.values[int(index)] = value


class ExponentiallyDecayingSample(object):
    RESCALE_THRESHOLD = 60 * 60

    def __init__(self, reservoir_size, alpha):
        self.values = []
        self.next_scale_time = Atomic(0)
        self.alpha = alpha
        self.reservoir_size = reservoir_size
        self.lock = RLock()
        self.clear()

    def clear(self):
        with self.lock:
            self.values = []
            self.start_time = time()
            self.next_scale_time.value = self.start_time + self.RESCALE_THRESHOLD

    def size(self):
        with self.lock:
            return len(self.values)

    def __len__(self):
        return self.size()

    def snapshot(self):
        with self.lock:
            return Snapshot(val for _, val in self.values)

    def weight(self, timestamp):
        return math.exp(self.alpha * timestamp)

    def rescale(self, now, next_time):
        if self.next_scale_time.compare_and_swap(next_time, now + self.RESCALE_THRESHOLD):
            with self.lock:
                old_start_time = self.start_time
                self.start_time = time()
                for key in list(self.values.keys()):
                    value = self.values.remove(key)
                self.values[key * math.exp(-self.alpha * (self.start_time - old_start_time))] = value

    def update(self, value, timestamp=None):
        if not timestamp:
            timestamp = time()
        with self.lock:
            try:
                priority = self.weight(timestamp - self.start_time)
            except OverflowError:
                priority = sys.float_info.max

            try:
                priority /= random.random()
            except ZeroDivisionError:
                pass

            if len(self.values) < self.reservoir_size:
                heapq.heappush(self.values, (priority, value))
            else:
                heapq.heappushpop(self.values, (priority, value))

