from unittest import TestCase
from mock import patch
from decorator import decorator
from metrology.stats.sample import UniformSample, ExponentiallyDecayingSample
import random


class UniformSampleTest(TestCase):
    def test_sample(self):
        sample = UniformSample(100)
        for i in range(1000):
            sample.update(i)
        snapshot = sample.snapshot()
        self.assertEqual(sample.size(), 100)
        self.assertEqual(snapshot.size(), 100)

        for value in snapshot.values:
            self.assertTrue(value < 1000.0)
            self.assertTrue(value >= 0.0)


class ExponentiallyDecayingSampleTest(TestCase):
    def test_sample_1000(self):
        sample = ExponentiallyDecayingSample(100, 0.99)
        for i in range(1000):
            sample.update(i)
        self.assertEqual(sample.size(), 100)
        snapshot = sample.snapshot()
        self.assertEqual(snapshot.size(), 100)

        for value in snapshot.values:
            self.assertTrue(value < 1000.0)
            self.assertTrue(value >= 0.0)

    def test_sample_10(self):
        sample = ExponentiallyDecayingSample(100, 0.99)
        for i in range(10):
            sample.update(i)
        self.assertEqual(sample.size(), 10)

        snapshot = sample.snapshot()
        self.assertEqual(snapshot.size(), 10)

        for value in snapshot.values:
            self.assertTrue(value < 10.0)
            self.assertTrue(value >= 0.0)

    def test_sample_100(self):
        sample = ExponentiallyDecayingSample(1000, 0.01)
        for i in range(100):
            sample.update(i)
        self.assertEqual(sample.size(), 100)

        snapshot = sample.snapshot()
        self.assertEqual(snapshot.size(), 100)

        for value in snapshot.values:
            self.assertTrue(value < 100.0)
            self.assertTrue(value >= 0.0)

    def timestamp_to_priority_is_noop(f):
        """
        Decorator that patches ExponentiallyDecayingSample class such that the
        timestamp->priority function is a no-op.
        """
        weight_fn =  "metrology.stats.sample.ExponentiallyDecayingSample.weight"
        return patch(weight_fn, lambda self, x : x)(
               patch("random.random", lambda:1.0 )
                   (f))


    @timestamp_to_priority_is_noop
    def test_sample_eviction(self):
        kSampleSize= 10
        kDefaultValue = 1.0
        sample = ExponentiallyDecayingSample(kSampleSize, 0.01)
        sample.start_time = 0
        timeStamps = range(1, kSampleSize*2)
        for count, timeStamp in enumerate(timeStamps):
            sample.update(kDefaultValue, timeStamp)
            self.assertLessEqual(sample.values.count, kSampleSize)
            self.assertLessEqual(sample.values.count, count+1)
            expected_min_key = timeStamps[max(0,count+1-kSampleSize)]
            self.assertEqual(sample.values.min_key(), expected_min_key)


    @timestamp_to_priority_is_noop
    def test_sample_ordering(self):
        kSampleSize= 3
        sample = ExponentiallyDecayingSample(kSampleSize, 0.01)
        sample.start_time = 0

        timestamps =  range(1, kSampleSize+1)
        values = ["VAL_"+str(i) for i in timestamps]
        expected = zip(timestamps, values)
        for timestamp, value in expected:
            sample.update(value, timestamp)
        self.assertEqual(list(sample.values.items()), expected)

        # timestamp less than any existing => no-op
        sample.update(None, 0.5 )
        self.assertEqual(list(sample.values.items()), expected)

        # out of order insertions
        expected = [2.0, 3.0, 4.0]
        sample.update(None, 4.0)
        sample.update(None, 3.0)
        self.assertEqual(list(sample.values.keys()), expected)

        # replacement of existing
        marker = "MARKER"
        replacement_timestamp = sample.values.min_key()
        sample.update(marker, replacement_timestamp)
        self.assertEqual(list(sample.values.keys()), expected)
        self.assertEqual(sample.values[replacement_timestamp], marker)

        print list(sample.values.items())


