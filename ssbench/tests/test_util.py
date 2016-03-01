# Copyright (c) 2016 SwiftStack, Inc.

import ssbench.util
import math
from unittest import TestCase


class TestUtil(TestCase):
    def test_mean(self):
        sequences = [([1, 2, 3, 4, 5], 3),
                     ([-1, -2, -3, -4, -5], -3),
                     ([-2, -1, 0, 1, 2], 0),
                     ([2, 2, 2, 2, 2], 2),
                     ([5], 5),
                     ([], None)]
        for seq, mean in sequences:
            self.assertEqual(ssbench.util.mean(seq), mean)

    def test_median(self):
        sequences = [([1, 2, 3], 2),
                     ([1, 2, 3, 4], 2.5),
                     ([2, 0, 4, 1, 3], 2),
                     ([0, -1, -2, -3], -1.5),
                     ([5], 5),
                     ([], None)]
        for seq, median in sequences:
            self.assertEqual(ssbench.util.median(seq), median)

    def test_uncorrected_stdev(self):
        sequences = [([1, 1, 1], 0),
                     ([1, 2, 3], math.sqrt(2.0 / 3)),
                     ([-1, -1, -1], 0),
                     ([1], 0),
                     ([-1, -2, -3], math.sqrt(2.0 / 3)),
                     ([], None)]

        for seq, std in sequences:
            self.assertEqual(ssbench.util.uncorrected_stdev(seq), std)
