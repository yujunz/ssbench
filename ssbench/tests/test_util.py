# Copyright (c) 2016 SwiftStack, Inc.

import math
import mock
import ssbench.util
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

    def test_is_ipv6(self):
        addrs = [('127.0.0.1', False),
                 ('::1', True),
                 ('0.0.0.0', False),
                 ('::', True)]
        for addr, ipv6 in addrs:
            self.assertEqual(ssbench.util.is_ipv6(addr), ipv6)

    @mock.patch.object(ssbench.util.socket, 'getaddrinfo')
    def test_is_ipv6_tuples(self, mock_getaddrinfo):
        test_tuples = [[(2, 1, 6, '', ('10.0.0.1', 0)),
                        (10, 1, 6, '', ('dead::beef', 0, 0, 0))],
                       [(10, 1, 6, '', ('dead::beef', 0, 0, 0))],
                       [(2, 1, 6, '', ('10.0.0.100', 0))],
                       [(10, 1, 6, '', ('dead::beef', 0, 0, 0)),
                        (2, 1, 6, '', ('10.0.0.1', 0))]]
        expected_values = [False, True, False, False]
        mock_getaddrinfo.side_effect = test_tuples
        for expected in expected_values:
            self.assertEqual(ssbench.util.is_ipv6('host'), expected)
