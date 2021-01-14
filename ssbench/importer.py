#
#Copyright (c) 2012-2021, NVIDIA CORPORATION.
#SPDX-License-Identifier: Apache-2.0

# Handle any/all wacky imports here

try:
    from random import SystemRandom
    random = SystemRandom()
except ImportError:
    import random  # noqa
