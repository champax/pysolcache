"""
# -*- coding: utf-8 -*-
# ===============================================================================
#
# Copyright (C) 2013/2025 Laurent Labatut / Laurent Champagnac
#
#
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
# ===============================================================================
"""

import logging
import unittest

import redis
from pysolbase.SolBase import SolBase
from pysolmeters.Meters import Meters

from pysolcache.HighCache import HighCache
from pysolcache.MemoryCache import MemoryCache
from pysolcache.RedisCache import RedisCache

SolBase.voodoo_init()
logger = logging.getLogger(__name__)


class TestHighCache(unittest.TestCase):
    """
    Test description
    """
    currentTempDir = ""

    def setUp(self):
        """
        Setup
        """

        # Reset counters
        Meters.reset()

        # Clear
        self.high_cache = None
        self.memory_cache = None
        self.redis_cache = None

        # Key prefix
        self.key_prefix = "rk_" + str(int(SolBase.mscurrent())) + "_"

        # Temp redis : clear ALL
        r = redis.Redis()
        r.flushall()
        del r

    def tearDown(self):
        """
        Stop
        """

        if self.memory_cache:
            logger.warning("Stopping memory_cache")
            self.memory_cache.stop_cache()
            self.memory_cache = None

        if self.redis_cache:
            logger.warning("Stopping redis_cache")
            self.redis_cache.stop_cache()
            self.redis_cache = None

        if self.high_cache:
            logger.warning("Stopping high_cache")
            self.high_cache.stop_cache()
            self.high_cache = None

        # Temp redis : clear ALL
        r = redis.Redis()
        r.flushall()
        del r

    def test_start_stop(self):
        """
        Test.
        """

        self.memory_cache = MemoryCache()
        self.assertTrue(self.memory_cache._is_started)

        self.redis_cache = RedisCache()
        self.assertTrue(self.redis_cache._is_started)

        self.high_cache = HighCache(memory_cache=self.memory_cache, redis_cache=self.redis_cache)

        self.high_cache.stop_cache()
        self.assertFalse(self.redis_cache._is_started)
        self.assertFalse(self.memory_cache._is_started)

        self.high_cache.start_cache()
        self.assertTrue(self.redis_cache._is_started)
        self.assertTrue(self.memory_cache._is_started)

        self.high_cache.stop_cache()
        self.assertFalse(self.redis_cache._is_started)
        self.assertFalse(self.memory_cache._is_started)

        self.high_cache = None
        self.redis_cache = None
        self.memory_cache = None

    def test_basic(self):
        """
        Test.
        """

        # Alloc
        self.memory_cache = MemoryCache()
        self.redis_cache = RedisCache()
        self.high_cache = HighCache(memory_cache=self.memory_cache, redis_cache=self.redis_cache)

        # Put
        self.high_cache.put(self.key_prefix + "L1L2_IMPLICIT", b"L1L2_IMPLICIT", 60000)
        self.high_cache.put(self.key_prefix + "L1L2_EXPLICIT", b"L1L2_EXPLICIT", 60000, l1=True, l2=True)

        self.high_cache.put(self.key_prefix + "L1_ONLY", b"L1_ONLY", 60000, l1=True, l2=False)
        self.high_cache.put(self.key_prefix + "L2_ONLY", b"L2_ONLY", 60000, l1=False, l2=True)

        self.high_cache.put(self.key_prefix + "NONE", b"NONE", 60000, l1=False, l2=False)

        # Check get
        self.assertEqual(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT"), b"L1L2_IMPLICIT")
        self.assertEqual(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT", l1=True, l2=False), b"L1L2_IMPLICIT")
        self.assertEqual(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT", l1=False, l2=True), b"L1L2_IMPLICIT")

        # Check getex
        v, level = self.high_cache.getex(self.key_prefix + "L1L2_IMPLICIT", l1=True, l2=False)
        self.assertEqual(level, 1)
        self.assertEqual(v, b"L1L2_IMPLICIT")

        v, level = self.high_cache.getex(self.key_prefix + "L1L2_IMPLICIT", l1=False, l2=True)
        self.assertEqual(level, 2)
        self.assertEqual(v, b"L1L2_IMPLICIT")

        v, level = self.high_cache.getex(self.key_prefix + "L1L2_IMPLICIT", l1=False, l2=False)
        self.assertEqual(level, 0)
        self.assertIsNone(v)

        self.assertEqual(self.high_cache.get(self.key_prefix + "L1L2_EXPLICIT"), b"L1L2_EXPLICIT")
        self.assertEqual(self.high_cache.get(self.key_prefix + "L1L2_EXPLICIT", l1=True, l2=False), b"L1L2_EXPLICIT")
        self.assertEqual(self.high_cache.get(self.key_prefix + "L1L2_EXPLICIT", l1=False, l2=True), b"L1L2_EXPLICIT")

        self.assertEqual(self.high_cache.get(self.key_prefix + "L1_ONLY", l1=True, l2=False), b"L1_ONLY")
        self.assertIsNone(self.high_cache.get(self.key_prefix + "L1_ONLY", l1=False, l2=True))
        self.assertEqual(self.high_cache.get(self.key_prefix + "L1_ONLY"), b"L1_ONLY")

        self.assertIsNone(self.high_cache.get(self.key_prefix + "L2_ONLY", l1=True, l2=False))
        self.assertEqual(self.high_cache.get(self.key_prefix + "L2_ONLY", l1=False, l2=True), b"L2_ONLY")
        self.assertEqual(self.high_cache.get(self.key_prefix + "L2_ONLY"), b"L2_ONLY")

        self.assertIsNone(self.high_cache.get(self.key_prefix + "NONE", l1=True, l2=False))
        self.assertIsNone(self.high_cache.get(self.key_prefix + "NONE", l1=False, l2=True))
        self.assertIsNone(self.high_cache.get(self.key_prefix + "NONE"))

        # REMOVE FROM L1
        self.high_cache.remove(self.key_prefix + "L1L2_IMPLICIT", l1=True, l2=False)
        self.assertEqual(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT"), b"L1L2_IMPLICIT")
        self.assertIsNone(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT", l1=True, l2=False))
        self.assertEqual(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT", l1=False, l2=True), b"L1L2_IMPLICIT")

        # REMOVE FROM L2
        self.high_cache.remove(self.key_prefix + "L1L2_IMPLICIT", l1=False, l2=True)
        self.assertIsNone(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT"))
        self.assertIsNone(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT", l1=True, l2=False))
        self.assertIsNone(self.high_cache.get(self.key_prefix + "L1L2_IMPLICIT", l1=False, l2=True))

        # REMOVE FROM BOTH
        self.high_cache.remove(self.key_prefix + "L1L2_EXPLICIT")
        self.assertIsNone(self.high_cache.get(self.key_prefix + "L1L2_EXPLICIT"))
        self.assertIsNone(self.high_cache.get(self.key_prefix + "L1L2_EXPLICIT", l1=True, l2=False))
        self.assertIsNone(self.high_cache.get(self.key_prefix + "L1L2_EXPLICIT", l1=False, l2=True))

        # Stop
        self.high_cache.stop_cache()
        self.assertFalse(self.redis_cache._is_started)
        self.assertFalse(self.memory_cache._is_started)

        self.high_cache = None
        self.redis_cache = None
        self.memory_cache = None
