"""
# -*- coding: utf-8 -*-
# ===============================================================================
#
# Copyright (C) 2013/2017 Laurent Labatut / Laurent Champagnac
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
import random
import unittest

import redis
from gevent.event import Event
from gevent.greenlet import Greenlet
from pysolbase.SolBase import SolBase
from pysolmeters.AtomicInt import AtomicIntSafe
from pysolmeters.Meters import Meters
from redis import Redis

from pysolcache.RedisCache import RedisCache

SolBase.voodoo_init()
logger = logging.getLogger(__name__)


class TestRedisCache(unittest.TestCase):
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

        if self.redis_cache:
            logger.warning("Stopping redis_cache")
            self.redis_cache.stop_cache()
            self.redis_cache = None

        # Temp redis : clear ALL
        r = redis.Redis()
        r.flushall()
        del r

    def test_start_stop(self):
        """
        Test.
        """

        # Alloc
        self.redis_cache = RedisCache()
        self.assertTrue(self.redis_cache._is_started)

        # Stop
        self.redis_cache.stop_cache()
        self.assertFalse(self.redis_cache._is_started)

        # Start
        self.redis_cache.start_cache()
        self.assertTrue(self.redis_cache._is_started)

        # Stop
        self.redis_cache.stop_cache()
        self.assertFalse(self.redis_cache._is_started)

        # Over
        self.redis_cache = None

    def test_init_no_stat(self):
        """
        Test.
        """

        # Alloc
        m = RedisCache()
        self.assertIsNotNone(m)
        m.stop_cache()

    def test_basic(self):
        """
        Test.
        """

        # Alloc
        self.redis_cache = RedisCache()

        # Get : must return nothing
        o = self.redis_cache.get(self.key_prefix + "not_found")
        self.assertIsNone(o)

        # Put
        self.redis_cache.put(self.key_prefix + "keyA", b"valA", 60000)
        o = self.redis_cache.get(self.key_prefix + "keyA")
        self.assertEqual(o, b"valA")

        # Delete
        self.redis_cache.remove(self.key_prefix + "keyA")
        self.assertIsNone(self.redis_cache.get(self.key_prefix + "keyA"))

        # Non bytes injection : must fail
        # noinspection PyBroadException,PyPep8
        try:
            # noinspection PyTypeChecker
            self.redis_cache.put(self.key_prefix + "toto", 12, 1000)
            self.fail("Must fail")
        except:
            pass

        # Non bytes injection : must fail
        # noinspection PyBroadException,PyPep8
        try:
            # noinspection PyTypeChecker
            self.redis_cache.put(self.key_prefix + "toto", u"unicode_buffer", 1000)
            self.fail("Must fail")
        except:
            pass

        # This MUST fail
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            self.redis_cache.put(999, b"value", 60000)
            self.fail("Put a key as non bytes,str MUST fail")
        except Exception:
            pass

        # This MUST fail
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            self.redis_cache.remove(999)
            self.fail("Remove a key as non bytes,str MUST fail")
        except Exception:
            pass

        # Put/Remove
        self.redis_cache.put(self.key_prefix + "todel", b"value", 60000)
        self.assertEqual(self.redis_cache.get(self.key_prefix + "todel"), b"value")
        self.redis_cache.remove(self.key_prefix + "todel")
        self.assertEqual(self.redis_cache.get(self.key_prefix + "todel"), None)

        # Put
        self.redis_cache.put("KEY \u001B\u0BD9\U0001A10D\u1501\xc3", b"zzz", 60000)
        self.assertEqual(self.redis_cache.get("KEY \u001B\u0BD9\U0001A10D\u1501\xc3"), b"zzz")

        # Stop
        self.redis_cache.stop_cache()
        self.redis_cache = None

    def test_basic_external_redis(self):
        """
        Test.
        """

        # External
        r = Redis()

        # Alloc
        self.redis_cache = RedisCache(
            pool_read_d=None,
            pool_write_d=None,
            redis_read_client=r,
            redis_write_client=r,
        )

        # Get : must return nothing
        o = self.redis_cache.get(self.key_prefix + "not_found")
        self.assertIsNone(o)

        # Put
        self.redis_cache.put(self.key_prefix + "keyA", b"valA", 60000)
        o = self.redis_cache.get(self.key_prefix + "keyA")
        self.assertEqual(o, b"valA")

        # Delete
        self.redis_cache.remove(self.key_prefix + "keyA")
        self.assertIsNone(self.redis_cache.get(self.key_prefix + "keyA"))

        # Non bytes injection : must fail
        # noinspection PyBroadException,PyPep8
        try:
            # noinspection PyTypeChecker
            self.redis_cache.put(self.key_prefix + "toto", 12, 1000)
            self.fail("Must fail")
        except:
            pass

        # Non bytes injection : must fail
        # noinspection PyBroadException,PyPep8
        try:
            # noinspection PyTypeChecker
            self.redis_cache.put(self.key_prefix + "toto", u"unicode_buffer", 1000)
            self.fail("Must fail")
        except:
            pass

        # This MUST fail
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            self.redis_cache.put(999, b"value", 60000)
            self.fail("Put a key as non bytes,str MUST fail")
        except Exception:
            pass

        # This MUST fail
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            self.redis_cache.remove(999)
            self.fail("Remove a key as non bytes,str MUST fail")
        except Exception:
            pass

        # Put/Remove
        self.redis_cache.put(self.key_prefix + "todel", b"value", 60000)
        self.assertEqual(self.redis_cache.get(self.key_prefix + "todel"), b"value")
        self.redis_cache.remove(self.key_prefix + "todel")
        self.assertEqual(self.redis_cache.get(self.key_prefix + "todel"), None)

        # Put
        self.redis_cache.put("KEY \u001B\u0BD9\U0001A10D\u1501\xc3", b"zzz", 60000)
        self.assertEqual(self.redis_cache.get("KEY \u001B\u0BD9\U0001A10D\u1501\xc3"), b"zzz")

        # Stop
        self.redis_cache.stop_cache()
        self.redis_cache = None

    def test_basic_ttl(self):
        """
        Test
        :return:
        """

        # Alloc
        self.redis_cache = RedisCache()

        # Put
        self.redis_cache.put(self.key_prefix + "keyA", b"valA", 60000)
        self.redis_cache.put(self.key_prefix + "keyB", b"valB", 1000)
        logger.info("ms cur=%s", SolBase.mscurrent())
        logger.info("A : %s", self.redis_cache.get(self.key_prefix + "keyA"))
        logger.info("B : %s", self.redis_cache.get(self.key_prefix + "keyB"))

        # Wait a bit
        SolBase.sleep(2000)
        logger.info("ms after sleep=%s", SolBase.mscurrent())

        # A : must be present
        # B : must be evicted (TTL elapsed)
        self.assertEqual(self.redis_cache.get(self.key_prefix + "keyA"), b"valA")
        self.assertIsNone(self.redis_cache.get(self.key_prefix + "keyB"))

        self.assertEqual(Meters.aig("rcs.cache_put"), 2)
        self.assertEqual(Meters.aig("rcs.cache_get_hit"), 3)
        self.assertEqual(Meters.aig("rcs.cache_get_miss"), 1)

        # Stop
        self.redis_cache.stop_cache()
        self.redis_cache = None

    def test_max_item_size(self):
        """
        Test
        :return:
        """

        # Alloc
        self.redis_cache = RedisCache(max_single_item_bytes=2)

        # Put
        self.assertTrue(self.redis_cache.put(self.key_prefix + "keyA", b"aa", 60000))
        self.assertFalse(self.redis_cache.put(self.key_prefix + "keyB", b"aaa", 60000))
        logger.info("ms cur=%s", SolBase.mscurrent())
        logger.info("A : %s", self.redis_cache.get(self.key_prefix + "keyA"))
        logger.info("B : %s", self.redis_cache.get(self.key_prefix + "keyB"))

        # A : must be present
        # B : must be out of cache
        self.assertEqual(self.redis_cache.get(self.key_prefix + "keyA"), b"aa")
        self.assertIsNone(self.redis_cache.get(self.key_prefix + "keyB"))

        self.assertEqual(Meters.aig("rcs.cache_put"), 1)
        self.assertEqual(Meters.aig("rcs.cache_get_hit"), 2)
        self.assertEqual(Meters.aig("rcs.cache_get_miss"), 2)

        # Stop
        self.redis_cache.stop_cache()
        self.redis_cache = None

    # ========================
    # BENCH
    # ========================

    def test_bench_greenlet_1_put_100_get_0_max_128000(self):
        """
        Test
        :return:
        """
        self._go_greenlet(1, 100, 0, 128000)

    def test_bench_greenlet_16_put_10_get_1_max_128000(self):
        """
        Test
        :return:
        """
        self._go_greenlet(16, 1, 1, 128000)

    def test_bench_greenlet_128_put_10_get_10_max_128000(self):
        """
        Test
        :return:
        """

        self._go_greenlet(128, 10, 10, 128000)

    def _go_greenlet(self, greenlet_count, put_count, get_count, bench_item_count):
        """
        Doc
        :param greenlet_count: greenlet_count
        :param put_count: put_count
        :param get_count: get_count
        :param bench_item_count: bench_item_count
        """

        g_event = None
        g_array = None
        try:
            # Settings
            g_count = greenlet_count
            g_ms = 10000

            # Continue callback loop
            self.callback_return = True

            # Go
            self.redis_cache = RedisCache()

            # Item count
            self.bench_item_count = bench_item_count
            self.bench_put_weight = put_count
            self.bench_get_weight = get_count
            self.bench_ttl_min_ms = 1000
            self.bench_ttl_max_ms = int(g_ms / 2)

            # Go
            self.run_event = Event()
            self.exception_raised = 0
            self.open_count = 0
            self.thread_running = AtomicIntSafe()
            self.thread_running_ok = AtomicIntSafe()

            # Item per greenlet
            item_per_greenlet = self.bench_item_count / g_count

            # Signal
            self.gorun_event = Event()

            # Alloc greenlet
            g_array = list()
            g_event = list()
            for _ in range(0, g_count):
                greenlet = Greenlet()
                g_array.append(greenlet)
                g_event.append(Event())

            # Run them
            cur_idx = 0
            for idx in range(0, len(g_array)):
                greenlet = g_array[idx]
                event = g_event[idx]
                greenlet.spawn(self._run_cache_bench, event, cur_idx, cur_idx + item_per_greenlet)
                cur_idx += item_per_greenlet
                SolBase.sleep(0)

            # Signal
            self.gorun_event.set()

            # Wait a bit
            dt = SolBase.mscurrent()
            while SolBase.msdiff(dt) < g_ms:
                SolBase.sleep(500)
                # Stat
                ms = SolBase.msdiff(dt)
                sec = float(ms / 1000.0)
                total_put = Meters.aig("rcs.cache_put")
                per_sec_put = round(float(total_put) / sec, 2)
                total_get = Meters.aig("rcs.cache_get_hit") + Meters.aig("rcs.cache_get_miss")
                per_sec_get = round(float(total_get) / sec, 2)

                logger.info("Running..., count=%s, run=%s, ok=%s, put/sec=%s get/sec=%s, cache=%s", self.open_count, self.thread_running.get(), self.thread_running_ok.get(), per_sec_put, per_sec_get, self.redis_cache)
                self.assertEqual(self.exception_raised, 0)

            # Over, signal
            logger.info("Signaling, count=%s", self.open_count)
            self.run_event.set()

            # Wait
            for g in g_event:
                g.wait(30.0)
                self.assertTrue(g.isSet())

            g_event = None
            g_array = None

            # Log
            Meters.write_to_logger()
        finally:
            self.run_event.set()
            if g_event:
                for g in g_event:
                    g.set()

            if g_array:
                for g in g_array:
                    g.kill()

            if self.redis_cache:
                self.redis_cache.stop_cache()
                self.redis_cache = None

    def _run_cache_bench(self, event, idx_min, idx_max):
        """
        Run
        :param idx_min: Index min
        :param idx_max: Index max
        """

        idx_max -= 1

        # Wait
        self.gorun_event.wait()

        # Go
        cur_count = 0
        logger.debug("Entering now, idx_min=%s, idx_max=%s", idx_min, idx_max)
        self.thread_running.increment()
        self.thread_running_ok.increment()
        try:
            while not self.run_event.isSet():
                cur_count += 1
                try:
                    cur_item = random.randint(idx_min, idx_max)
                    s_cur_item = "%s" % cur_item
                    b_cur_item = SolBase.unicode_to_binary(s_cur_item, "utf-8")
                    cur_ttl = random.randint(self.bench_ttl_min_ms, self.bench_ttl_max_ms)
                    for _ in range(0, self.bench_put_weight):
                        self.redis_cache.put(s_cur_item, b_cur_item, cur_ttl)
                        SolBase.sleep(0)

                    for _ in range(0, self.bench_get_weight):
                        v = self.redis_cache.get(s_cur_item)
                        if v:
                            self.assertEqual(v, b_cur_item)
                        SolBase.sleep(0)
                except Exception as e:
                    self.exception_raised += 1
                    logger.warning("Ex=%s", SolBase.extostr(e))
                    self.thread_running_ok.increment(-1)
                    return
                finally:
                    pass
        finally:
            self.assertGreater(cur_count, 0)
            logger.debug("Exiting")
            event.set()
            self.thread_running.increment(-1)
