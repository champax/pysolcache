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

from gevent.event import Event
from gevent.greenlet import Greenlet
from pysolbase.SolBase import SolBase
from pysolmeters.AtomicInt import AtomicIntSafe
from pysolmeters.Meters import Meters

from pysolcache import max_int
from pysolcache.MemoryCache import MemoryCache

SolBase.voodoo_init()
logger = logging.getLogger(__name__)


class TestMemoryCache(unittest.TestCase):
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
        self.mem_cache = None

        self.callback_call = 0
        self.callback_evicted = 0
        self.callback_return = False

        self.evict_count = 0
        self.evict_last_key = None
        self.evict_last_value = None

    def tearDown(self):
        """
        Stop
        """

        if self.mem_cache:
            logger.warning("Stopping mem_cache")
            self.mem_cache.stop_cache()
            self.mem_cache = None

    def watchdog_callback(self, evicted_count):
        """
        Callback
        :param evicted_count: Evicted count
        :return: True if reschedule the callback, False otherwise
        """

        logger.info("Called, evicted_count=%s", evicted_count)

        self.callback_call += 1
        self.callback_evicted += evicted_count
        return self.callback_return

    def eviction_callback(self, k, v):
        """
        Eviction callback
        :param k: key
        :param v: value
        """

        self.evict_count += 1
        self.evict_last_key = k
        self.evict_last_value = v

    def test_start_stop(self):
        """
        Test.
        """

        # Alloc
        self.mem_cache = MemoryCache()
        self.assertTrue(self.mem_cache._is_started)

        # Stop
        self.mem_cache.stop_cache()
        self.assertFalse(self.mem_cache._is_started)

        # Start
        self.mem_cache.start_cache()
        self.assertTrue(self.mem_cache._is_started)

        # Stop
        self.mem_cache.stop_cache()
        self.assertFalse(self.mem_cache._is_started)

        # Over
        self.mem_cache = None

    def test_size_handling(self):
        """
        Test.
        """

        # Alloc
        self.mem_cache = MemoryCache()

        # Put
        self.mem_cache.put("A", b"A1", 60000)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 1 + 2)

        # Put again, not the same data
        self.mem_cache.put("A", b"A11", 60000)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 1 + 3)

        # Put again, not the same data
        self.mem_cache.put("A", b"A", 60000)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 1 + 1)

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_init_no_stat(self):
        """
        Test.
        """

        # Alloc
        m = MemoryCache()
        self.assertIsNotNone(m)

    def test_basic(self):
        """
        Test.
        """

        # Alloc
        self.mem_cache = MemoryCache()

        # Get : must return nothing
        o = self.mem_cache.get("not_found")
        self.assertIsNone(o)

        # Put
        self.mem_cache.put("keyA", b"valA", 60000)
        o = self.mem_cache.get("keyA")
        self.assertEqual(o, b"valA")
        o = self.mem_cache._get_raw("keyA")
        self.assertIsNotNone(o)
        self.assertIsInstance(o, tuple)
        self.assertEqual(o[1], b"valA")
        self.assertLessEqual(o[0] - SolBase.mscurrent(), 60000)
        logger.info("TTL approx=%s", o[0] - SolBase.mscurrent())

        # Put with lower TTL : TTL MUST BE UPDATED
        self.mem_cache.put("keyA", b"valA", 30000)
        o = self.mem_cache.get("keyA")
        self.assertEqual(o, b"valA")
        o = self.mem_cache._get_raw("keyA")
        self.assertIsNotNone(o)
        self.assertIsInstance(o, tuple)
        self.assertEqual(o[1], b"valA")
        self.assertLessEqual(o[0] - SolBase.mscurrent(), 30000)
        logger.info("TTL approx=%s", o[0] - SolBase.mscurrent())

        # Non bytes injection : must fail
        # noinspection PyBroadException,PyPep8
        try:
            # noinspection PyTypeChecker
            self.mem_cache.put("toto", 12, 1000)
            self.fail("Must fail")
        except:
            pass

        # Non bytes injection : must fail
        # noinspection PyBroadException,PyPep8
        try:
            # noinspection PyTypeChecker
            self.mem_cache.put("toto", u"unicode_buffer", 1000)
            self.fail("Must fail")
        except:
            pass

        # This MUST fail
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            self.mem_cache.put(999, b"value", 60000)
            self.fail("Put a key as non bytes,str MUST fail")
        except Exception:
            pass

        # This MUST fail
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            self.mem_cache.remove(999)
            self.fail("Remove a key as non bytes,str MUST fail")
        except Exception:
            pass

        # Put/Remove
        self.mem_cache.put("todel", b"value", 60000)
        self.assertEqual(self.mem_cache.get("todel"), b"value")
        self.mem_cache.remove("todel")
        self.assertEqual(self.mem_cache.get("todel"), None)

        # Put
        self.mem_cache.put("KEY \u001B\u0BD9\U0001A10D\u1501\xc3", b"zzz", 60000)
        self.assertEqual(self.mem_cache.get("KEY \u001B\u0BD9\U0001A10D\u1501\xc3"), b"zzz")

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_basic_eviction_max_capacity_lru(self):
        """
        Test
        :return:
        """

        # Alloc
        self.mem_cache = MemoryCache(max_item=3, cb_evict=self.eviction_callback)

        # Put
        self.mem_cache.put("keyA", b"valA", 60000)
        self.mem_cache.put("keyB", b"valB", 60000)
        self.mem_cache.put("keyC", b"valC", 60000)

        # Use A and C => B becomes the older to be used
        self.mem_cache.get("keyA")
        self.mem_cache.get("keyC")

        # We are maxed (3 items)
        # Add D => B must be kicked
        self.mem_cache.put("keyD", b"valD", 60000)
        self.assertEqual(self.mem_cache.get("keyA"), b"valA")
        self.assertEqual(self.mem_cache.get("keyC"), b"valC")
        self.assertEqual(self.mem_cache.get("keyD"), b"valD")
        self.assertIsNone(self.mem_cache.get("keyB"))
        self.assertEqual(self.evict_count, 1)
        self.assertEqual(self.evict_last_key, "keyB")
        self.assertEqual(self.evict_last_value, b"valB")
        self.assertEqual(Meters.aig("mcs.cache_evict_lru_put"), 1)

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_basic_eviction_lru(self):
        """
        Test
        :return:
        """

        # Alloc
        self.mem_cache = MemoryCache(cb_evict=self.eviction_callback)

        # Put
        self.mem_cache.put("keyA", b"valA", 60000)
        self.mem_cache.put("keyB", b"valB", 60000)
        self.mem_cache.put("keyC", b"valC", 60000)

        # Evict LRU
        self.mem_cache._evict_key_lru()

        # A was put first => must be kicked
        self.assertIsNone(self.mem_cache.get("keyA"))
        self.assertEqual(self.mem_cache.get("keyB"), b"valB")
        self.assertEqual(self.mem_cache.get("keyC"), b"valC")
        self.assertEqual(self.evict_count, 1)
        self.assertEqual(self.evict_last_key, "keyA")
        self.assertEqual(self.evict_last_value, b"valA")

        # Evict LRU
        self.mem_cache._evict_key_lru()

        # B was put first => must be kicked
        self.assertIsNone(self.mem_cache.get("keyA"))
        self.assertIsNone(self.mem_cache.get("keyB"))
        self.assertEqual(self.mem_cache.get("keyC"), b"valC")
        self.assertEqual(self.evict_count, 2)
        self.assertEqual(self.evict_last_key, "keyB")
        self.assertEqual(self.evict_last_value, b"valB")

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_basic_eviction_with_get_lru(self):
        """
        Test
        :return:
        """

        # Alloc
        self.mem_cache = MemoryCache(cb_evict=self.eviction_callback)

        # Put
        self.mem_cache.put("keyA", b"valA", 60000)
        self.mem_cache.put("keyB", b"valB", 60000)

        # A was put first => must be kicked, but we GET IT => B must be kicked
        self.assertEqual(self.mem_cache.get("keyA"), b"valA")

        # Evict LRU
        self.mem_cache._evict_key_lru()

        # A was used more recently => B must be kicked
        self.assertIsNone(self.mem_cache.get("keyB"))
        self.assertEqual(self.mem_cache.get("keyA"), b"valA")
        self.assertEqual(self.evict_count, 1)
        self.assertEqual(self.evict_last_key, "keyB")
        self.assertEqual(self.evict_last_value, b"valB")

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_basic_eviction_with_get_ttl(self):
        """
        Test
        :return:
        """

        # Alloc
        self.mem_cache = MemoryCache(cb_evict=self.eviction_callback)

        # Put
        self.mem_cache.put("keyA", b"valA", 60000)
        self.mem_cache.put("keyB", b"valB", 500)
        logger.info("ms cur=%s", SolBase.mscurrent())
        logger.info("A : %s", self.mem_cache._get_raw("keyA"))
        logger.info("B : %s", self.mem_cache._get_raw("keyB"))

        # Wait a bit
        SolBase.sleep(600)
        logger.info("ms after sleep=%s", SolBase.mscurrent())

        # A : must be present
        # B : must be evicted (TTL elapsed)
        self.assertEqual(self.mem_cache.get("keyA"), b"valA")
        self.assertIsNone(self.mem_cache.get("keyB"))
        self.assertEqual(self.evict_count, 1)
        self.assertEqual(self.evict_last_key, "keyB")
        self.assertEqual(self.evict_last_value, b"valB")
        self.assertEqual(Meters.aig("mcs.cache_evict_ttl_get"), 1)

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_basic_eviction_with_watchdog_ttl(self):
        """
        Test
        :return:
        """

        # Go
        self.mem_cache = MemoryCache(watchdog_interval_ms=1000, cb_watchdog=self.watchdog_callback, cb_evict=self.eviction_callback)

        # Put
        self.mem_cache.put("keyA", b"valA", 60000)
        self.mem_cache.put("keyB", b"valB", 500)
        self.mem_cache.put("keyC", b"valC", 500)
        self.mem_cache.put("keyD", b"valD", 60000)
        logger.info("ms cur=%s", SolBase.mscurrent())
        logger.info("A : %s", self.mem_cache._get_raw("keyA"))
        logger.info("B : %s", self.mem_cache._get_raw("keyB"))
        logger.info("C : %s", self.mem_cache._get_raw("keyC"))
        logger.info("D : %s", self.mem_cache._get_raw("keyD"))

        # Wait a bit
        ms_start = SolBase.mscurrent()
        while SolBase.msdiff(ms_start) < (1000.0 * 2.0):
            if self.callback_call > 0:
                break
            else:
                SolBase.sleep(10)
        logger.info("ms after wait=%s", SolBase.mscurrent())
        logger.info("_hash_key = %s", self.mem_cache._hash_key)
        logger.info("_hash_context = %s", self.mem_cache._hash_context)

        # A : must be present
        # B : must be evicted (TTL elapsed, by watchdog)
        self.assertEqual(self.callback_call, 1)
        self.assertEqual(self.callback_evicted, 2)
        self.assertFalse("valB" in self.mem_cache._hash_key)
        self.assertFalse("valB" in self.mem_cache._hash_context)
        self.assertFalse("valC" in self.mem_cache._hash_key)
        self.assertFalse("valC" in self.mem_cache._hash_context)
        self.assertIsNone(self.mem_cache.get("keyB"))
        self.assertIsNone(self.mem_cache.get("keyC"))
        self.assertEqual(self.mem_cache.get("keyA"), b"valA")
        self.assertEqual(self.mem_cache.get("keyD"), b"valD")
        self.assertEqual(self.evict_count, 2)
        self.assertTrue(self.evict_last_key == "keyB" or self.evict_last_key == "keyC")
        self.assertTrue(self.evict_last_value == b"valB" or self.evict_last_value == b"valC")

        self.assertEqual(Meters.aig("mcs.cache_evict_ttl_watchdog"), 2)

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_watchdog_schedule(self):
        """
        Test
        :return:
        """

        # Continue callback loop
        self.callback_return = True

        # Go
        self.mem_cache = MemoryCache(watchdog_interval_ms=500, cb_watchdog=self.watchdog_callback)

        # Put
        self.mem_cache.put("keyA", b"valA", 60000)
        self.mem_cache.put("keyD", b"valD", 60000)

        # Wait a bit
        ms_start = SolBase.mscurrent()
        while SolBase.msdiff(ms_start) < (500.0 * 5.0):
            if self.callback_call >= 1:
                logger.info("Run 1 exit")
                break
            else:
                SolBase.sleep(1)
        logger.info("Run 1 done")

        self.assertEqual(self.callback_call, 1)
        self.assertEqual(self.callback_evicted, 0)

        # Wait a bit
        ms_start = SolBase.mscurrent()
        while SolBase.msdiff(ms_start) < (500.0 * 5.0):
            if self.callback_call >= 2:
                logger.info("Run 2 exit")
                break
            else:
                SolBase.sleep(1)
        logger.info("Run 2 done")

        self.assertGreaterEqual(self.callback_call, 2)
        self.assertEqual(self.callback_evicted, 0)

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

        # Wait a bit
        SolBase.sleep(500 * 2)

        # Nothing must happen
        self.assertEqual(self.callback_call, 2)
        self.assertEqual(self.callback_evicted, 0)

    # ========================
    # BENCH
    # ========================

    def test_bench_greenlet_1_put_100_get_0_max_128000(self):
        """
        Test
        :return:
        """
        self._go_greenlet(1, 100, 0, 128000)

    def test_bench_greenlet_16_put_10_get_1_max_50(self):
        """
        Test
        :return:
        """
        self._go_greenlet(16, 1, 1, 128000, max_item=50)

    def test_bench_greenlet_16_put_10_get_1_maxsize_300bytes(self):
        """
        Test
        :return:
        """

        self._go_greenlet(
            16, 1, 1, 128000,
            watchdog_interval_ms=500,
            max_item=50,
            max_bytes=300,
            max_single_item_bytes=50,
            purge_min_bytes=20,
            purge_min_count=5,
        )

    def test_bench_greenlet_1_put_1_get_100_max_128000(self):
        """
        Test
        :return:
        """
        self._go_greenlet(1, 1, 100, 128000)

    def test_bench_greenlet_128_put_10_get_10_max_128000(self):
        """
        Test
        :return:
        """

        self._go_greenlet(128, 10, 10, 128000)

    def test_bench_greenlet_128_put_10_get_10_maxsize_10000(self):
        """
        Test
        :return:
        """
        self._go_greenlet(
            128, 10, 10, 128000,
            watchdog_interval_ms=500,
            max_item=max_int,
            max_bytes=10000,
            max_single_item_bytes=50,
            purge_min_bytes=int(10000 / 2),
            purge_min_count=5000,
        )

    def _go_greenlet(self, greenlet_count, put_count, get_count, bench_item_count,
                     watchdog_interval_ms=60000,
                     max_item=128000,
                     max_bytes=32 * 1024 * 1024,
                     max_single_item_bytes=1 * 1024 * 1024,
                     purge_min_bytes=8 * 1024 * 1024,
                     purge_min_count=1000):
        """
        Doc
        :param greenlet_count: greenlet_count
        :param put_count: put_count
        :param get_count: get_count
        :param bench_item_count: bench_item_count
        :param watchdog_interval_ms: watchdog_interval_ms
        :param max_item: max_item
        :param max_bytes: max_bytes
        :param max_single_item_bytes: max_single_item_bytes
        :param purge_min_bytes: purge_min_bytes
        :param purge_min_count: purge_min_count
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
            self.mem_cache = MemoryCache(
                watchdog_interval_ms=watchdog_interval_ms,
                max_item=max_item,
                max_bytes=max_bytes,
                max_single_item_bytes=max_single_item_bytes,
                purge_min_bytes=purge_min_bytes,
                purge_min_count=purge_min_count
            )

            # Item count
            self.bench_item_count = bench_item_count
            self.bench_put_weight = put_count
            self.bench_get_weight = get_count
            self.bench_ttl_min_ms = 1000
            self.bench_ttl_max_ms = g_ms / 2

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
                total_put = Meters.aig("mcs.cache_put")
                per_sec_put = round(float(total_put) / sec, 2)
                total_get = Meters.aig("mcs.cache_get_hit") + Meters.aig("mcs.cache_get_miss")
                per_sec_get = round(float(total_get) / sec, 2)

                logger.info("Running..., count=%s, run=%s, ok=%s, put/sec=%s get/sec=%s, cache=%s", self.open_count, self.thread_running.get(), self.thread_running_ok.get(), per_sec_put, per_sec_get, self.mem_cache)
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

            if self.mem_cache:
                max_count = 0
                total_size = 0
                i = 0
                for (k, v) in self.mem_cache._hash_key.items():
                    i += 1
                    total_size += len(k) + len(v[1])
                    if i < max_count:
                        logger.info("%s => %s", k, v)
                self.assertEqual(total_size, self.mem_cache._current_data_bytes.get())

                self.mem_cache.stop_cache()
                self.mem_cache = None

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
                        self.mem_cache.put(s_cur_item, b_cur_item, cur_ttl)
                        SolBase.sleep(0)

                    for _ in range(0, self.bench_get_weight):
                        v = self.mem_cache.get(s_cur_item)
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

    # =====================================
    # SIZE STUFF
    # =====================================

    def test_size_limit_on_data_size_first(self):
        """
        Test.
        """

        # Alloc
        self.mem_cache = MemoryCache(
            max_bytes=5 * 10 * 2,
            max_single_item_bytes=6 * 2,
            purge_min_bytes=5 * 5 * 2,
            purge_min_count=2,
            max_item=max_int,
        )

        # Put 10 items
        for i in range(10, 20):
            self.mem_cache.put("key" + str(i), SolBase.unicode_to_binary("val%s" % i, "utf-8"), 60000)

        logger.info("Cache=%s", self.mem_cache)

        # Must have all of them
        for i in range(10, 20):
            self.assertEqual(self.mem_cache.get("key" + str(i)), SolBase.unicode_to_binary("val%s" % i, "utf-8"))
        self.assertEqual(len(self.mem_cache._hash_key), 10)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * 10 * 2)

        # Then add a new one : we will over size the cache
        self.mem_cache.put("key" + str(99), SolBase.unicode_to_binary("val99", "utf-8"), 60000)

        # We must have evicted AT least :
        # 5*5 + 5 bytes => 5 items + the item added => 6 items
        # 2 items minimum
        # So 6 items : 10 to 15 must be evicted
        logger.info("Hash = %s", self.mem_cache._hash_key)
        for i in range(10, 16):
            self.assertIsNone(self.mem_cache.get("key" + str(i)))
        for i in range(16, 20):
            self.assertEqual(self.mem_cache.get("key" + str(i)), SolBase.unicode_to_binary("val%s" % i, "utf-8"))
        self.assertEqual(self.mem_cache.get("key" + str(99)), SolBase.unicode_to_binary("val99", "utf-8"))
        self.assertEqual(len(self.mem_cache._hash_key), 5)
        self.assertEqual(len(self.mem_cache._hash_context), 5)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * 5 * 2)

        # Try add a big one this time : must not be done (over limit)
        self.mem_cache.put("BIGDATA", b"aaaaaaaaaaaaaaaaaaaa", 60000)
        self.assertIsNone(self.mem_cache.get("BIGDATA"))

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_size_limit_on_min_item_to_evict_first(self):
        """
        Test.
        """

        # Alloc

        self.mem_cache = MemoryCache(
            max_bytes=5 * 10 * 2,
            max_single_item_bytes=6 * 2,
            purge_min_bytes=1 * 5,
            purge_min_count=6,
            max_item=max_int,
        )

        # Put 10 items
        for i in range(10, 20):
            self.mem_cache.put("key" + str(i), SolBase.unicode_to_binary("val%s" % i, "utf-8"), 60000)

        logger.info("Cache=%s", self.mem_cache)

        # Must have all of them
        for i in range(10, 20):
            self.assertEqual(self.mem_cache.get("key" + str(i)), SolBase.unicode_to_binary("val%s" % i, "utf-8"))
        self.assertEqual(len(self.mem_cache._hash_key), 10)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * len(self.mem_cache._hash_key) * 2)

        # Then add a new one : we will over size the cache
        self.mem_cache.put("key" + str(99), SolBase.unicode_to_binary("val99", "utf-8"), 60000)

        # We must have evicted AT least :
        # 1*5 + 5 bytes => 1 items + the item added => 2 items
        # 6 items minimum
        # So : 10 to 15 must be evicted
        logger.info("Hash = %s", self.mem_cache._hash_key)
        for i in range(10, 16):
            self.assertIsNone(self.mem_cache.get("key" + str(i)))
        for i in range(16, 20):
            self.assertEqual(self.mem_cache.get("key" + str(i)), SolBase.unicode_to_binary("val%s" % i, "utf-8"))
        self.assertEqual(self.mem_cache.get("key" + str(99)), SolBase.unicode_to_binary("val99", "utf-8"))
        self.assertEqual(len(self.mem_cache._hash_key), 5)
        self.assertEqual(len(self.mem_cache._hash_context), 5)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * len(self.mem_cache._hash_key) * 2)

        # Try add a big one this time : must not be done (over limit)
        self.mem_cache.put("BIGDATA", b"aaaaaaaaaaaaaaaaaaaa", 60000)
        self.assertIsNone(self.mem_cache.get("BIGDATA"))
        self.mem_cache.put("aaaaaaaaaaaaaaaaaaaa", b"BIGKEY", 60000)
        self.assertIsNone(self.mem_cache.get("aaaaaaaaaaaaaaaaaaaa"))
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * len(self.mem_cache._hash_key) * 2)

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_size_limit_cache_clear(self):
        """
        Test.
        """

        # Alloc
        self.mem_cache = MemoryCache(
            max_bytes=5 * 10 * 2,
            max_single_item_bytes=6 *2,
            purge_min_bytes=1 * 5,
            purge_min_count=100,
            max_item=max_int,
        )

        # Put 10 items
        for i in range(10, 20):            
            self.mem_cache.put("key" + str(i), SolBase.unicode_to_binary("val%s" % i, "utf-8"), 60000)

        logger.info("Cache=%s", self.mem_cache)

        # Must have all of them
        for i in range(10, 20):
            self.assertEqual(self.mem_cache.get("key" + str(i)), SolBase.unicode_to_binary("val%s" % i, "utf-8"))
        self.assertEqual(len(self.mem_cache._hash_key), 10)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * len(self.mem_cache._hash_key) * 2)

        # Then add a new one : we will over size the cache
        self.mem_cache.put("key" + str(99), SolBase.unicode_to_binary("val99", "utf-8"), 60000)

        # We must have evicted AT least :
        # 1*5 + 5 bytes => 1 items + the item added => 2 items
        # 100 items minimum
        # So : All items must be kicked, will remain only the new one
        logger.info("Hash = %s", self.mem_cache._hash_key)
        self.assertEqual(self.mem_cache.get("key" + str(99)), SolBase.unicode_to_binary("val99", "utf-8"))
        self.assertEqual(len(self.mem_cache._hash_key), 1)
        self.assertEqual(len(self.mem_cache._hash_context), 1)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * len(self.mem_cache._hash_key) * 2)

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None

    def test_size_limit_max_item_count(self):
        """
        Test.
        """

        # Alloc
        self.mem_cache = MemoryCache(
            max_bytes=max_int,
            max_single_item_bytes=6 * 2,
            purge_min_bytes=1 * 5,
            purge_min_count=0,
            max_item=10
        )

        # Put 10 items
        for i in range(10, 20):
            self.mem_cache.put("key" + str(i), SolBase.unicode_to_binary("val%s" % i, "utf-8"), 60000)

        logger.info("Cache=%s", self.mem_cache)

        # Must have all of them
        for i in range(10, 20):
            self.assertEqual(self.mem_cache.get("key" + str(i)), SolBase.unicode_to_binary("val%s" % i, "utf-8"))
        self.assertEqual(len(self.mem_cache._hash_key), 10)
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * len(self.mem_cache._hash_key) * 2)

        # Then add a new one : we will over size the cache
        self.mem_cache.put("key" + str(99), SolBase.unicode_to_binary("val99", "utf-8"), 60000)

        # We must have evicted only first added
        logger.info("Hash = %s", self.mem_cache._hash_key)
        self.assertIsNone(self.mem_cache.get("key" + str(10)))
        for i in range(11, 20):
            self.assertEqual(self.mem_cache.get("key" + str(i)), SolBase.unicode_to_binary("val%s" % i, "utf-8"))
        self.assertEqual(self.mem_cache.get("key" + str(99)), SolBase.unicode_to_binary("val99", "utf-8"))
        self.assertEqual(self.mem_cache._current_data_bytes.get(), 5 * len(self.mem_cache._hash_key) * 2)

        # Stop
        self.mem_cache.stop_cache()
        self.mem_cache = None
