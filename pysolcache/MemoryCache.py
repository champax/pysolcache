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
from collections import OrderedDict
from threading import Lock

import gevent
from pysolbase.SolBase import SolBase
from pysolmeters.AtomicInt import AtomicIntSafe
from pysolmeters.Meters import Meters

logger = logging.getLogger(__name__)


class MemoryCache(object):
    """
    A binary memory cache, supporting :
    - Keys : str
    - Values : bytes or str (str len computation may be approximate compared to real memory usage)
    - Max bytes
    - Max items count
    - Items TTL
    - LRU evictions
    """

    def __init__(self,
                 watchdog_interval_ms=60000,
                 max_item=128000,
                 max_bytes=32 * 1024 * 1024,
                 max_single_item_bytes=1 * 1024 * 1024,
                 purge_min_bytes=8 * 1024 * 1024,
                 purge_min_count=1000,
                 cb_watchdog=None,
                 cb_evict=None,
                 meters_prefix=""):
        """
        Memory cache
        :param watchdog_interval_ms: watchdog interval in ms
        :type watchdog_interval_ms: int
        :param max_item: max items in cache
        :type max_item: int
        :param max_bytes: max bytes in cache (keys AND values)
        :type max_bytes: int
        :param max_single_item_bytes: max single item bytes (if greater : no cache) (keys + values)
        :type max_single_item_bytes: int
        :param purge_min_bytes: purge minimum bytes
        :type purge_min_bytes: int
        :param purge_min_count: purge minimum count
        :type purge_min_count: int
        :param cb_watchdog: callback for unittest
        :param cb_evict: callback for unittest
        :param meters_prefix: Prefix for meters names
        :type meters_prefix: str
        """

        # Params
        self._watchdog_interval_ms = watchdog_interval_ms
        self._max_item = max_item
        self._max_bytes = max_bytes
        self._max_single_item_bytes = max_single_item_bytes
        self._purge_min_bytes = purge_min_bytes
        self._purge_min_count = purge_min_count

        # Internal memory structure
        self._is_started = False
        self._run_lock = Lock()
        self._hash_key = dict()
        self._hash_context = OrderedDict()
        self._watchdog_greenlet = None
        self._current_data_bytes = AtomicIntSafe()

        # Size purge lock (only one at a time)
        self._size_purge_lock = Lock()

        # Unittest
        self._cb_watchdog = cb_watchdog
        self._cb_evict = cb_evict

        # Meters
        self.meters_prefix = meters_prefix

        # Initialize
        self.start_cache()

    def __del__(self):
        """
        Destructor
        """

        if self._is_started:
            self.stop_cache()

    def __str__(self):
        """
        Str override
        :return str
        :rtype: str
        """

        return "id={0}*hash={1}/{2}*put/bypass/hit/miss={3}/{4}/{5}/{6}*evict.w/g/lru/lrusize={7}/{8}/{9}/{10}*ex={11}/{12}*bytes={13}".format(
            id(self),
            len(self._hash_key),
            len(self._hash_context),
            Meters.aig(self.meters_prefix + "mcs.cache_put"),
            Meters.aig(self.meters_prefix + "mcs.cache_put_too_big"),
            Meters.aig(self.meters_prefix + "mcs.cache_get_hit"),
            Meters.aig(self.meters_prefix + "mcs.cache_get_miss"),
            Meters.aig(self.meters_prefix + "mcs.cache_evict_ttl_watchdog"),
            Meters.aig(self.meters_prefix + "mcs.cache_evict_ttl_get"),
            Meters.aig(self.meters_prefix + "mcs.cache_evict_lru_put"),
            Meters.aig(self.meters_prefix + "mcs.cache_evict_lru_size_put"),
            Meters.aig(self.meters_prefix + "mcs.cache_ex"),
            Meters.aig(self.meters_prefix + "mcs.cache_purge_failed"),
            self._current_data_bytes.get())

    def start_cache(self):
        """
        Start the cache
        """

        logger.info("Entering, id=%s", id(self))

        with self._run_lock:
            try:
                # Check
                if self._is_started:
                    logger.warning("_is_started=True, doing nothing")
                    return

                # Log
                logger.info("Starting now")
                logger.info("_max_items=%s", self._max_item)
                logger.info("_watchdog_interval_ms=%s", self._watchdog_interval_ms)

                # Started
                self._is_started = True

                # Start greenlet
                logger.info("scheduling initial greenlet")
                self._watchdog_greenlet = gevent.spawn_later(self._watchdog_interval_ms * 0.001, self._watchdog_run)

                # Done
                logger.info("started")
            except Exception as e:
                logger.error("Exception, stopping, e=%s", SolBase.extostr(e))
                self.stop_cache()

        # Wait
        SolBase.sleep(0)

    def stop_cache(self):
        """
        Stop the cache
        :return
        """

        with self._run_lock:
            try:
                # Check
                if not self._is_started:
                    return

                # Signal
                self._is_started = False

                # Kill greenlet
                if self._watchdog_greenlet:
                    self._watchdog_greenlet.kill(block=True)
                    self._watchdog_greenlet = None

            except Exception as e:
                logger.error("Exception, ex=%s", SolBase.extostr(e))
            finally:
                # Reset
                self._is_started = False
                self._watchdog_greenlet = None

    def _schedule_next_watchdog(self):
        """
        Schedule next run.
        """

        try:
            # Started ?
            if not self._is_started:
                return

            with self._run_lock:
                # Re-check
                if not self._is_started:
                    return

                # Yes, schedule
                self._watchdog_greenlet = gevent.spawn_later(self._watchdog_interval_ms * 0.001, self._watchdog_run)

            # Wait
            SolBase.sleep(0)

        except Exception as e:
            logger.error("_schedule_next_watchdog : Exception, e=%s", SolBase.extostr(e))
        finally:
            pass

    def _watchdog_run(self):
        """
        Watch dog
        :return Nothing
        """

        if not self._is_started:
            return

        reschedule = True
        try:
            # Current meters
            Meters.aii(self.meters_prefix + "mcs.cur_bytes", self._current_data_bytes.get())
            Meters.aii(self.meters_prefix + "mcs.cur_size_hash", len(self._hash_key))

            # Evict
            ms = SolBase.mscurrent()
            evicted_count = self._evict_all_expired_keys()

            # Check (evict can take some time)
            if not self._is_started:
                return

            Meters.dtci(self.meters_prefix + "mcs.cache_dtc_watchdog", SolBase.msdiff(ms))

            # Stat
            if evicted_count > 0:
                Meters.aii(self.meters_prefix + "mcs.cache_evict_ttl_watchdog", evicted_count)

            # Callback (unittest)
            if self._cb_watchdog:
                reschedule = self._cb_watchdog(evicted_count)

        except Exception as e:
            if self._is_started:
                logger.error("_watchdog_run : Exception, id=%s, e=%s", id(self), SolBase.extostr(e))
                Meters.aii(self.meters_prefix + "mcs.cache_ex")
            else:
                logger.debug("_watchdog_run : Exception, id=%s, e=%s", id(self), SolBase.extostr(e))
                reschedule = False
        finally:
            Meters.aii(self.meters_prefix + "mcs.cache_watchdog_run_count")
            # Schedule next write
            if reschedule and self._is_started:
                self._schedule_next_watchdog()

    # ========================================
    # HELPERS
    # ========================================

    def _notify_eviction(self, k, v):
        """
        Notify eviction
        :param k: Key
        :type k: str
        :param v: Value
        :type v: bytes,str
        :return Nothing
        """

        # Decrement current size
        self._current_data_bytes.increment(-(len(k) + len(v)))

        # Callback
        if self._cb_evict:
            self._cb_evict(k, v)
            logger.debug("evicted, key=%s", k)

    @classmethod
    def _is_expired(cls, ttl_ms, cur_ms):
        """
        Return true if ttl is expired
        :param ttl_ms: Ttl
        :type ttl_ms: float
        :param ttl_ms: Current ms
        :type ttl_ms: float
        :return bool
        """

        return ttl_ms <= cur_ms

    def _evict_key_lru(self):
        """
        Evict the least recent use key
        :return bytes count evicted
        :rtype int
        """

        # _hash_context is ordered
        # We are in "FIFO" : last=False

        kv = self._hash_context.popitem(last=False)

        # This one is removed from _hash_context
        # Kick it from normal dict (we receive a tuple(key, value)
        del (self._hash_key[kv[0]])

        # Notify
        self._notify_eviction(kv[0], kv[1][1])

        return len(kv[0]) + len(kv[1][1])

    def _evict_all_expired_keys(self):
        """
        Evict all expired keys
        :return The evicted key count
        :rtype: int
        """

        # We use the _hash_key directly since we don't care order here
        tu_to_evict = list()
        cur_ms = SolBase.mscurrent()
        for tu in self._hash_key.items():
            # v is a tuple (ms, object)
            if self._is_expired(tu[1][0], cur_ms):
                tu_to_evict.append(tu)

        # Now evict
        for (k, v) in tu_to_evict:
            self._safe_unhash(k)
            self._notify_eviction(k, v[1])

        return len(tu_to_evict)

    def _purge_cache(self, len_to_add):
        """
        Purge cache if required
        :param len_to_add:  Value to add
        :type len_to_add: int
        :return Nothing
        """

        # --------------------
        # MAX SIZE
        # --------------------
        if self._max_bytes:
            # Size limit... check
            if (self._current_data_bytes.get() + len_to_add) > self._max_bytes:
                # -------------------------------------
                # SIZE PURGE : IN LOCK, ONLY ONE AT A TIME
                # => We DON'T want the cache to be blasted by several purge @ same time
                # -------------------------------------
                with self._size_purge_lock:
                    # RECHECK
                    if (self._current_data_bytes.get() + len_to_add) > self._max_bytes:
                        # Size limit : Reached => Need to prune the cache
                        bytes_to_purge = self._purge_min_bytes + len_to_add
                        items_to_purge = self._purge_min_count
                        bytes_purged = 0
                        item_purged = 0

                        # Purge
                        while True:
                            bytes_purged += self._evict_key_lru()
                            item_purged += 1
                            Meters.aii(self.meters_prefix + "mcs.cache_evict_lru_size_put")
                            if bytes_purged >= bytes_to_purge and item_purged >= items_to_purge:
                                # Reached, done
                                break
                            elif len(self._hash_key) == 0:
                                # Nothing more
                                break

                        # Recheck
                        if (self._current_data_bytes.get() + len_to_add) > self._max_bytes:
                            # Failed...
                            logger.warning("Cache purge failed, len_to_add=%s, items_count=%s, data size=%s, bytes_purged=%s, item_purged=%s, bytes_to_purge=%s, items_to_purge=%s",
                                           len_to_add, len(self._hash_key),
                                           self._current_data_bytes.get(),
                                           bytes_purged, item_purged,
                                           bytes_to_purge, items_to_purge
                                           )

                            # Stats
                            Meters.aii(self.meters_prefix + "mcs.cache_purge_failed")

        # --------------------
        # MAX ITEM COUNT
        # --------------------

        if len(self._hash_context) >= self._max_item:
            # Evict
            self._evict_key_lru()
            # Stats
            Meters.aii(self.meters_prefix + "mcs.cache_evict_lru_put")

    def _safe_unhash(self, key):
        """
        Remove a key from cache (from both dict)
        :param key: Any key
        :type key: str
        """

        try:
            # Kick from both hashes
            try:
                del (self._hash_key[key])
            except KeyError:
                pass

            try:
                del (self._hash_context[key])
            except KeyError:
                pass
        except Exception as e:
            logger.warning("Exception, ex=%s", SolBase.extostr(e))
            Meters.aii(self.meters_prefix + "mcs.cache_ex")

    # ========================================
    # PUT
    # ========================================

    def put(self, key, val, ttl_ms):
        """
        Put in cache
        :param key: Any key
        :type key: str
        :param val: Any val
        :type val: bytes,str
        :param ttl_ms: Ttl in ms
        :type ttl_ms : int
        :return bool (true is cached)
        :rtype bool
        """

        try:
            if not isinstance(val, (str, bytes)):
                raise Exception("Value must be (str, bytes)")
            elif not isinstance(key, str):
                raise Exception("Key must be (str)")

            # Len of items to be added
            item_len = len(key) + len(val)

            # If item len is greater than specified threshold, do nothing
            if self._max_bytes and item_len > self._max_single_item_bytes:
                Meters.aii(self.meters_prefix + "mcs.cache_put_too_big")
                return False

            # If maxed, kick one
            self._purge_cache(item_len)

            # Key
            tu_obj = (SolBase.mscurrent() + ttl_ms, val)

            # If whenever this is already in cache, we must kick it & adjust the size
            if key in self._hash_key:
                # Get
                tu_old = self._hash_key[key]
                # Kick
                self._safe_unhash(key)
                # Notify
                self._notify_eviction(key, tu_old[1])

            # Key hash
            self._hash_key[key] = tu_obj

            # Ordered key hash
            self._hash_context[key] = tu_obj

            # Size
            self._current_data_bytes.increment(item_len)

            # Stat
            Meters.aii(self.meters_prefix + "mcs.cache_put")
            return True
        except Exception as e:
            logger.warning("Exception, ex=%s", SolBase.extostr(e))
            Meters.aii(self.meters_prefix + "mcs.cache_ex")

    # ========================================
    # GET
    # ========================================

    def get(self, key):
        """
        Get from cache.
        Can evict if ttl expired and return None.
        :param key: Any key
        :type key: str
        :return An obj or null if not in cache
        :rtype: bytes,str,None
        """

        v = self.get_raw(key)
        if v:
            return v[1]
        else:
            return None

    # ========================================
    # REMOVE
    # ========================================

    def remove(self, key):
        """
        Remove a key from cache.
        :param key: Any key
        :type key: str
        """

        if not isinstance(key, (bytes, str)):
            raise Exception("Key must be (bytes, str)")

        if key in self._hash_key:
            # Get
            tu_old = self._hash_key[key]
            # Kick
            self._safe_unhash(key)
            # Notify
            self._notify_eviction(key, tu_old[1])

    # ========================================
    # GET RAW
    # ========================================

    def get_raw(self, key):
        """
        Get from cache.
        Can evict if ttl expired and return None.
        :param key: Any key
        :type key: str
        :return tuple(ttl_ms, value as bytes or str)
        :rtype; tuple, None
        """

        try:
            # Get it
            # [0] : Expired ms
            # [1] : Object cached
            tu_obj = self._hash_key.get(key)
            if not tu_obj:
                # Stat
                Meters.aii(self.meters_prefix + "mcs.cache_get_miss")
                return None

            # Got it, check TTL
            if self._is_expired(tu_obj[0], SolBase.mscurrent()):
                # Expired => kick it & exit
                self._safe_unhash(key)
                # Notify
                self._notify_eviction(key, tu_obj[1])
                # Stat
                Meters.aii(self.meters_prefix + "mcs.cache_get_miss")
                Meters.aii(self.meters_prefix + "mcs.cache_evict_ttl_get")
                return None

            # Got a HIT : Must update it in the ordered dict : del & re-add
            del (self._hash_context[key])
            self._hash_context[key] = tu_obj

            # Return the data
            Meters.aii(self.meters_prefix + "mcs.cache_get_hit")

            return tu_obj
        except Exception as e:
            logger.warning("Exception, ex=%s", SolBase.extostr(e))
            Meters.aii(self.meters_prefix + "mcs.cache_ex")
            return None
