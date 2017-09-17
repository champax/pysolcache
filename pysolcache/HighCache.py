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

from pysolcache.MemoryCache import MemoryCache
from pysolcache.RedisCache import RedisCache

logger = logging.getLogger(__name__)


class HighCache(object):
    """
    A High cache, handling L1 (MemoryCache), L2 (RedisCache)
    Provided MemoryCache and RedisCache must be ready for start/stop.
    - Keys : (str,unicode)
    - Values : (str,unicode)
    """

    def __init__(self,
                 memory_cache,
                 redis_cache):
        """
        Constructor
        :param memory_cache: MemoryCache
        :type memory_cache: MemoryCache
        :param redis_cache: RedisCache
        :type redis_cache: RedisCache
        """

        # Params
        self._memory_cache = memory_cache
        self._redis_cache = redis_cache

        if not isinstance(self._memory_cache, MemoryCache):
            raise Exception("MemoryCache required")
        elif not isinstance(self._redis_cache, RedisCache):
            raise Exception("RedisCache required")

        # Start
        self.start_cache()

        # Ok
        logger.info("Initialize : started")

    # ========================================
    # START / STOP (LOCKED)
    # ========================================

    def start_cache(self):
        """
        Start
        """

        # Safer : start if required

        # noinspection PyProtectedMember
        s = self._memory_cache._is_started
        if not s:
            logger.warn("_memory_cache : _is_started=%s, forcing start_cache now", s)
            self._memory_cache.start_cache()

        # noinspection PyProtectedMember
        s = self._redis_cache._is_started
        if not s:
            logger.warn("_redis_cache : _is_started=%s, forcing start_cache now", s)
            self._redis_cache.start_cache()

    def __del__(self):
        """
        Destructor
        """

        self.stop_cache()

    def stop_cache(self):
        """
        Stop
        """

        # noinspection PyProtectedMember
        s = self._memory_cache._is_started
        if s:
            self._memory_cache.stop_cache()

        # noinspection PyProtectedMember
        s = self._redis_cache._is_started
        if s:
            self._redis_cache.stop_cache()

    # ========================================
    # GET
    # ========================================

    def get(self, key, l1=True, l2=True):
        """
        Get from cache.
        :param key: Any key
        :type key: str, unicode
        :param l1: fetch from L1?
        :type l1: bool
        :param l2: fetch from L2?
        :type l2: bool
        """

        if l1:
            v = self._memory_cache.get(key)
            if v:
                return v

        if l2:
            v = self._redis_cache.get(key)
            if v:
                return v

        return None

    def getex(self, key, l1=True, l2=True):
        """
        Get from cache.
        :param key: Any key
        :type key: str, unicode
        :param l1: fetch from L1?
        :type l1: bool
        :param l2: fetch from L2?
        :type l2: bool
        :return tuple (value or None, integer 0 for miss, 1 for L1 hit, 2 for L2 hit)
        :rtype tuple
        """

        if l1:
            v = self._memory_cache.get(key)
            if v:
                return v, 1

        if l2:
            v = self._redis_cache.get(key)
            if v:
                return v, 2

        return None, 0

    # ========================================
    # REMOVE
    # ========================================

    def remove(self, key, l1=True, l2=True):
        """
        Remove a key from cache.
        :param key: Any key
        :type key: str, unicode
        :param l1: remove from L1?
        :type l1: bool
        :param l2: remove from L2?
        :type l2: bool
        """

        if l1:
            self._memory_cache.remove(key)

        if l2:
            self._redis_cache.remove(key)

    # ========================================
    # PUT
    # ========================================

    def put(self, key, val, ttl_ms, l1=True, l2=True):
        """
        Put in cache
        :param key: Any key
        :type key: str, unicode
        :param val: Any val
        :type val: str, unicode
        :param ttl_ms: Ttl in ms
        :type ttl_ms : int
        :param l1: put in L1?
        :type l1: bool
        :param l2: put in L2?
        :type l2: bool
        :return tuple bool,bool (true if cached in L1, true if cached in L2)
        :rtype tuple
        """

        b_l1 = False
        b_l2 = False

        if l1:
            b_l1 = self._memory_cache.put(key, val, ttl_ms)
        if l2:
            b_l2 = self._redis_cache.put(key, val, ttl_ms)

        return b_l1, b_l2
