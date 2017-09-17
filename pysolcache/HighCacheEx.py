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
import ujson

from pysolbase.SolBase import SolBase

from pysolcache.HighCache import HighCache

logger = logging.getLogger(__name__)


class HighCacheEx(HighCache):
    """
    A High cache handling put in L1 in case of L1 MISS / L2 HIT.
    Handle underlying data storage as (ms_added, ttl_ms, data)
    For backwards comp, if _decode fails, key is removed from both caches.
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

        HighCache.__init__(self, memory_cache, redis_cache)
        logger.info("Initialize : started")

    # ========================================
    # GET
    # ========================================

    def _decode(self, buf):
        """
        Decode buffer
        :param buf: str
        :type buf: str
        :return tuple (data, ttl_ms, ms_added
        :rtype tuple
        """

        try:
            d = ujson.loads(buf)
            return d["data"], d["ttl_ms"], d["ms_added"]
        except Exception as e:
            logger.warn("Unable to decode, buf=%s, ex=%s", buf, SolBase.extostr(e))
            return None, None, None

    def _encode(self, val, ttl_ms, ms_added=None):
        """
        Encode
        :param val: str
        :type val: str
        :param ttl_ms: int
        :type ttl_ms: int
        :param ms_added: int, float, None
        :type ms_added: int, float, None
        :return:
        """

        if not ms_added:
            ms_added = SolBase.mscurrent()

        d = {
            "ms_added": int(ms_added),
            "ttl_ms": ttl_ms,
            "data": val,
        }
        return ujson.dumps(d)

    def get(self, key, l1=True, l2=True, put_l1=True):
        """
        Get from cache.
        :param key: Any key
        :type key: str, unicode
        :param l1: fetch from L1?
        :type l1: bool
        :param l2: fetch from L2?
        :type put_l1: bool
        :param put_l1: put into L1 if L1 miss and L2 hit
        :type l2: bool
        """

        v, _ = self.getex(key, l1, l2)
        return v

    def getex(self, key, l1=True, l2=True, put_l1=True):
        """
        Get from cache.
        :param key: Any key
        :type key: str, unicode
        :param l1: fetch from L1?
        :type l1: bool
        :param l2: fetch from L2?
        :type l2: bool
        :type put_l1: bool
        :param put_l1: put into L1 if L1 miss and L2 hit
        :return tuple (value or None, integer 0 for miss, 1 for L1 hit, 2 for L2 hit)
        :rtype tuple
        """

        if l1:
            v = self._memory_cache.get(key)
            if v:
                data, _, _ = self._decode(v)
                if not data:
                    self.remove(key)
                    return None, 0
                return data, 1

        if l2:
            v = self._redis_cache.get(key)
            if v:
                # Decode
                data, ttl_ms, ms_added = self._decode(v)
                if not data:
                    self.remove(key)
                    return None, 0

                # Hit L2, Miss L1
                if put_l1 and l1:
                    # Fix ttl
                    ttl_ms -= int(SolBase.mscurrent()) - ms_added
                    # If remaining ttl, put in L1
                    if ttl_ms > 0:
                        self.put(key, data, ttl_ms, l1=True, l2=False)
                        logger.info("l2 hit, l1 miss, l1.added, ttl_ms=%s, key=%s", ttl_ms, key)

                # Over
                return data, 2

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

        buf = self._encode(val, ttl_ms)

        b_l1 = False
        b_l2 = False

        if l1:
            b_l1 = self._memory_cache.put(key, buf, ttl_ms)
        if l2:
            b_l2 = self._redis_cache.put(key, buf, ttl_ms)

        return b_l1, b_l2
