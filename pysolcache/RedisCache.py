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
from threading import Lock

import redis
from pysolbase.SolBase import SolBase
from pysolmeters.Meters import Meters

logger = logging.getLogger(__name__)


class RedisCache(object):
    """
    A redis backed cache supporting :
    - Keys : str
    - Values : bytes
    """

    READ_DEFAULT_D = {"host": "localhost", "port": 6379, "db": 0, "max_connections": None}
    WRITE_DEFAULT_D = READ_DEFAULT_D

    # noinspection PyDefaultArgument
    def __init__(self,
                 pool_read_d=READ_DEFAULT_D,
                 pool_write_d=WRITE_DEFAULT_D,
                 max_single_item_bytes=1 * 1024 * 1024,
                 redis_read_client=None,
                 redis_write_client=None,
                 meters_prefix=""):

        """
        Constructor
        :param pool_read_d: read pool parameters (DEPRECATED, PREFER using redis_read_client)
        :type pool_read_d: dict,None
        :param pool_write_d: read pool parameters (DEPRECATED, PREFER using redis_write_client)
        :type pool_write_d: dict,None
        :param max_single_item_bytes: max single item bytes (if greater : no cache)
        :type max_single_item_bytes: int
        :param redis_read_client: Redis read client. If set it has highest priority. Otherwise client will be allocated using pool_read_d.
        :type redis_read_client: redis.StrictRedis,None
        :param redis_write_client: Redis write client. If set it has highest priority. Otherwise client will be allocated using pool_write_d.
        :type redis_write_client: redis.StrictRedis,None
        :param meters_prefix: str
        :type meters_prefix: str
        """

        # Params
        self._max_single_item_bytes = max_single_item_bytes

        # Meters
        self.meters_prefix = meters_prefix

        # Store dict
        self._pool_read_d = pool_read_d
        self._pool_write_d = pool_write_d

        # Initialize
        self._read_pool = None
        self._read_redis = None

        self._write_pool = None
        self._write_redis = None

        # External client
        self.redis_read_client = redis_read_client
        self.redis_write_client = redis_write_client

        # Must have external OR internal
        if self.redis_read_client is None and self._pool_read_d is None:
            raise Exception("No redis provided (redis_read_client and _pool_read_d None")
        elif self.redis_write_client is None and self._pool_write_d is None:
            raise Exception("No redis provided (redis_write_client and _pool_write_d None")

        # State
        self._is_started = False
        self._run_lock = Lock()

        # Logs
        logger.info("Initialize, _max_single_item_bytes=%s", self._max_single_item_bytes)
        logger.info("Initialize, _pool_read_d=%s", self._pool_read_d)
        logger.info("Initialize, _pool_write_d=%s", self._pool_write_d)

        # Start
        self.start_cache()

        # Ok
        logger.info("Initialize : started")

    def __str__(self):
        """
        Str override
        :return str
        :rtype: str
        """

        return "id={0}*put/bypass/hit/miss={1}/{2}/{3}/{4}*ex={5}".format(
            id(self),
            Meters.aig(self.meters_prefix + "rcs.cache_put"),
            Meters.aig(self.meters_prefix + "rcs.cache_put_too_big"),
            Meters.aig(self.meters_prefix + "rcs.cache_get_hit"),
            Meters.aig(self.meters_prefix + "rcs.cache_get_miss"),
            Meters.aig(self.meters_prefix + "rcs.cache_ex"),
        )

    # ========================================
    # START / STOP (LOCKED)
    # ========================================

    def start_cache(self):
        """
        Start
        """

        with self._run_lock:
            if self._is_started:
                logger.warning("_is_started=%s, doing nothing", self._is_started)
                return

            # Initialize pools now
            if self.redis_read_client is None:
                logger.info("Initialize read redis now (alloc), _pool_read_d=%s", self._pool_read_d)
                self._read_pool, self._read_redis = self._redis_open(self._pool_read_d)
            else:
                # No pool, use external
                logger.info("Initialize read redis now (external), redis_read_client=%s", self.redis_read_client)
                self._read_pool = None
                self._read_redis = self.redis_read_client

            if self.redis_write_client is None:
                logger.info("Initialize write redis now (alloc), _pool_write_d=%s", self._pool_write_d)
                self._write_pool, self._write_redis = self._redis_open(self._pool_write_d)
            else:
                # No pool, use external
                logger.info("Initialize write redis now (external), redis_write_client=%s", self.redis_write_client)
                self._write_pool = None
                self._write_redis = self.redis_write_client

            self._is_started = True

    def __del__(self):
        """
        Destructor
        """

        if self._is_started:
            self.stop_cache()

    def stop_cache(self):
        """
        Stop
        """

        with self._run_lock:
            if not self._is_started:
                return

            # We close only if _read_pool is allocated, otherwise redis_read_client has been provided externally and we dont touch it
            if self._read_pool:
                self._redis_close(self._read_pool, self._read_redis)
                self._read_pool = None
                self._read_redis = None
            else:
                self._read_redis = None
                self.redis_read_client = None

            # We close only if _write_pool is allocated, otherwise redis_read_client has been provided externally and we dont touch it
            if self._write_pool:
                self._redis_close(self._write_pool, self._write_redis)
                self._write_pool = None
                self._write_redis = None
            else:
                self._write_redis = None
                self.redis_write_client = None

            self._is_started = False

    # ========================================
    # OPEN / CLOSE
    # ========================================

    @classmethod
    def _redis_close(cls, pool, redis_instance):
        """
        Close redis
        :param pool: redis.ConnectionPool
        :type: redis.ConnectionPool
        :param redis_instance: redis.Redis
        :type redis_instance: redis.Redis
        """

        if redis_instance:
            del redis_instance

        if pool:
            pool.disconnect()
            del pool

    @classmethod
    def _redis_open(cls, d_param):
        """
        Open a redis instance
        :param d_param: dict
        :type d_param: dict
        :return tuple pool, redis_instance
        :rtype tuple
        """

        # Pool
        try:
            pool = redis.ConnectionPool(
                host=d_param["host"],
                port=d_param["port"],
                db=d_param["db"],
                max_connections=d_param["max_connections"]
            )
            logger.info("Initialized pool=%s, d_param=%s", pool, d_param)

            # Redis
            redis_instance = redis.Redis(connection_pool=pool)
            logger.info("Initialized redis_instance=%s, d_param=%s", redis_instance, d_param)

            return pool, redis_instance
        except Exception as e:
            logger.error("Exception, ex=%s", SolBase.extostr(e))
            raise

    # ========================================
    # GET
    # ========================================

    def get(self, key):
        """
        Get from cache.
        :param key: Any key
        :type key: str
        :return An obj or null if not in cache
        :rtype bytes, None
        """

        ms_start = SolBase.mscurrent()
        try:
            if not isinstance(key, (bytes, str)):
                raise Exception("Key must be (bytes, str)")

            # Use read redis
            v = self._read_redis.get(key)
            if v:
                Meters.aii(self.meters_prefix + "rcs.cache_get_hit")
            else:
                Meters.aii(self.meters_prefix + "rcs.cache_get_miss")
            return v
        except Exception as e:
            logger.warning("Exception, ex=%s", SolBase.extostr(e))
            Meters.aii(self.meters_prefix + "rcs.cache_ex")
            return None
        finally:
            Meters.dtci(self.meters_prefix + "rcs.cache_dtc_read", SolBase.msdiff(ms_start))

    # ========================================
    # REMOVE
    # ========================================

    def remove(self, key):
        """
        Remove a key from cache.
        :param key: Any key
        :type key: str
        """

        ms_start = SolBase.mscurrent()
        try:
            if not isinstance(key, (bytes, str)):
                raise Exception("Key must be (bytes, str)")

            # Use write redis
            self._write_redis.delete(key)

        except Exception as e:
            logger.warning("Exception, ex=%s", SolBase.extostr(e))
            Meters.aii(self.meters_prefix + "rcs.cache_ex")
        finally:
            Meters.dtci(self.meters_prefix + "rcs.cache_dtc_write", SolBase.msdiff(ms_start))

    # ========================================
    # PUT
    # ========================================

    def put(self, key, val, ttl_ms):
        """
        Put in cache
        :param key: Any key
        :type key: str
        :param val: Any val
        :type val: bytes
        :param ttl_ms: Ttl in ms
        :type ttl_ms : int
        :return bool (true is cached)
        :rtype bool
        """

        try:
            if not isinstance(val, bytes):
                raise Exception("Value must be bytes")
            elif not isinstance(key, (bytes, str)):
                raise Exception("Key must be (bytes, str)")

            # Len of items to be added
            item_len = len(val)

            # If item len is greater than specified threshold, do nothing
            if item_len > self._max_single_item_bytes:
                Meters.aii(self.meters_prefix + "rcs.cache_put_too_big")
                return False

            # Redis use second
            ttl_sec = int(ttl_ms / 1000.0)

            # Use write redis
            self._write_redis.setex(
                name=key,
                value=val,
                time=ttl_sec)

            # Stat
            Meters.aii(self.meters_prefix + "rcs.cache_put")
            return True
        except Exception as e:
            logger.warning("Exception, ex=%s", SolBase.extostr(e))
            Meters.aii(self.meters_prefix + "rcs.cache_ex")
            return False
