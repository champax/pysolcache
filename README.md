pysolcache
============

Welcome to pysol

Copyright (C) 2013/2025 Laurent Labatut / Laurent Champagnac

pysolcache is a set of python caches : in-memory and/or redis.

Usefull to handle L1 (memory) and L2 (redis) cache for python daemons.

In all cases, serialization/deserialization of stored datas have to be done at client side (ie, serialize as u wish, ujson or equivalent)

All caches are instrumented by Meters (pysolmeters).

MemoryCache:
- A pure python memory cache storing string/binary keys to string/binary values
- Max bytes capped
- Max items count capped
- Items TTLs
- LRU evictions
- Watchdog evictions

RedisCache:
- A redis backed cache, storing string/binary keys to string/binary values

HighCache:
- A high level cache, coupling MemoryCache adn RedisCache, which handle respectively L1 cache (in memory) and L2 cache (inside redis)

HighCacheEx:
- A high level cache, storing internal data as tuple (ms_added, ttl_ms, string/binary data)
- Provided same level of functionality as HighCache but is able to perform an automatic L1 put in case of L2 hit and L1 miss

It is gevent (co-routines) based.

