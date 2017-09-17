pysolcache
============

Welcome to pysol

Copyright (C) 2013/2017 Laurent Labatut / Laurent Champagnac

pysolcache is a set of python caches, memory, redis or both.

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

Usage
===============

MemoryCache

```
c = MemoryCache()

# Put, ttl 60000 ms
c.put("keyA", "valA", 60000)

# Get
v = c.get("keyA")

# Remove
v = c.remove("keyA")

# Stop
c.stop_cache()
```

RedisCache

```
c = RedisCache()

# Put, ttl 60000 ms
c.put("keyA", "valA", 60000)

# Get
v = c.get("keyA")

# Remove
v = c.remove("keyA")

# Stop
c.stop_cache()
```

HighCache / HighCacheEx

```
mc = MemoryCache()
rc = RedisCache()
hc = HighCache(mc, rc)

# Put, ttl 60000 ms, in L1 and L2
hc.put("keyA", "valA", 60000, l1=True, l2=True)

# Get from L1 only
v = c.get("keyA", l1=True, l2=False)

# Get from L2 only
v = c.get("keyA", l1=False, l2=True)

# Try get from L1, if miss, try from L2
v = c.get("keyA", l1=True, l2=True)

# Remove from L1
v = c.remove("keyA", l1=True, l2=False)

# Stop
hc.stop_cache()
```

Source code
===============

- We are pep8 compliant (as far as we can, with some exemptions)
- We use a right margin of 360 characters (please don't talk me about 80 chars)
- All unittest files must begin with `test_` or `Test`, should implement setUp and tearDown methods
- All tests must adapt to any running directory
- The whole project is backed by gevent (http://www.gevent.org/)
- We use docstring (:return, :rtype, :param, :type etc..), they are mandatory
- We use PyCharm "noinspection", feel free to use them

Requirements
===============

- Debian 8 Jessie or greater, x64, Python 2.7

Unittests
===============

To run unittests, you will need:

- redis-server installed and serving 6379 without creds.

License
===============

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA


