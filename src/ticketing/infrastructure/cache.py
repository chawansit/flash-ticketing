import json
from contextlib import contextmanager
from uuid import uuid4

from redis import Redis
from redis.exceptions import RedisError

from ticketing.domain import Failure

ACQUIRE = """
for i,k in ipairs(KEYS) do if redis.call('EXISTS',k)==1 then return 0 end end
for i,k in ipairs(KEYS) do redis.call('SET',k,ARGV[1],'PX',ARGV[2]) end
return 1
"""
RELEASE = """
for i,k in ipairs(KEYS) do
  if redis.call('GET',k)==ARGV[1] then redis.call('DEL',k) end
end
return 1
"""
PUT = """
local old = redis.call('GET',KEYS[1])
if old and tonumber(cjson.decode(old).version) > tonumber(ARGV[1]) then return 0 end
local incoming = cjson.decode(ARGV[2])
local previous = {}
if old then
  for _,seat in ipairs(cjson.decode(old).seats) do previous[seat.seat_id] = seat end
end
for _,seat in ipairs(incoming.seats) do
  local before = previous[seat.seat_id]
  if before and before.source_version == seat.source_version then
    seat.version = before.version
  end
end
redis.call('SET',KEYS[1],cjson.encode(incoming),'EX',30)
return 1
"""
RATE = """
local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],1) end; return n
"""


class RedisSeats:
    def __init__(self, url):
        self.redis = Redis.from_url(
            url, decode_responses=True, socket_timeout=0.1, socket_connect_timeout=0.1
        )

    @contextmanager
    def shield(self, event, seats):
        keys = [f"shield:{{{event}}}:{seat}" for seat in seats]
        token = str(uuid4())
        try:
            acquired = self.redis.eval(ACQUIRE, len(keys), *keys, token, 2000)
        except RedisError as exc:
            raise Failure("ADMISSION_UNAVAILABLE", 503) from exc
        if not acquired:
            raise Failure("SEAT_BUSY")
        try:
            yield
        finally:
            try:
                self.redis.eval(RELEASE, len(keys), *keys, token)
            except RedisError:
                pass  # A finite lease is never the ownership authority.

    def rate_limit(self, actor):
        try:
            if self.redis.eval(RATE, 1, f"rate:{actor}") > 20:
                raise Failure("RATE_LIMITED", 429)
        except RedisError as exc:
            raise Failure("ADMISSION_UNAVAILABLE", 503) from exc

    def read(self, event):
        try:
            raw = self.redis.get(f"seatmap:{event}")
        except RedisError as exc:
            raise Failure("SEATMAP_UNAVAILABLE", 503) from exc
        if not raw:
            raise Failure("SEATMAP_WARMING", 503)
        return json.loads(raw)

    def put(self, event, version, data):
        self.redis.eval(PUT, 1, f"seatmap:{event}", version, json.dumps(data, default=str))
