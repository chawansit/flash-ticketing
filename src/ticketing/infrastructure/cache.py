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
# One hash holds metadata and seat fields; a Lua invocation is atomic with readers.
PUT = """
local exists = redis.call('HEXISTS',KEYS[1],'version') == 1
local dirty = redis.call('HEXISTS',KEYS[1],'updating') == 1
if ARGV[1] == 'patch' and (not exists or dirty) then return -1 end
local rows = cjson.decode(ARGV[2])
local version = tonumber(redis.call('HGET',KEYS[1],'version') or '0')
local changed = {}
if ARGV[1] == 'full' then version = 0 end
for _,seat in ipairs(rows) do
  local raw = redis.call('HGET',KEYS[1],'seat:'..seat.seat_id)
  local old = raw and cjson.decode(raw) or nil
  local newer = not old or tonumber(seat.source_version) > tonumber(old.source_version)
  local selected = newer and seat or old
  if ARGV[1] == 'full' then
    version = version + tonumber(selected.source_version)
  elseif newer then
    version = version + tonumber(seat.source_version) - (old and tonumber(old.source_version) or 0)
  end
  if newer or dirty then table.insert(changed, selected) end
end
redis.call('HSET',KEYS[1],'updating',1)
for _,seat in ipairs(changed) do
  seat.version = version
  redis.call('HSET',KEYS[1],'seat:'..seat.seat_id,cjson.encode(seat))
end
redis.call('HSET',KEYS[1],'version',version)
if ARGV[1] == 'full' then redis.call('EXPIRE',KEYS[1],30) end
redis.call('HDEL',KEYS[1],'updating')
return version
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

    @staticmethod
    def key(event):
        return f"seatmap:v2:{{{event}}}"

    def read(self, event):
        try:
            raw = self.redis.hgetall(self.key(event))
        except RedisError as exc:
            raise Failure("SEATMAP_UNAVAILABLE", 503) from exc
        if "version" not in raw or "updating" in raw:
            raise Failure("SEATMAP_WARMING", 503)
        return {
            "event_id": str(event),
            "version": int(raw["version"]),
            "seats": sorted(
                [json.loads(value) for key, value in raw.items() if key.startswith("seat:")],
                key=lambda seat: seat["seat_id"],
            ),
        }

    def put(self, event, version, data):
        # Source row versions, not aggregate snapshot order, fence racing updates.
        return self.redis.eval(PUT, 1, self.key(event), "full", json.dumps(data["seats"], default=str))

    def patch(self, event, seats):
        return self.redis.eval(PUT, 1, self.key(event), "patch", json.dumps(seats, default=str)) >= 0
