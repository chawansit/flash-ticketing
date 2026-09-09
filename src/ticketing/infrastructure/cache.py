import hashlib
import json
from collections import OrderedDict
from contextlib import contextmanager
from threading import Lock
from uuid import uuid4

from redis import Redis
from redis.exceptions import RedisError

from ticketing.domain import Failure
from ticketing.observability import BROWSE_BODY_BYTES, BROWSE_BODY_ENTRIES, BROWSE_BODY_OUTCOMES

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
if ARGV[1] == 'full' then
  redis.call('HSET',KEYS[1],'layout',ARGV[3],'layout_etag',ARGV[4])
  redis.call('HSETNX',KEYS[1],'incarnation',ARGV[5])
  redis.call('EXPIRE',KEYS[1],30)
end
redis.call('HDEL',KEYS[1],'updating')
return version
"""
BROWSE = """
local meta = redis.call('HMGET',KEYS[1],'version','incarnation','updating','layout_etag')
if not meta[1] or not meta[2] or meta[3] or not meta[4] then return {503} end
local tag = ARGV[1] == 'layout' and meta[4] or ('"availability:'..ARGV[2]..':'..meta[2]..':'..meta[1]..'"')
for _,candidate in ipairs(cjson.decode(ARGV[3])) do
  if candidate == '*' or candidate == tag then return {304,tag} end
end
if ARGV[1] == 'layout' then
  local layout = redis.call('HGET',KEYS[1],'layout')
  if not layout then return {503} end
  return {200,tag,layout}
end
local result = {200,tag,meta[1]}
local fields = redis.call('HGETALL',KEYS[1])
for i=1,#fields,2 do
  if string.sub(fields[i],1,5) == 'seat:' then table.insert(result,fields[i+1]) end
end
return result
"""
RATE = """
local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],1) end; return n
"""


class RedisSeats:
    def __init__(self, url, *, browse_entries=2048, browse_bytes=32 * 1024 * 1024):
        if browse_entries < 0 or browse_bytes < 0:
            raise ValueError("Browse cache limits must be nonnegative")
        self._browse_entries = browse_entries
        self._browse_bytes = browse_bytes
        self._bodies = OrderedDict()
        self._body_bytes = 0
        self._body_lock = Lock()
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
        layout = json.dumps(
            {
                "event_id": str(event),
                "seats": [
                    {"seat_id": seat["seat_id"], "price": seat["price"]}
                    for seat in sorted(data["seats"], key=lambda seat: seat["seat_id"])
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        tag = '"layout:' + hashlib.sha256(layout.encode()).hexdigest() + '"'
        return self.redis.eval(
            PUT, 1, self.key(event), "full", json.dumps(data["seats"], default=str), layout, tag, str(uuid4())
        )

    def patch(self, event, seats):
        return self.redis.eval(PUT, 1, self.key(event), "patch", json.dumps(seats, default=str)) >= 0

    def browse(self, event, kind, if_none_match=None):
        if kind not in {"layout", "availability"}:
            raise ValueError("Unknown browse representation")
        tags = [part.strip().removeprefix("W/") for part in (if_none_match or "").split(",")]
        try:
            result = self.redis.eval(BROWSE, 1, self.key(event), kind, str(event), json.dumps(tags))
        except RedisError as exc:
            raise Failure("SEATMAP_UNAVAILABLE", 503) from exc
        if int(result[0]) == 503:
            raise Failure("SEATMAP_WARMING", 503)
        status, tag = int(result[0]), result[1]
        if status == 304:
            return status, tag, None
        if kind == "layout":
            return status, tag, json.loads(result[2])
        seats = [json.loads(raw) for raw in result[3:]]
        return (
            status,
            tag,
            {
                "event_id": str(event),
                "version": int(result[2]),
                "seats": [
                    {key: seat[key] for key in ("seat_id", "status", "reserved_until")}
                    for seat in sorted(seats, key=lambda seat: seat["seat_id"])
                ],
            },
        )


    def browse_encoded(self, event, kind, if_none_match=None):
        """Reuse immutable JSON only after the atomic Redis validator confirms it."""
        key = (str(event), kind)
        with self._body_lock:
            cached = self._bodies.get(key)
            if cached is not None:
                self._bodies.move_to_end(key)
        validators = if_none_match or ""
        if cached is not None:
            validators += "," + cached[0]
        status, tag, body = self.browse(event, kind, validators)
        if status == 304:
            client_tags = [part.strip().removeprefix("W/") for part in (if_none_match or "").split(",")]
            if "*" in client_tags or tag in client_tags:
                BROWSE_BODY_OUTCOMES.labels("client_304").inc()
                return 304, tag, None
            # This reference remains valid even if another request evicts the entry.
            assert cached is not None and cached[0] == tag
            BROWSE_BODY_OUTCOMES.labels("reuse").inc()
            return 200, tag, cached[1]
        encoded = json.dumps(body, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
        BROWSE_BODY_OUTCOMES.labels("serialize").inc()
        with self._body_lock:
            previous = self._bodies.pop(key, None)
            if previous is not None:
                self._body_bytes -= len(previous[1])
            if self._browse_entries and len(encoded) <= self._browse_bytes:
                self._bodies[key] = (tag, encoded)
                self._body_bytes += len(encoded)
                while len(self._bodies) > self._browse_entries or self._body_bytes > self._browse_bytes:
                    _, removed = self._bodies.popitem(last=False)
                    self._body_bytes -= len(removed[1])
            BROWSE_BODY_BYTES.set(self._body_bytes)
            BROWSE_BODY_ENTRIES.set(len(self._bodies))
        return 200, tag, encoded
