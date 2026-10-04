import hashlib
import json
import time
from collections import OrderedDict
from contextlib import contextmanager
from threading import Lock
from uuid import uuid4

from redis import Redis
from redis.exceptions import RedisError

from ticketing.domain import Failure
from ticketing.observability import (
    BROWSE_BODY_BYTES,
    BROWSE_BODY_ENTRIES,
    BROWSE_BODY_OUTCOMES,
    SEAT_DELTA_OUTCOMES,
    SEAT_DELTA_PHASE_SECONDS,
    SEAT_DELTA_RAW_ENTRIES,
    SEAT_DELTA_RESETS,
    SEAT_DELTA_RESULT_SEATS,
)

DELTA_HISTORY_LIMIT = 512

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
local hash_exists = redis.call('EXISTS',KEYS[1]) == 1
local exists = redis.call('HEXISTS',KEYS[1],'version') == 1
local dirty = redis.call('HEXISTS',KEYS[1],'updating') == 1
if ARGV[1] == 'patch' and (not exists or dirty) then return -1 end
local rows = cjson.decode(ARGV[2])
local payload = ARGV[11]
local position = 1
local input_version = ARGV[12]
if not input_version or not tonumber(input_version) then
  return redis.error_reply('invalid seat aggregate prediction')
end
local input_suffix = ',"version":'..input_version..'}'
local count = #rows / 2
if #rows % 2 ~= 0 then return redis.error_reply('invalid seat metadata count') end
if not payload or (count == 0 and payload ~= '') then
  return redis.error_reply('invalid seat payload framing')
end
local version = tonumber(redis.call('HGET',KEYS[1],'version') or '0')
local prior_version = version
local changed = {}
local changed_bodies = {}
local changed_old = {}
local ttl_seconds = tonumber(ARGV[6] or '30')
local rebuild = ARGV[1] == 'full' and not exists
local source_total = 0
if rebuild then version = 0 end
-- Keep command argument groups bounded while the whole invocation stays atomic.
for first=1,count,256 do
  local last = math.min(first+255,count)
  local raw_rows
  if hash_exists then
    local fields = {}
    for index=first,last do fields[index-first+1] = 'seat:'..rows[index*2-1] end
    raw_rows = redis.call('HMGET',KEYS[1],unpack(fields))
  end
  for index=first,last do
    local seat_id = rows[index*2-1]
    local separator = string.find(payload,'\\n',position,true)
    if (index < count and not separator) or (index == count and separator) then
      return redis.error_reply('invalid seat payload framing')
    end
    local body = string.sub(payload,position,separator and separator-1 or #payload)
    if string.sub(body,1,1) ~= '{' or string.sub(body,-#input_suffix) ~= input_suffix then
      return redis.error_reply('invalid seat payload object')
    end
    position = separator and separator+1 or #payload+1
    local raw = raw_rows and raw_rows[index-first+1]
    local old = raw and cjson.decode(raw) or nil
    local source_version = tonumber(rows[index*2])
    local newer = not old or source_version > tonumber(old.source_version)
    local selected_source = newer and source_version or tonumber(old.source_version)
    if ARGV[1] == 'full' then
      source_total = source_total + selected_source
    end
    if rebuild then
      version = version + selected_source
    elseif newer then
      version = version + source_version - (old and tonumber(old.source_version) or 0)
    end
    if rebuild or newer or dirty then
      local selected = #changed+1
      changed[selected] = seat_id
      changed_bodies[selected] = newer and body or false
      if not newer then changed_old[selected] = old end
    end
  end
end
if ARGV[1] == 'full' and not rebuild and source_total > version then
  version = source_total
end
if ARGV[1] == 'full' and not exists then redis.call('DEL',KEYS[2]) end
redis.call('HSET',KEYS[1],'updating',1)
local encoded_version = cjson.encode(version)
local publish_delta = #changed > 0 and version > prior_version
local delta_seats = {}
for first=1,#changed,256 do
  local fields = {}
  for index=first,math.min(first+255,#changed) do
    local body = changed_bodies[index]
    local encoded
    if body then
      if encoded_version == input_version then
        encoded = body
      else
        encoded = string.sub(body,1,#body-#input_suffix)..',"version":'..encoded_version..'}'
      end
    else
      local old = changed_old[index]
      old.version = version
      encoded = cjson.encode(old)
    end
    local offset = (index-first)*2
    fields[offset+1] = 'seat:'..changed[index]
    fields[offset+2] = encoded
    if publish_delta then delta_seats[#delta_seats+1] = encoded end
  end
  redis.call('HSET',KEYS[1],unpack(fields))
end
redis.call('HSET',KEYS[1],'version',version)
if ARGV[1] == 'full' then
  redis.call('HSET',KEYS[1],'layout',ARGV[3],'layout_etag',ARGV[4])
  redis.call('HSETNX',KEYS[1],'incarnation',ARGV[5])
  if ARGV[7] and ARGV[7] ~= '' then
    redis.call('HSET',KEYS[1],'sale_starts_epoch',ARGV[7],
      'sale_ends_epoch',ARGV[8],'currency',ARGV[9])
  end
end
redis.call('HDEL',KEYS[1],'updating')
if publish_delta then
  local header = cjson.encode({from_version=prior_version,version=version})
  local entry = string.sub(header,1,-2)..',"seats":['..table.concat(delta_seats,',')..']}'
  redis.call('ZADD',KEYS[2],version,entry)
  local map_ttl = redis.call('TTL',KEYS[1])
  local delta_ttl = redis.call('TTL',KEYS[2])
  if map_ttl > 0 and (delta_ttl < 0 or delta_ttl > map_ttl) then
    redis.call('EXPIRE',KEYS[2],map_ttl)
  end
  local excess = redis.call('ZCARD',KEYS[2]) - tonumber(ARGV[10] or '512')
  if excess > 0 then redis.call('ZREMRANGEBYRANK',KEYS[2],0,excess-1) end
end
if ARGV[1] == 'full' then
  redis.call('EXPIRE',KEYS[1],ttl_seconds)
  if redis.call('EXISTS',KEYS[2]) == 1 then redis.call('EXPIRE',KEYS[2],ttl_seconds) end
end
return version
"""
BROWSE = """
local meta = redis.call('HMGET',KEYS[1],'version','incarnation','updating','layout_etag')
if not meta[1] or not meta[2] or meta[3] or not meta[4] then return {503} end
local tag = ARGV[1] == 'layout' and meta[4] or ('"availability:'..ARGV[2]..':'..meta[2]..':'..meta[1]..'"')
for _,candidate in ipairs(cjson.decode(ARGV[3])) do
  if candidate == '*' or candidate == tag then
    redis.call('EXPIRE',KEYS[1],tonumber(ARGV[4]))
    return {304,tag}
  end
end
if ARGV[1] == 'layout' then
  local layout = redis.call('HGET',KEYS[1],'layout')
  if not layout then return {503} end
  redis.call('EXPIRE',KEYS[1],tonumber(ARGV[4]))
  return {200,tag,layout}
end
local result = {200,tag,meta[1],meta[2]}
local fields = redis.call('HGETALL',KEYS[1])
for i=1,#fields,2 do
  if string.sub(fields[i],1,5) == 'seat:' then table.insert(result,fields[i+1]) end
end
redis.call('EXPIRE',KEYS[1],tonumber(ARGV[4]))
return result
"""
DELTAS = """
local meta = redis.call('HMGET',KEYS[1],'version','updating','incarnation')
if not meta[1] or meta[2] or not meta[3] then return {503} end
local version = tonumber(meta[1])
local since = tonumber(ARGV[1])
if not since or since < 0 then return {422,version} end
if ARGV[3] == '' or ARGV[3] ~= meta[3] then
  return {409,version,'incarnation_mismatch'}
end
if since > version then return {409,version,'ahead'} end
redis.call('EXPIRE',KEYS[1],tonumber(ARGV[2]))
if redis.call('EXISTS',KEYS[2]) == 1 then redis.call('EXPIRE',KEYS[2],tonumber(ARGV[2])) end
if since == version then return {200,version,meta[3]} end
local entries = redis.call('ZRANGEBYSCORE',KEYS[2],'('..since,'+inf')
if #entries == 0 then return {409,version,'missing_history'} end
local expected = since
local result = {200,version,meta[3]}
for _,raw in ipairs(entries) do
  local entry = cjson.decode(raw)
  local entry_from = tonumber(entry.from_version)
  if entry_from < expected then return {409,version,'history_overlap'} end
  if entry_from > expected then return {409,version,'history_missing'} end
  expected = tonumber(entry.version)
  table.insert(result,raw)
end
if expected ~= version then return {409,version,'tail_gap'} end
return result
"""
RATE = """
local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],1) end; return n
"""
TOUCH = """
local key = KEYS[1]
local meta = redis.call('HMGET',key,'version','updating')
if not meta[1] or meta[2] then
  return 0
end
redis.call('PEXPIRE', key, tonumber(ARGV[1]))
return 1
"""


class RedisSeats:
    def __init__(
        self,
        url,
        *,
        browse_entries=2048,
        browse_bytes=32 * 1024 * 1024,
        seatmap_ttl_seconds=120,
        delta_history_entries=DELTA_HISTORY_LIMIT,
    ):
        if browse_entries < 0 or browse_bytes < 0:
            raise ValueError("Browse cache limits must be nonnegative")
        if seatmap_ttl_seconds <= 0:
            raise ValueError("Seatmap TTL must be positive")
        if delta_history_entries <= 0:
            raise ValueError("Delta history limit must be positive")
        self._browse_entries = browse_entries
        self._browse_bytes = browse_bytes
        self._seatmap_ttl_seconds = seatmap_ttl_seconds
        self._seatmap_ttl_ms = seatmap_ttl_seconds * 1000
        self.delta_history_entries = delta_history_entries
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

    @staticmethod
    def delta_key(event):
        return f"seatdelta:v1:{{{event}}}"

    def read(self, event):
        try:
            raw = self.redis.hgetall(self.key(event))
        except RedisError as exc:
            raise Failure("SEATMAP_UNAVAILABLE", 503) from exc
        if "version" not in raw or "updating" in raw:
            raise Failure("SEATMAP_WARMING", 503)
        self._touch(event)
        return {
            "event_id": str(event),
            "version": int(raw["version"]),
            "incarnation": raw["incarnation"],
            "seats": sorted(
                [json.loads(value) for key, value in raw.items() if key.startswith("seat:")],
                key=lambda seat: seat["seat_id"],
            ),
        }

    @staticmethod
    def _encoded_seats(seats):
        # Compact JSON escapes embedded newlines, leaving unambiguous frame separators.
        seats = list(seats)
        encode = json.JSONEncoder(default=str, separators=(",", ":")).encode
        metadata = encode([value for seat in seats for value in (seat["seat_id"], seat["source_version"])])
        prediction = sum(seat["source_version"] for seat in seats)
        payload = "\n".join(
            encode({**{key: value for key, value in seat.items() if key != "version"}, "version": prediction})
            for seat in seats
        )
        return metadata, payload, str(prediction)

    def _patch_arguments(self, event, seats):
        metadata, payload, prediction = self._encoded_seats(seats)
        return (
            self.key(event), self.delta_key(event), "patch", metadata,
            "", "", "", str(self._seatmap_ttl_seconds),
            "", "", "", str(self.delta_history_entries), payload, prediction,
        )

    def _full_arguments(self, event, data):
        metadata, payload, prediction = self._encoded_seats(data["seats"])
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
        return (
            self.key(event),
            self.delta_key(event),
            "full",
            metadata,
            layout,
            tag,
            str(uuid4()),
            str(self._seatmap_ttl_seconds),
            str(data.get("sale_starts_epoch", "")),
            str(data.get("sale_ends_epoch", "")),
            data.get("currency", ""),
            str(self.delta_history_entries),
            payload,
            prediction,
        )

    def put(self, event, version, data):
        # Source row versions, not aggregate snapshot order, fence racing updates.
        return self.redis.eval(PUT, 2, *self._full_arguments(event, data))

    def put_many(self, updates):
        updates = list(updates)
        if not updates:
            return []
        pipeline = self.redis.pipeline(transaction=False)
        for event, _version, data in updates:
            pipeline.eval(PUT, 2, *self._full_arguments(event, data))
        results = pipeline.execute(raise_on_error=False)
        return [
            result if isinstance(result, RedisError) else int(result) >= 0
            for result in results
        ]

    def patch(self, event, seats):
        return self.redis.eval(PUT, 2, *self._patch_arguments(event, seats)) >= 0

    def patch_many(self, updates):
        updates = list(updates)
        if not updates:
            return []
        pipeline = self.redis.pipeline(transaction=False)
        for event, seats in updates:
            pipeline.eval(PUT, 2, *self._patch_arguments(event, seats))
        results = pipeline.execute(raise_on_error=False)
        for result in results:
            if isinstance(result, RedisError):
                raise result
        return [int(result) >= 0 for result in results]

    def deltas(self, event, since, incarnation=None):
        redis_started = time.monotonic()
        try:
            result = self.redis.eval(
                DELTAS,
                2,
                self.key(event),
                self.delta_key(event),
                str(since),
                str(self._seatmap_ttl_seconds),
                incarnation or "",
            )
        except RedisError as exc:
            raise Failure("SEATMAP_UNAVAILABLE", 503) from exc
        finally:
            SEAT_DELTA_PHASE_SECONDS.labels("redis").observe(
                time.monotonic() - redis_started
            )
        status, version = int(result[0]), int(result[1]) if len(result) > 1 else None
        if status == 503:
            raise Failure("SEATMAP_WARMING", 503)
        if status == 422:
            raise Failure("INVALID_VERSION", 422)
        if status == 409:
            reason = result[2] if len(result) > 2 else "unknown"
            SEAT_DELTA_OUTCOMES.labels("reset").inc()
            SEAT_DELTA_RESETS.labels(reason).inc()
            snapshot = self.read(event)
            return {
                "event_id": str(event),
                "from_version": since,
                "version": snapshot["version"],
                "incarnation": snapshot["incarnation"],
                "reset_required": True,
                "seats": [
                    {key: seat[key] for key in ("seat_id", "status", "reserved_until")}
                    for seat in snapshot["seats"]
                ],
            }
        incarnation = result[2]
        SEAT_DELTA_RAW_ENTRIES.observe(max(0, len(result) - 3))
        collapse_started = time.monotonic()
        latest = {}
        for raw in result[3:]:
            for seat in json.loads(raw)["seats"]:
                latest[seat["seat_id"]] = {
                    key: seat[key] for key in ("seat_id", "status", "reserved_until")
                }
        SEAT_DELTA_PHASE_SECONDS.labels("collapse").observe(
            time.monotonic() - collapse_started
        )
        SEAT_DELTA_RESULT_SEATS.observe(len(latest))
        SEAT_DELTA_OUTCOMES.labels("delta" if latest else "empty").inc()
        return {
            "event_id": str(event),
            "from_version": since,
            "version": version,
            "incarnation": incarnation,
            "reset_required": False,
            "seats": sorted(latest.values(), key=lambda seat: seat["seat_id"]),
        }

    def browse(self, event, kind, if_none_match=None):
        if kind not in {"layout", "availability"}:
            raise ValueError("Unknown browse representation")
        tags = [part.strip().removeprefix("W/") for part in (if_none_match or "").split(",")]
        try:
            result = self.redis.eval(
                BROWSE,
                1,
                self.key(event),
                kind,
                str(event),
                json.dumps(tags),
                str(self._seatmap_ttl_seconds),
            )
        except RedisError as exc:
            raise Failure("SEATMAP_UNAVAILABLE", 503) from exc
        if int(result[0]) == 503:
            raise Failure("SEATMAP_WARMING", 503)
        status, tag = int(result[0]), result[1]
        if status == 304:
            return status, tag, None
        if kind == "layout":
            return status, tag, json.loads(result[2])
        seats = [json.loads(raw) for raw in result[4:]]
        return (
            status,
            tag,
            {
                "event_id": str(event),
                "version": int(result[2]),
                "incarnation": result[3],
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

    def _touch(self, event):
        try:
            self.redis.eval(TOUCH, 1, self.key(event), str(self._seatmap_ttl_ms))
        except RedisError:
            pass
