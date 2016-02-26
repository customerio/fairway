package fairway

func FairwayAck() string {
	return `
local namespace = KEYS[1];

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

local queue   = ARGV[1];
local facet   = ARGV[2];
local message = ARGV[3];

local active_facets  = k(queue, 'active_facets');
local round_robin    = k(queue, 'facet_queue');
local facet_pool     = k(queue, 'facet_pool');
local inflight       = k(queue, 'inflight');
local messages       = k(queue, facet);
local inflight_facet = k(queue, facet .. ':inflight');
local inflight_limit = k(queue, 'limit');
local priorities     = k(queue, 'priorities');

local removed = tonumber(redis.call('zrem', inflight, message));
redis.call('srem', inflight_facet, message);

if removed > 0 then
  -- Manage facet queue and active facets
  local current       = tonumber(redis.call('hget', facet_pool, facet)) or 0;
  local priority      = tonumber(redis.call('hget', priorities, facet)) or 1;
  local length        = redis.call('llen', messages);
  local inflight_cur  = tonumber(redis.call('scard', inflight_facet)) or 0;
  local inflight_max  = tonumber(redis.call('get', inflight_limit)) or 0;

  local n = 0

  -- redis.log(redis.LOG_WARNING, current.."/"..length.."/"..priority.."/"..inflight_max.."/"..inflight_cur)

  if inflight_max > 0 then
    n = math.min(length, priority, inflight_max - inflight_cur);
  else
    n = math.min(length, priority);
  end

  -- redis.log(redis.LOG_WARNING, "ACK: "..current.."/"..n);

  if n > current then
    -- redis.log(redis.LOG_WARNING, "growing");
    redis.call('lpush', round_robin, facet);
    redis.call('hset', facet_pool, facet, current + 1);
  end

  if (length == 0 and inflight_cur == 0 and n == 0) then
    redis.call('del', inflight_facet);
    redis.call('hdel', facet_pool, facet);
    redis.call('srem', active_facets, facet);
  end
end

return removed
`
}
