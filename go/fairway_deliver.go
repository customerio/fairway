package fairway

func FairwayDeliver() string {
	return `
local namespace = KEYS[1];
local topic     = ARGV[1];
local facet     = ARGV[2];
local message   = ARGV[3];

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

local registered_queues_key = namespace .. 'registered_queues';
local registered_queues     = redis.call('hgetall', registered_queues_key);

-- Determine whether or not the message should
-- be delivered to each registered queue.
for i = 1, #registered_queues, 2 do
  local queue       = registered_queues[i];
  local queue_topic = registered_queues[i+1];

  -- If the message topic matches the queue topic,
  -- we deliver the message to the queue.
  if string.find(topic, queue_topic) then
    local priorities     = k(queue, 'priorities');
    local active_facets  = k(queue, 'active_facets');
    local round_robin    = k(queue, 'facet_queue');
    local facet_pool     = k(queue, 'facet_pool');
    local inflight_total = k(queue, facet .. ':inflight');
    local inflight_limit = k(queue, 'limit');

    -- Delivering the message to a queue is as simple as
    -- pushing it onto the facet's message list, and
    -- incrementing the length of the queue itself.
    local length = redis.call('lpush', k(queue, facet), message)
    redis.call('incr', k(queue, 'length'));

    -- Manage facet queue and active facets
    local current       = tonumber(redis.call('hget', facet_pool, facet)) or 0;
    local priority      = tonumber(redis.call('hget', priorities, facet)) or 1;
    local inflight_cur  = tonumber(redis.call('get', inflight_total)) or 0;
    local inflight_max  = tonumber(redis.call('get', inflight_limit)) or 0;

    local n = 0

    -- redis.log(redis.LOG_WARNING, current.."/"..length.."/"..priority.."/"..inflight_max.."/"..inflight_cur);

    if inflight_max > 0 then
      n = math.min(length, priority, inflight_max - inflight_cur);
    else
      n = math.min(length, priority);
    end

    -- redis.log(redis.LOG_WARNING, "PUSH: "..current.."/"..n);

    if n > current then
      -- redis.log(redis.LOG_WARNING, "growing");
      redis.call('lpush', round_robin, facet);
      redis.call('hset', facet_pool, facet, current + 1);
    end

    redis.call('sadd', active_facets, facet)
  end
end

-- For any clients listening over pub/sub,
-- we should publish the message.
redis.call('publish', namespace .. topic, message);
`
}
