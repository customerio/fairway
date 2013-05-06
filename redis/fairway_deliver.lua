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
    local set_priorities = k(queue, 'priorities');
    local active_facets  = k(queue, 'active_facets');
    local round_robin    = k(queue, 'facet_queue');

    -- Delivering the message to a queue is as simple as
    -- pushing it onto the facet's message list, and
    -- incrementing the length of the queue itself.
    redis.call('lpush', k(queue, facet), message)
    redis.call('incr', k(queue, 'length'));

    -- If the facet just became active, we need to add
    -- the facet to the round-robin queue, so it's
    -- messages will be processed.
    if redis.call('sadd', active_facets, facet) == 1 then
      local priority = tonumber(redis.call('hget', set_priorities, facet)) or 1

      -- If the facet currently has a priority of 0,
      -- then we shouldn't process any of the facet's
      -- messages. Don't push it on the round-robin queue.
      if priority > 0 then
        redis.call('lpush', round_robin, facet);
      end
    end
  end
end

-- For any clients listening over pub/sub,
-- we should publish the message.
redis.call('publish', namespace .. topic, message);
