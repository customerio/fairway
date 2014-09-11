package fairway

func FairwayPull() string {
	return `
local namespace = KEYS[1];
local timestamp = tonumber(KEYS[2]);
local wait = tonumber(KEYS[3]);

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

-- Multiple queues can be passed through
-- fairway_pull. We'll loop through all 
-- provided queues, and return a message
-- from the first one that isn't empty.
for i, queue in ipairs(ARGV) do
  local priorities     = k(queue, 'priorities');
  local active_facets  = k(queue, 'active_facets');
  local round_robin    = k(queue, 'facet_queue');
  local facet_pool     = k(queue, 'facet_pool');
  local inflight       = k(queue, 'inflight');
  local inflight_limit = k(queue, 'limit');

  if wait ~= -1 then
    -- Check if any current inflight messages
    -- have been inflight for a long time.
    local inflightmessage = redis.call('zrange', inflight, 0, 0, 'WITHSCORES');

    -- If we have an inflight message and it's score
    -- is less than the current pull timestamp, reset
    -- the inflight score for the the message and resend.
    if #inflightmessage > 0 then
      if tonumber(inflightmessage[2]) <= timestamp then
        redis.call('zadd', inflight, timestamp + wait, inflightmessage[1]);
        return {queue, inflightmessage[1]}
      end
    end
  end

  -- Pull a facet from the round-robin list.
  -- This list guarantees each active facet will have a
  -- message pulled from the queue every time through..
  local facet = redis.call('rpop', round_robin);

  if facet then
    -- If we found an active facet, we know the facet
    -- has at least one message available to be pulled
    -- from it's message queue.
    local messages       = k(queue, facet);
    local inflight_total = k(queue, facet .. ':inflight');

    local message = redis.call('rpop', messages);

    if message then
      if wait ~= -1 then
        redis.call('zadd', inflight, timestamp + wait, message);
        redis.call('incr', inflight_total);
      end

      redis.call('decr', k(queue, 'length'));
    end

    local length = redis.call('llen', messages);

    -- If the length of the facet's message queue
    -- is empty, then it is no longer active as
    -- it no longer has any messages.
    if length == 0 then

      -- If we aren't tracking inflight messages,
      -- remove facet form active facets. If we are
      -- tracking inflight messages, this happens
      -- when acknowledging the message.
      if wait == -1 then
        -- We remove the facet from the set of active
        -- facets and don't push the facet back on the
        -- round-robin queue.
        redis.call('srem', active_facets, facet);
      end
     
    -- If the facet still has messages to process,
    -- it remains in the active facet set, and is
    -- pushed back on the round-robin queue.
    --
    -- Additionally, the priority of the facet may
    -- have changed, so we'll check and update the
    -- current facet's priority if needed.
    else
      local priority = tonumber(redis.call('hget', priorities, facet)) or 1
      local current  = tonumber(redis.call('hget', facet_pool, facet)) or 1

      -- If the current priority is less than the
      -- desired priority, let's increase the priority
      -- by pushing the current facet on the round-robin
      -- queue twice, and incrementing the current
      -- priority.
      --
      -- Note: If there aren't enough messages left
      -- on the facet, we don't increase priority.
      if current < priority and length > current then
        redis.call('lpush', round_robin, facet);
        redis.call('lpush', round_robin, facet);
        redis.call('hset', facet_pool, facet, current + 1);
        
      -- If the current priority is greater than the
      -- desired priority, let's decrease the priority
      -- by not pushing the current facet on the round-robin
      -- queue, and decrementing the current priority.
      --
      -- Note: Also decrement priority if there aren't
      -- enough messages for the current priority. This
      -- ensures priority (entries in the round-robin queue)
      -- never exceeds the number of messages for a given
      -- facet.
      elseif current > priority or current > length then
        redis.call('hset', facet_pool, facet, current - 1);
      
      -- If we are keeping track of inflight messages
      -- check to see if we're at the max number of inflight
      -- messages, and if so, don't place the facet back
      -- on the round-robin queue (the next acknowledge will)
      elseif wait ~= -1 then
        local max = tonumber(redis.call('get', inflight_limit)) or 0

        if max > 0 then
          local current = tonumber(redis.call('get', inflight_total)) or 0

          if current < max then
            redis.call('lpush', round_robin, facet);
          end
        else
          redis.call('lpush', round_robin, facet);
        end

      -- If the current priority is equals the
      -- desired priority and we aren't at our max inflight,
      -- let's maintain the current priority by pushing
      -- the current facet on the round-robin queue once.
      else
        redis.call('lpush', round_robin, facet);
      end
    end

    return {queue, message};
  end
end

`
}
