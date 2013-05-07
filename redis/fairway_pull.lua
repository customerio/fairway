local namespace = KEYS[1];

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

-- Multiple queues can be passed through
-- fairway_pull. We'll loop through all 
-- provided queues, and return a message
-- from the first one that isn't empty.
for i, queue in ipairs(ARGV) do
  local priorities    = k(queue, 'priorities');
  local active_facets = k(queue, 'active_facets');
  local round_robin   = k(queue, 'facet_queue');
  local facet_pool    = k(queue, 'facet_pool');

  -- Pull a facet from the round-robin list.
  -- This list guarantees each active facet will have a
  -- message pulled from the queue every time through..
  local facet = redis.call('rpop', round_robin);

  if facet then
    -- If we found an active facet, we know the facet
    -- has at least one message available to be pulled
    -- from it's message queue.
    local messages = k(queue, facet);
    local message  = redis.call('rpop', messages);

    if message then
      redis.call('decr', k(queue, 'length'));
    end

    local length = redis.call('llen', messages);

    -- If the length of the facet's message queue
    -- is empty, then it is no longer active as
    -- it no longer has any messages.
    if length == 0 then
      -- We remove the facet from the set of active
      -- facets and don't push the facet back on the
      -- round-robin queue.
      redis.call('srem', active_facets, facet);
     
    -- If the facet still has messages to process,
    -- it remains in the active facet set, and is
    -- pushed back on the round-robin queue.
    --
    -- Additionally, the priority of the facet may
    -- have changed, so we'll check and update the
    -- current facet's priority if needed.
    else
      local priority = tonumber(redis.call('hget', priorities, facet)) or 1
      local current  = tonumber(redis.call('hget', facet_pool, facet))

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
      
      -- If the current priority is equals the
      -- desired priority, let's maintain the current priority
      -- by pushing the current facet on the round-robin
      -- queue once.
      else
        redis.call('lpush', round_robin, facet);
      end
    end

    return {queue, message};
  end
end
