package fairway

func FairwayPull() string {
	return `
local namespace = KEYS[1];
local timestamp = tonumber(KEYS[2]);
local n = tonumber(KEYS[3]);
local wait = tonumber(KEYS[4]);

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

local pull = function (queue, n)
  local round_robin    = k(queue, 'facet_queue');
  local inflight       = k(queue, 'inflight');

  -- Pull a facet from the round-robin list.
  -- This list guarantees each active facet will have a
  -- message pulled from the queue every time through..
  local facet = redis.call('rpop', round_robin);
  local msgs = {};

  if facet then
    -- If we found an active facet, we know the facet
    -- has at least one message available to be pulled
    -- from it's message queue.
    local messages       = k(queue, facet);
    local inflight_facet = k(queue, facet .. ':inflight');

    for i = 1, n, 1 do
      local msg = redis.call('rpop', messages);

      if msg then
        if wait ~= -1 then
          redis.call('zadd', inflight, timestamp + wait, msg);
          redis.call('sadd', inflight_facet, msg);
        end

        table.insert(msgs, msg);
      end
    end

    redis.call('decrby', k(queue, 'length'), #msgs);
  end

  return {facet, msgs};
end

local manage = function (queue, facet)
  local active_facets  = k(queue, 'active_facets');
  local round_robin    = k(queue, 'facet_queue');
  local inflight_limit = k(queue, 'limit');
  local priorities     = k(queue, 'priorities');
  local facet_pool     = k(queue, 'facet_pool');
  local messages       = k(queue, facet);
  local inflight_facet = k(queue, facet .. ':inflight');

  local current       = tonumber(redis.call('hget', facet_pool, facet)) or 0;
  local priority      = tonumber(redis.call('hget', priorities, facet)) or 1;
  local length        = redis.call('llen', messages);
  local inflight_cur  = tonumber(redis.call('scard', inflight_facet)) or 0;
  local inflight_max  = tonumber(redis.call('get', inflight_limit)) or 0;
  
  local n = 0
  
  -- redis.log(redis.LOG_WARNING, current.."/"..length.."/"..priority.."/"..inflight_max.."/"..inflight_cur);
  
  if inflight_max > 0 then
    n = math.min(length, priority, inflight_max - inflight_cur);
  else
    n = math.min(length, priority);
  end
  
  -- redis.log(redis.LOG_WARNING, "PULL: "..current.."/"..n);
  
  if n < current then
    -- redis.log(redis.LOG_WARNING, "shrinking");
    redis.call('hset', facet_pool, facet, current - 1);
  elseif n > current then
    -- redis.log(redis.LOG_WARNING, "growing");
    redis.call('lpush', round_robin, facet);
    redis.call('lpush', round_robin, facet);
    redis.call('hset', facet_pool, facet, current + 1);
  else
    -- redis.log(redis.LOG_WARNING, "maintaining");
    redis.call('lpush', round_robin, facet);
  end
  
  if (length == 0 and inflight_cur == 0 and n == 0) then
    redis.call('del', inflight_facet);
    redis.call('hdel', facet_pool, facet);
    redis.call('srem', active_facets, facet);
  end
end

-- Multiple queues can be passed through
-- fairway_pull. We'll loop through all 
-- provided queues, and return a message
-- from the first one that isn't empty.
for i, queue in ipairs(ARGV) do
  local inflight = k(queue, 'inflight');

  if wait ~= -1 then
    -- Check if any current inflight messages
    -- have been inflight for a long time.
    local inflightmessages = redis.call('zrange', inflight, 0, n, 'WITHSCORES');

    local msgs = {}

    -- If we have an inflight message and it's score
    -- is less than the current pull timestamp, reset
    -- the inflight score for the the message and resend.
    if #inflightmessages > 0 then
      for i = 1, #inflightmessages, 2 do
        local msg = inflightmessages[i];
        local ts  = tonumber(inflightmessages[i+1]);

        if tonumber(ts) <= timestamp then
          redis.call('zadd', inflight, timestamp + wait, msg);
          table.insert(msgs, msg);
        end
      end
    end

    if #msgs > 0 then
      return {queue, msgs}
    end
  end

  local pulled = pull(queue, n);
  local facet = pulled[1];
  local msgs = pulled[2];

  if facet then
    manage(queue, facet);
    
    -- if #msgs > 0 then
    -- else
    --   -- TODO loop through until we find a message
    --   pulled = pull(queue);
    --   facet = pulled[1];
    --   message = pulled[2];       
    
    --   if facet then
    --     manage(queue, facet);
    --   end
    -- end

    return {queue, msgs};
  end
end
`
}
