local namespace    = KEYS[1];
local queue        = ARGV[1];
local facet        = ARGV[2];
local new_priority = tonumber(ARGV[3]);

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

local set_priorities  = k(queue, 'priorities');
local real_priorities = k(queue, 'current_priorities');
local round_robin     = k(queue, 'facet_queue');

-- Find the current state of the facet for the queue
local priority = tonumber(redis.call('hget', set_priorities, facet)) or 1;
local current  = tonumber(redis.call('hget', real_priorities, facet));

-- If priority is currently zero, we need to jump
-- start the facet by adding it to the round-robin
-- queue and updating the current priority.
if new_priority > 0 and priority == 0 and current == 0 then
  redis.call('lpush', round_robin, facet);
  redis.call('hset', real_priorities, facet, 1);
end

-- Other than the 0 priority case, we can just
-- set the new priority, and the real priority
-- will update lazily on pull.
redis.call('hset', set_priorities, facet, new_priority);
