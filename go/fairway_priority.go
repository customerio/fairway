package fairway

func FairwayPriority() string {
	return `
local namespace    = KEYS[1];
local queue        = ARGV[1];
local facet        = ARGV[2];
local new_priority = tonumber(ARGV[3]);

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

local priorities  = k(queue, 'priorities');
local round_robin = k(queue, 'facet_queue');
local facet_pool  = k(queue, 'facet_pool');

-- Find the current state of the facet for the queue
local priority = tonumber(redis.call('hget', priorities, facet)) or 1;
local current  = tonumber(redis.call('hget', facet_pool, facet));

-- If priority is currently zero, we need to jump
-- start the facet by adding it to the round-robin
-- queue and updating the current priority.
if new_priority > 0 and priority == 0 and current == 0 then
  redis.call('lpush', round_robin, facet);
  redis.call('hset', facet_pool, facet, 1);
end

-- Other than the 0 priority case, we can just
-- set the new priority, and the real priority
-- will update lazily on pull.
redis.call('hset', priorities, facet, new_priority);

`
}
