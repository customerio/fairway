package fairway

func FairwayDestroy() string {
	return `
local namespace = KEYS[1];

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

-- Multiple queues can be passed through
-- fairway_destroy. We'll loop through all 
-- provided queues, and delete related keys
-- for each queue.
for i, queue in ipairs(ARGV) do
  local priorities    = k(queue, 'priorities');
  local active_facets = k(queue, 'active_facets');
  local round_robin   = k(queue, 'facet_queue');
  local facet_pool    = k(queue, 'facet_pool');
  local length        = k(queue, 'length');

  local facets = redis.call('smembers', active_facets);

  for i = 1, #facets, 1 do
    redis.call('del', k(queue, facets[i]));
  end

  redis.call('del', priorities);
  redis.call('del', active_facets);
  redis.call('del', round_robin);
  redis.call('del', facet_pool);
  redis.call('del', length);
end

`
}
