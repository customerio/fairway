package fairway

func FairwayInflight() string {
	return `
local namespace = KEYS[1];

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

for i, queue in ipairs(ARGV) do
  return redis.call('zrange', k(queue, 'inflight'), 0, -1);
end
`
}
