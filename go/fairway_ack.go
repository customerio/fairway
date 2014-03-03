package fairway

func FairwayAck() string {
	return `
local namespace = KEYS[1];

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

local queue = ARGV[1];
local message = ARGV[2];

return redis.call('zrem', k(queue, 'inflight'), message);
`
}
