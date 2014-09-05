package fairway

func FairwayPing() string {
	return `
local namespace = KEYS[1];
local timestamp = tonumber(KEYS[2]);
local wait = tonumber(KEYS[3]);

local k = function (queue, subkey)
  return namespace .. queue .. ':' .. subkey;
end

local queue = ARGV[1];
local message = ARGV[2];

local inflight = k(queue, 'inflight');

redis.call('zadd', inflight, timestamp + wait, message);
`
}
