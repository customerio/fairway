package fairway

func FairwayPeek() string {
	return `
local namespace = KEYS[1];

for index, queue_name in ipairs(ARGV) do
  local facet_queue   = namespace .. queue_name .. ':facet_queue';

  local facet = redis.call('lrange', facet_queue, -1, -1)[1];

  if facet then
    local message_queue = namespace .. queue_name .. ':' .. facet;
    local message = redis.call('lrange', message_queue, -1, -1)[1];

    return {queue_name, message};
  end
end

`
}
