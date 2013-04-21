local namespace = KEYS[1];

for index, queue_name in ipairs(ARGV) do
  local set_priorities  = namespace .. queue_name .. ':priorities';
  local real_priorities = namespace .. queue_name .. ':current_priorities';
  local active_facets   = namespace .. queue_name .. ':active_facets';
  local facet_queue     = namespace .. queue_name .. ':facet_queue';

  local facet = redis.call('rpop', facet_queue);

  if facet then
    local message_queue = namespace .. queue_name .. ':' .. facet;
    local message = redis.call('rpop', message_queue);

    if message then
      redis.call('decr', namespace .. queue_name .. ':length');
    end

    local length = redis.call('llen', message_queue);

    if length == 0 then
      redis.call('srem', active_facets, facet);
    else
      local priority = tonumber(redis.call('hget', set_priorities, facet)) or 1
      local current  = tonumber(redis.call('hget', real_priorities, facet)) or 1

      if current < priority and length > current then
        -- Increase current priority
        redis.call('lpush', facet_queue, facet);
        redis.call('lpush', facet_queue, facet);
        redis.call('hset', real_priorities, facet, current + 1);
      elseif current > length or current > priority then
        -- Contract current priority
        redis.call('hset', real_priorities, facet, current - 1);
      else
        -- Keep current priority the same
        redis.call('lpush', facet_queue, facet);
      end
    end

    return {queue_name, message};
  end
end
