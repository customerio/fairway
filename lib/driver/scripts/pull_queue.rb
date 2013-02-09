module Driver
  PULL_QUEUE = <<-SCRIPT
    local namespace     = KEYS[1];
    local queue_name    = ARGV[1];
    local active_facets = namespace .. ':' .. queue_name .. ':active_facets';
    local facet_queue   = namespace .. ':' .. queue_name .. ':facet_queue';

    local facet = redis.call('rpop', facet_queue);

    if facet then
      local message = redis.call('rpop', namespace .. ':' .. facet);

      if redis.call('llen', namespace .. ':' .. facet) == 0 then
        redis.call('srem', active_facets, facet);
      else
        redis.call('lpush', facet_queue, facet);
      end

      return message;
    end
  SCRIPT
end

