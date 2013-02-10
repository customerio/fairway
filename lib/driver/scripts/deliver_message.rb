module Driver
  DELIVER_MESSAGE = <<-SCRIPT
    local namespace = KEYS[1];
    local topic     = ARGV[1];
    local facet     = ARGV[2];
    local message   = ARGV[3];

    local registered_queues_key = namespace .. 'registered_queues';
    local registered_queues     = redis.call('hgetall', registered_queues_key);

    for i = 1, #registered_queues, 2 do
      local queue_name    = registered_queues[i];
      local queue_message = registered_queues[i+1];

      if string.find(topic, queue_message) then
        local active_facets = namespace .. queue_name .. ':active_facets';
        local facet_queue   = namespace .. queue_name .. ':facet_queue';

        redis.call('lpush', namespace .. queue_name .. ':' .. facet, message)

        if redis.call('sadd', active_facets, facet) == 1 then
          redis.call('lpush', facet_queue, facet);
        end
      end
    end

    redis.call('publish', namespace .. topic, message);
  SCRIPT
end

