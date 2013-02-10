module Driver
  DELIVER_MESSAGE = <<-SCRIPT
    local namespace     = KEYS[1];
    local environment   = ARGV[1];
    local type          = ARGV[2];
    local name          = ARGV[3];
    local message       = ARGV[4];
    local message_topic = environment .. ':' .. type .. ':' .. name;

    local registered_queues_key = namespace .. 'registered_queues';
    local registered_queues     = redis.call('hgetall', registered_queues_key);

    for i = 1, #registered_queues, 2 do
      local queue_name    = registered_queues[i];
      local queue_message = registered_queues[i+1];

      if string.find(message_topic, queue_message) then
        local facet         = queue_name .. ':' .. environment;
        local active_facets = namespace .. queue_name .. ':active_facets';
        local facet_queue   = namespace .. queue_name .. ':facet_queue';

        redis.call('lpush', namespace .. facet, message)

        if redis.call('sadd', active_facets, facet) == 1 then
          redis.call('lpush', facet_queue, facet);
        end
      end
    end

    redis.call('publish', namespace .. message_topic, message);
  SCRIPT
end

