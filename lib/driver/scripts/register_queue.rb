module Driver
  REGISTER_QUEUE = <<-SCRIPT
    local set           = KEYS[1] .. ':registered_queues';
    local queue_name    = ARGV[1];
    local queue_message = ARGV[2];

    redis.call('sadd', set, queue_name .. '|' .. queue_message);
  SCRIPT
end

