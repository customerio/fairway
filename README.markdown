# Driver (redis backbone)

## Responsibilities

* Consume events from various sources
* Publish events for services who are listening
* adds events to queues which services have registered
* keeps track of which environments have events queued
* allows messages to be pulled off of queues in round-robin by environment

### Queue structure

Once a service registers a queue, with a regex for what messages should be
added to the queue, the backbone will begin pushing matching messages onto the queue.

In order to make sure one environment doesn't back up the queue for everyone else,
each queue as a number of "facets", one for each environment with queued messages.

Each facet is added to a list if it has messages waiting to be processed. This list
is used to enforce a round-robin stategy for pulling messages off of the queue. This
means we'll process one message for every environment which has messages queued, before
looping back and processing additional messages.

### Redis LUA scripting

Driver uses [LUA scripts](http://redis.io/commands/eval) inside of redis heavily. This is for a few reasons:

* There is complex logic that can't be expressed in normal redis commands.
* Each script contains many redis commands and it's important that these
commands are processed atomically.  A LUA script does that.
* Since the script is run inside of redis, once the script has started,
there's very low latency for each redis command.  So, the script executes
much faster than if we made each call independantly over the network.

### LUA script for processing messages through the backbone

Usage:

    redis.eval(
      process_message_script,
      [5, "email_event", "opened_email"],
      [{ environment_id: 5, type: "email_event", name: "opened_email" }]
    )

Code:

    local environment  = KEYS[1];
    local type         = KEYS[2];
    local name         = KEYS[3];
    local message      = ARGV[1];

    local message_topic = environment .. ':' .. type .. ':' .. name;

    -- send pub/sub notification of message

    redis.call('publish', message_topic, message);

    -- retrieve registered message queues

    local registered_queues = redis.call('smembers', 'registered_queues');

    for i = 1, #registered_queues do
      local queue_parts   = split(registered_queues[i], '|');
      local queue_name    = queue_parts[1];
      local queue_message = queue_parts[2];

      -- if queue matches the message topic, queue message for environment facet

      if message_topic.find(queue_message) then
        local facet_queue = queue_name .. ':' .. environment;

        redis.call('lpush', facet_queue, message)

        if not redis.call('sismember', queue_name .. ':facets', facet_queue) then
          -- add facet to list of awaiting facets

          redis.call('sadd', queue_name .. ':facets', facet_queue);
          redis.call('lpush', queue_name .. ':facet_order', facet_queue);
        end
      end
    end

### LUA script for retrieving a queued message

Usage: `redis.eval(pull_message_script, ["email_events"])`

    local queue_name  = KEYS[1];

    -- find next facet we should pull from

    local facet_queue = redis.call('rpop', queue_name .. ':facet_order');

    -- pull message off of facet queue

    local message = redis.call('rpop', facet_queue);

    if redis.call('llen', facet_queue) == 0 then
      -- remove facet from the queue's facets

      redis.call('srem', queue_name .. ':facets', facet_queue);
    else
      -- push facet back on the facet ordering queue

      redis.call('lpush', queue_name .. ':facet_order', facet_queue);
    end

    return message;

# Services

## Responsibilities

* Subscribe to published messages
* Register messages to queue
* Pull messages off of queue and process them

### Ruby script for subscribing to event notifications

    redis.psubscribe('*:page:*') do
      # do something with each page view for any environment
    end

### LUA script for registering event types to be queued

Usage: `redis.eval(create_queue_script, ["email_events", ".*:email_event:.*"])`

    local queue_name    = KEYS[1];
    local queue_message = KEYS[2];

    redis.call('sadd', 'registered_queues', queue_name .. '|' .. queue_message);
