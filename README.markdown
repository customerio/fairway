# Fairway - a fair way to queue messages in multi-user systems.

## Installation

Install the gem:

    gem install fairway

Then make sure you `bundle install`.

## Configuration

    Fairway.configure do |config|
      config.redis     = { host: "localhost", port: 6379 }
      config.namespace = "fairway"
    
      config.facet do |message|
        message[:user]
      end

      config.register_queue("all_messages")
    end
    
## What's a facet?

In many queuing systems, if a single user manages to queue up a lot of messages/jobs at once,
everyone else in the system has to wait.  Facets are a way of splitting up a single queue by
user (or any other criteria) to ensure fair processing of each facet's jobs.

When pulling off a faceted queue, facets are processed in a round-robin fashion, so you'll pull
off one message for each facet which contains messages before doubling back and pulling
additional messages from a given facet.

You can define how to facet your messages during configuration:

    Fairway.configure do |config|
      config.facet do |message|
        message[:user]
      end
    end

Now, any message delivered by fairway, will use the `user` key of the message to determine
which facet to use for this message.

You could also just have a queue for each user, but at scale, this can get crazy and many
queuing systems don't perform well with thousands of queues.

## Queuing messages

In order to queue messages, you need to register a queue. You can register multiple queues,
and each queue will contain delivered messages.

Registering a queue is part of your fairway configuration:

    Fairway.configure do |config|
      config.register_queue("myqueue")
      config.register_queue("yourqueue")
    end

After configuring your queues, all you have to do is create a fairway connection,
and it'll handle persisting your queues in Redis:

    connection = Fairway::Connection.new

## Delivering messages

To add messages to your queues, you can deliver messages to fairway:

    connection = Fairway::Connection.new
    connection.deliver(type: "invite_friends", user: "bob", friends: ["nancy", "john"])

Now, any registered queues will contain this message, faceted if you've defined
a facet in your configuration.

## Consuming messages from a queue

Once you have messages on a queue, you'd like to pull them off an perform some
processing on each message. Start by instanting a queue object for the queue you'd
like to pull from:

    connection = Fairway::Connection.new
    queue      = Fairway::Queue.new(connection, "myqueue")

Now you can pull messages off the queue.

    queue.pull

Behind the scenes, we're using a round-robin strategy to ensure equal weighting of
any facets which contain messages.

If there are no messages in any facets of the queue, `queue.pull` will return `nil`.

## Channeling messages

In many cases, you don't want all messages to be queued up in every queue. You'd like
to only queue messages that you care about for a given queue.

You can accomplish this with messages channels. By default, all messages use a `default`
channel. You can customize this by creating a `Fairway::ChanneledConnection` and creating
a block which defines the channel for a given message:

    connection           = Fairway::Connection.new
    channeled_connection = Fairway::ChanneledConnection.new do |message|
      message[:type]
    end

You can also register queues for a given channel:

    Fairway.configure do |config|
      config.register_queue("invite_queue", "invite_friends")
    end
    
Now, only messages which have the channel `invite_friends` will be queued in your
`invite_queue`.

If you'd like to queue all channels which match a pattern:

    Fairway.configure do |config|
      config.register_queue("invite_queue", "invite_.*")
    end

Now, messages from the channels `invite_friends`, `invite_pets`, `invite_parents` will
be queued in the `invite_queue`.

## Pub/Sub

To listen for messages without the overhead of queuing them, you can subscribe to messages:

    connection = Fairway::Connection.new

    connection.subscribe do |message|
      # Do something with each message
    end

If you'd like to only receive some messages, you can subscribe to just a particular channel:

    connection = Fairway::Connection.new

    connection.subscribe("invite_*") do |message|
      # Do something with each message which
      # has a channel matching "invite_*"
    end

## Sidekiq

We don't want to create a new project for handling the processing of queued messages/jobs. For
that reason, we've integrated fairway with Sidekiq.

    require 'fairway/sidekiq'

    connection = Fairway::Connection.new
    queues     = Fairway::Queue.new(connection, "myqueue", "yourqueue")

    Sidekiq.options[:fetch] = FairwayFetch do |fetch|
      fetch.from :sidekiq, 2
      fetch.from queues, 1 do |message|
        # translate message to normalized Sidekiq job, if needed
      end
    end

`fetch.from :sidekiq, 2` will fetch from sidekiq queues you have defined through the
normal sidekiq configuration.

`fetch.from queues, 1` will pull messages from your fairway queue, and allow you to translate
them into fairway jobs if you'd like.

The second parameters are fetch weights, so in the above example, we'll look for jobs first from
your normal sidekiq queues twice as often as your fairway queues.

## Queue structure

TODO: low level description of what's going on? performance?

## LUA scripting

Fairway uses [LUA scripting](http://redis.io/commands/eval) heavily. This is for a few reasons:

* There is complex logic that can't be expressed in normal redis commands.
* Each script contains many redis commands and it's important that these
commands are processed atomically.
* Since the script is run inside of redis, once the script has started,
there's very low latency for each redis command.  So, the script executes
much faster than if we made each call independantly over the network.

This means your must be using a Redis version `>= 2.6.0`
