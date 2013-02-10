# Driver (redis backbone)

## Responsibilities

* Allow consumption of messages from various sources
* Publish messages for services who are listening
* adds messages to queues which services have registered
* keeps track of which facets have messages queued
* allows messages to be pulled off of queues in round-robin by facet

### Queue structure

Once a service registers a queue, with a regex for what messages should be
added to the queue, the backbone will begin pushing matching messages onto the queue.

In some cases, queuing systems can have problems in multi-user systems. If one user
queues up a ton of messages, messages for other users may be delayed.

Driver solves that by allowing "facets" for each queue, which can be set to a facet
for each user.

Each facet is added to a list if it has messages waiting to be processed. This list
is used to enforce a round-robin stategy for pulling messages off of the queue. This
means we'll process one message for every facet which has messages queued, before
looping back and processing additional messages.

### Redis LUA scripting

Driver uses [LUA scripts](http://redis.io/commands/eval) inside of redis heavily. This is for a few reasons:

* There is complex logic that can't be expressed in normal redis commands.
* Each script contains many redis commands and it's important that these
commands are processed atomically.  A LUA script does that.
* Since the script is run inside of redis, once the script has started,
there's very low latency for each redis command.  So, the script executes
much faster than if we made each call independantly over the network.

This means your Redis version must be `>= 2.6.0`

### Usage

 Add driver to your Gemfile

    gem 'driver', git: 'git@github.com:customerio/driver.git'

Make sure to `bundle install`.

##### Configure driver

    Driver.configure do |config|
      config.redis     = { host: "yourserver.com", port: 6379 }
      config.namespace = "letsdrive"

      config.facet do |message|
        message[:user_id]
      end

      config.topic do |message|
        "#{message[:user_id]}:#{message[:type]}"
      end
    end

If you don't configure, it'll default to:

    Driver.configure do |config|
      config.redis     = { host: "localhost", port: 6379 }
      config.namespace = nil

      config.facet do |message|
        message[:facet]
      end

      config.topic do |message|
        message[:topic]
      end
    end

##### Create an instance of driver

    driver = Driver::Client.new

##### Send messages

    driver.deliver(facet: 1, type: :page, name: "http://customer.io/blog", referrer: "http://customer.io")

You can pass any hash of data you'd like. Using the default configuration, this message will have a topic
of `page`, which can useful if you'd like to listen for, or process, messages.

##### Listen for messages

If a message is sent in the middle of the forest, and no one is listening, was it ever really sent?

You can listen for messages that are delivered by driver by subscribing to message topics:

    driver.redis.psubscribe("page:*google*") do |on|
      on.pmessage do |pattern, channel, message|
        puts "[#{channel}] #{message}"
      end
    end

If you've configured your topic to be `"#{message[:type]}:#{message[:name]}`, this will listen for any page events with google in the name.

Now, if you deliver a message, it'll be printed out on the console.

*Note:* redis psubscribe is blocking. So, you'll need multiple console windows open.
One to deliver the message, and one to listen for them.

##### Create a queue

Ok, so now you can listen to messages, but what if your listener dies and you miss all your important messages?

Not to worry, you can tell driver to queue up any messages you want to know about.

    driver.register_queue("myqueue", ".*:page:.*google.*")

Now driver will deliver all page events with google in the name to the queue named `myqueue`. To retrieve messages
from your queue:

    message = driver.pull("myqueue")

This will return a message or nil (if no messages are queued).

*Note:* `pull` is facet aware, and will rotate through all facets with queued messages.
