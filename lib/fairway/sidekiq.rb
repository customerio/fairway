require "fairway/sidekiq/composite_fetch"
require "fairway/sidekiq/fetcher"
require "fairway/sidekiq/fetcher_factory"
require "fairway/sidekiq/non_blocking_fetch"
require "fairway/sidekiq/queue_fetch"

# conn = Fairway::Connection.new
# queue_reader = Fairway::QueueReader.new(conn, "default")
# sidekiq_queues = { high: 2, default: 1 }
# Sidekiq.options[:fetcher] = Fairway::FetcherFactory.new(queue_reader, sidekiq_queues)
