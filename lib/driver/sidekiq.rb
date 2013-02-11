require "driver/sidekiq/composite_fetch"
require "driver/sidekiq/fetcher"
require "driver/sidekiq/fetcher_factory"
require "driver/sidekiq/non_blocking_fetch"
require "driver/sidekiq/queue_fetch"

# driver_conn = Driver::Connection.new
# driver_queue_reader = Driver::QueueReader.new(driver_conn, "default")
# sidekiq_queues = { high: 2, default: 1 }
# Sidekiq.options[:fetcher] = Driver::Sidekiq::FetcherFactory.new(driver_queue_reader, sidekiq_queues)
