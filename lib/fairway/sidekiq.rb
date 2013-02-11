require "sidekiq"
require "sidekiq/manager"

require "fairway/sidekiq/composite_fetch"
require "fairway/sidekiq/fetcher"
require "fairway/sidekiq/fetcher_factory"
require "fairway/sidekiq/non_blocking_fetch"
require "fairway/sidekiq/queue_fetch"

# conn         = Fairway::Connection.new
# queue_reader = Fairway::QueueReader.new(conn, "fairway")
#
# queue_fetch  = Fairway::Sidekiq::QueueFetch.new(queue_reader) do |message|
#   # Transform message into a sidekiq job
#   message
# end
# 
# non_blocking_fetch = Fairway::Sidekiq::NonBlockingFetch.new(Sidekiq.options)
# fetch              = Fairway::Sidekiq::CompositeFetch.new(queue_fetch => 1, non_blocking_fetch => 1)
# Sidekiq.options[:fetcher] = Fairway::Sidekiq::FetcherFactory.new(fetch)
