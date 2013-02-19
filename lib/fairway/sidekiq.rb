require "sidekiq"

require "fairway/sidekiq/composite_fetch"
require "fairway/sidekiq/basic_fetch"
require "fairway/sidekiq/fetch"

# conn         = Fairway::Connection.new
# queue_reader = Fairway::QueueReader.new(conn, "fairway")
#
# queue_fetch  = Fairway::Sidekiq::Fetch.new(queue_reader) do |message|
#   # Transform message into a sidekiq job
#   message
# end
# 
# sidekiq_fetch = Fairway::Sidekiq::BasicFetch.new(Sidekiq.options)
# Sidekiq.options[:fetch] = Fairway::Sidekiq::CompositeFetch.new(queue_fetch => 1, sidekiq_fetch => 1)
