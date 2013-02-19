require "sidekiq"
require "sidekiq/fetch"

require "fairway/sidekiq/composite_fetch"
require "fairway/sidekiq/basic_fetch"
require "fairway/sidekiq/fetch"

# conn  = Fairway::Connection.new
# queue = Fairway::Queue.new(conn, "fairway")
#
# queue_fetch  = Fairway::Sidekiq::Fetch.new(queue) do |message|
#   # Transform message into a sidekiq job
#   message
# end
# 
# sidekiq_fetch = Fairway::Sidekiq::BasicFetch.new(Sidekiq.options)
# Sidekiq.options[:fetch] = Fairway::Sidekiq::CompositeFetch.new(queue_fetch => 1, sidekiq_fetch => 1)
