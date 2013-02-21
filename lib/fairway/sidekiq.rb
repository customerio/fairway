require "sidekiq"
require "sidekiq/fetch"

require "fairway/sidekiq/fetch"
require "fairway/sidekiq/basic_fetch"
require "fairway/sidekiq/fairway_fetch"

# conn  = Fairway::Connection.new
# queues = Fairway::Queue.new(conn, "queue1", "queue2")
# 
# Sidekiq.options[:fetch] = Fairway::Sidekiq::Fetch.new do |fetch|
#   fetch.from :sidekiq, 1
#   fetch.from queues, 1
# end
