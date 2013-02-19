require "fairway/version"

require "active_support/core_ext"
require "redis"
require "hiredis"
require "connection_pool"
require "redis-namespace"

require "fairway/config"
require "fairway/scripts"
require "fairway/channeled_connection"
require "fairway/connection"
require "fairway/queue_reader"

module Fairway
  def self.config
    @config ||= Config.new
  end

  def self.configure
    yield(config) if block_given?
  end
end
