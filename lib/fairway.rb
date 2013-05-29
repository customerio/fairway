require "fairway/version"

require "active_support/core_ext"
require "redis"
require "redis-namespace"

require "fairway/config"
require "fairway/scripts"
require "fairway/channeled_connection"
require "fairway/connection"
require "fairway/queue"
require "fairway/facet"

module Fairway
  def self.config
    @config ||= Config.new
  end

  def self.configure
    yield(config)
  end

  def self.reconfigure
    @config = Config.new
    yield(config)
  end
end
