require "driver/version"

require "active_support/core_ext"
require "redis"
require "hiredis"
require "redis-namespace"

require "driver/config"
require "driver/scripts"
require "driver/channeled_connection"
require "driver/connection"
require "driver/queue_reader"

module Driver
  def self.config
    @config ||= Config.new
  end

  def self.configure
    yield(config) if block_given?
  end
end
