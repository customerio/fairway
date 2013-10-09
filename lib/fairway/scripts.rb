require "pathname"
require "digest/sha1"

module Fairway
  class Scripts
    def self.script_shas
      @script_shas ||= {}
    end

    attr_reader :redis

    def initialize(redis, namespace)
      @redis     = redis
      @namespace = namespace
    end

    def register_queue(name, channel)
      redis.pools.each do |pool|
        pool.with do |conn|
          conn.hset(registered_queues_key, name, channel)
        end
      end
    end

    def unregister_queue(name)
      redis.pools.each do |pool|
        pool.with do |conn|
          conn.hdel(registered_queues_key, name)
        end
      end
    end

    def registered_queues
      redis.with do |conn|
        conn.hgetall(registered_queues_key)
      end
    end

    def method_missing(method_name, *args)
      loaded = false

      if multi?(method_name)
        each_pool do |conn|
          conn.evalsha(script_sha(method_name), [namespace], args)
        end
      elsif first?(method_name)
        first_pool do |conn|
          conn.evalsha(script_sha(method_name), [namespace], args)
        end
      else
        redis.with do |conn|
          conn.evalsha(script_sha(method_name), [namespace], args)
        end
      end
    rescue Redis::CommandError => ex
      if ex.message.include?("NOSCRIPT") && !loaded
        each_pool do |conn|
          conn.script(:load, script_source(method_name))
        end

        loaded = true
        retry
      else
        raise
      end
    end

  private

    def first?(script)
      ["fairway_pull", "fairway_peek"].include?(script.to_s)
    end

    def multi?(script)
      ["fairway_priority", "fairway_destroy"].include?(script.to_s)
    end

    def each_pool(&block)
      redis.pools.each do |pool|
        pool.with do |conn|
          yield(conn)
        end
      end
    end

    def first_pool(&block)
      redis.pools.each do |pool|
        pool.with do |conn|
          val = yield(conn)
          return val if val
        end
      end

      nil
    end

    def registered_queues_key
      "#{namespace}registered_queues"
    end

    def namespace
      @namespace.blank? ? "" : "#{@namespace}:"
    end

    def script_sha(name)
      self.class.script_shas[name] ||= Digest::SHA1.hexdigest(script_source(name))
    end

    def script_source(name)
      script_path(name).read
    end

    def script_path(name)
      Pathname.new(__FILE__).dirname.join("../../redis/#{name}.lua")
    end
  end
end
