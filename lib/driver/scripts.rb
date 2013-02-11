require "pathname"
require "digest/sha1"

module Driver
  class Scripts
    def self.script_shas
      @script_shas ||= {}
    end

    def initialize(redis, namespace)
      @redis = redis
      @namespace = namespace
    end

    def method_missing(method_name, *args)
      loaded = false
      @redis.evalsha(script_sha(method_name), [@namespace], args)
    rescue Redis::CommandError => ex
      if ex.message.include?("NOSCRIPT") && !loaded
        @redis.script(:load, script_source(method_name))
        loaded = true
        retry
      else
        raise
      end
    end

  private

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
