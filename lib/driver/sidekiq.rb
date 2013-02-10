require 'sidekiq/fetch'

module Driver
  class SidekiqFetch < ::Sidekiq::BasicFetch
    def initialize(options)
      Sidekiq.logger.info("DriverSidekiqFetch activated")
      super
    end

    def retrieve_work
      work = Driver::Client.new.pull(*queues_cmd)
      UnitOfWork.new(*work) if work
    end
  end
end
