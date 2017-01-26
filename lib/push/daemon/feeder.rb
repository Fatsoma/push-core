module Push
  module Daemon
    class Feeder
      extend DatabaseReconnectable
      extend InterruptibleSleep

      # The number of messages to batch at the same time
      BATCH_SIZE = 1000

      def self.name
        "Feeder"
      end

      def self.start(config)
        reconnect_database unless config.foreground

        loop do
          break if @stop
          enqueue_notifications
          interruptible_sleep config.push_poll
        end
      end

      def self.stop
        @stop = true
        interrupt_sleep
      end

      protected

      def self.enqueue_notifications
        begin
          with_database_reconnect_and_retry(name) do
            ready_apps = Push::Daemon::App.ready
            push_message_ids = Push::Message.ready_for_delivery.pluck(:id)
            push_message_ids.in_groups_of(BATCH_SIZE, false) do |ids|
              Push::Message.where(id: ids).each do |push_message|
                Push::Daemon::App.deliver(push_message) if ready_apps.include?(push_message.app)
              end
            end
          end
        rescue StandardError => e
          Push::Daemon.logger.error(e)
        end
      end
    end
  end
end