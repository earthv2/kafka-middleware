# frozen_string_literal: true

module Earth2
  module Kafka
    class TopicMapper
      def prefix
        @prefix ||= [
          ENV.fetch('CLUSTER_NAME'),
          Earth2::Kafka.config.scope
        ].reject(&:blank?).join('.').concat('.')
      end
    end
  end
end
