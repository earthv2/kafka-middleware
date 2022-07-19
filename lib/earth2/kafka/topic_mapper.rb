# frozen_string_literal: true

module Earth2
  module Kafka
    class TopicMapper
      attr_reader :scope

      def initialize(scope = nil)
        @scope = scope.presence && normalize_scope(scope)
      end

      # @param topic [String, Symbol] The topic
      # @return [String, Symbol] topic as on input
      # @example
      #   incoming('dev.identity.users_created') #=> 'users_created'
      def incoming(topic)
        topic.to_s.gsub(prefix, '')
      end

      # @param topic [String, Symbol] The topic
      # @return [String, Symbol] topic as on input
      # @example
      #   outgoing('users_created') #=> 'dev.identity.users_created'
      def outgoing(topic)
        [prefix, topic.to_s].join
      end

      protected

      def normalize_scope(scope)
        scope.is_a?(Array) ? scope.reject(&:blank?).join('.') : scope
      end

      def prefix
        @prefix ||= scope.concat('.')
      end
    end
  end
end
