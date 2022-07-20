# frozen_string_literal: true

module Earth2
  module Kafka
    module ApplicationExtension
      module_function

      def to_kafka_uri(uris)
        return if uris.nil?

        uris = uris.split(',') if uris.is_a?(String)
        uris.map { |host| host.start_with?('kafka://') ? host : "kafka://#{host}" }
      end

      def kafka_dispatcher
        glue_dispatcher
      end

      def glue_dispatcher
        @glue_dispatcher ||= Earth2::Kafka::GlueDispatcher.new(registry: glue_cached_registry)
      end

      def glue_cached_registry
        @glue_cached_registry ||= begin
          registry = Earth2::Kafka::GlueSchemaRegistry.new(schema_name)
          Earth2::Kafka::CachedSchemaRegistry.new(registry)
        end
      end

      def schema_name
        @schema_name ||= ENV.fetch('GLUE_SCHEMA_NAME', "earth2-#{Rails.env}")
      end
    end
  end
end
