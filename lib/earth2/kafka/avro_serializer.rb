# frozen_string_literal: true

module Earth2
  module Kafka
    class AvroSerializer
      attr_reader :avro, :options

      def initialize(avro, options = {})
        @avro = avro
        @options = options
      end

      def call(data, options = {})
        data = data.to_hash if data.respond_to?(:to_hash)

        avro.encode(data, **options.reverse_merge(@options))
      end
    end
  end
end
