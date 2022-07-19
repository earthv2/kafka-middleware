# frozen_string_literal: true

module Earth2
  module Kafka
    class AvroDeserializer
      attr_reader :avro, :options

      def initialize(avro, options = {})
        @avro = avro
        @options = options
      end

      def call(params)
        return if params.raw_payload.nil?

        avro.decode(params.raw_payload, **options)
      end
    end
  end
end
