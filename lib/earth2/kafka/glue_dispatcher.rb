# frozen_string_literal: true

module Earth2
  module Kafka
    class GlueDispatcher
      attr_reader :registry, :codec

      HEADER_VERSION_BYTE = [3].pack('C').freeze
      private_constant :HEADER_VERSION_BYTE

      COMPRESSION_BYTE = [5].pack('C').freeze
      private_constant :COMPRESSION_BYTE

      COMPRESSION_DEFAULT_BYTE = [0].pack('C').freeze
      private_constant :COMPRESSION_DEFAULT_BYTE

      COMPRESSIONS_BYTES = [COMPRESSION_BYTE, COMPRESSION_DEFAULT_BYTE].freeze
      private_constant :COMPRESSIONS_BYTES

      SCHEMA_VALIDATION_OPTIONS = {
        recursive: true,
        encoded: false,
        fail_on_extra_fields: true
      }.freeze
      private_constant :SCHEMA_VALIDATION_OPTIONS

      class << self
        def binary_to_uuid(binary)
          literals = binary.unpack('n*').map { |x| format('%.4x', x) }
          format('%s%s-%s-%s-%s-%s%s%s', *literals)
        end

        def uuid_to_bytes(uuid)
          uuid.scan(/[0-9a-f]{4}/).map { |x| x.to_i(16) }
        end

        def uuid_to_binary(uuid)
          uuid_to_bytes(uuid).pack('n*')
        end
      end

      def initialize(registry: nil, codec: nil)
        @registry = registry.is_a?(String) ? GlueSchemaRegistry.new(registry) : registry
        @codec = codec
      end

      def encode(message, schema_name: nil, version: 'latest', validate: true)
        schema, schema_id = fetch_schema(schema_name: schema_name, version: version)

        Avro::SchemaValidator.validate!(schema, message, SCHEMA_VALIDATION_OPTIONS) if validate

        stream = StringIO.new
        writer = Avro::IO::DatumWriter.new(schema)
        encoder = Avro::IO::BinaryEncoder.new(stream)

        # See details about header
        # https://github.com/awslabs/aws-glue-schema-registry/blob/017fdc5840ab64f2d297d6a4babac007a7f52b4e/serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/SerializationDataEncoder.java#L45-L49
        encoder.write(HEADER_VERSION_BYTE)
        encoder.write(should_compress? ? COMPRESSION_BYTE : COMPRESSION_DEFAULT_BYTE)
        encoder.write(self.class.uuid_to_binary(schema_id))

        writer.write(message, encoder)

        stream.string
      end

      def decode(data)
        stream = StringIO.new(data)
        decoder = Avro::IO::BinaryDecoder.new(stream)

        version_byte = decoder.read(1)
        validate_version_byte(version_byte)

        compression_byte = decoder.read(1)
        validate_compression_byte(compression_byte)

        schema_id = self.class.binary_to_uuid(decoder.read(16))
        writers_schema, = fetch_schema(schema_id: schema_id)

        reader = Avro::IO::DatumReader.new(writers_schema)
        message = reader.read(decoder)

        message.symbolize_keys
      end

      protected

      def validate_version_byte(version_byte)
        raise 'Header version byte is wrong' if version_byte != HEADER_VERSION_BYTE
      end

      def validate_compression_byte(compression_byte)
        raise 'Compression byte is wrong' unless compression_byte.in?(COMPRESSIONS_BYTES)
      end

      def compress(message, should_compress: false)
        should_compress ? codec.compress(message) : message
      end

      def decompress(message, should_decompress: false)
        should_decompress ? codec.decompress(message) : message
      end

      def should_compress?
        codec.present?
      end

      def fetch_schema(schema_name: nil, schema_id: nil, version: 'latest')
        schema_data = registry.get_schema(schema_name: schema_name, schema_id: schema_id, version: version)
        schema_id = schema_data.fetch('id')
        schema = Avro::Schema.parse(schema_data.fetch('schema'))
        [schema, schema_id]
      end
    end
  end
end
