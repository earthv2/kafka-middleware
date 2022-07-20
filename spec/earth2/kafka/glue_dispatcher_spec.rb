# frozen_string_literal: true

require 'spec_helper'
require 'earth2/kafka/glue_schema_registry'
require 'json'

RSpec.describe Earth2::Kafka::GlueDispatcher do
  describe '#uuid_to_binary' do
    it 'must be decodable' do
      uuid = '217aad3b-003d-4df0-a9ee-4fc9708f40bd'

      binary_string = described_class.uuid_to_binary(uuid)

      expect(uuid).to eq(described_class.binary_to_uuid(binary_string))
    end
  end

  describe '.encode' do
    it 'must be encodable' do
      schema = File.read('spec/fixtures/user.avsc')
      json = JSON.parse(File.read('spec/fixtures/user.json'))
      schema_id = '217aad3b-003d-4df0-a9ee-4fc9708f40bd'

      allow_any_instance_of(Earth2::Kafka::GlueSchemaRegistry).
        to receive(:get_schema).and_return({ 'schema' => schema, 'id' => schema_id })

      messaging = described_class.new(registry: 'earth2-development')
      encoded_data = messaging.encode(json, schema_name: 'user')

      decoded_data = messaging.decode(encoded_data)

      expect(decoded_data).to eq(json.symbolize_keys)
    end
  end
end
