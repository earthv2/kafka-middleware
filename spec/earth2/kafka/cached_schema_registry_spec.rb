# frozen_string_literal: true

require 'spec_helper'
# require 'earth2/cached_schema_registry'

RSpec.describe Earth2::Kafka::CachedSchemaRegistry do
  describe '.get_schema' do
    context 'when non cached' do
      it 'returns a schema object' do
        schema = File.read('spec/fixtures/user.avsc')
        schema_id = '217aad3b-003d-4df0-a9ee-4fc9708f40bd'
        schema_object = { 'schema' => schema, 'id' => schema_id }

        registry = Earth2::Kafka::GlueSchemaRegistry.new('earth2-development')
        allow(registry).to receive(:get_schema).and_return(schema_object)

        cached_registry = described_class.new(registry)
        result = cached_registry.get_schema(schema_id: schema_id)

        expect(registry).to have_received(:get_schema)
        expect(result).to eq(schema_object)
      end
    end

    context 'when cached' do
      it 'calls a single remote call' do
        schema = File.read('spec/fixtures/user.avsc')
        schema_id = '217aad3b-003d-4df0-a9ee-4fc9708f40bd'
        schema_object = { 'schema' => schema, 'id' => schema_id }
        threads_count = 5

        registry = Earth2::Kafka::GlueSchemaRegistry.new('earth2-development')
        cached_registry = described_class.new(registry)

        allow(registry).to receive(:get_schema) { sleep(1) and schema_object }
        allow(cached_registry).to receive(:get_schema).and_call_original

        threads_count.times.map do
          Thread.start do
            result = cached_registry.get_schema(schema_id: schema_id)
            expect(result).to eq(schema_object)
          end
        end.each(&:join)

        expect(registry).to have_received(:get_schema).once
        expect(cached_registry).to have_received(:get_schema).exactly(threads_count).times
      end
    end
  end
end
