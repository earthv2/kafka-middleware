# frozen_string_literal: true

require 'active_support/cache'
require 'active_support/notifications'

module Earth2
  module Kafka
    class CachedSchemaRegistry
      attr_reader :registry

      Cache = ActiveSupport::Cache

      DEFAULT_MEMORY_OPTIONS = {
        expires_in: 2.hours
      }.freeze
      private_constant :DEFAULT_MEMORY_OPTIONS

      def initialize(registry, memory: DEFAULT_MEMORY_OPTIONS, redis: nil)
        @registry = registry
        @cache_stores = [Cache::MemoryStore.new(memory.dup)]
        @cache_stores << Cache::RedisCacheStore.new(redis) if redis.present?
        @monitors = Concurrent::Hash.new
      end

      def get_schema(schema_name: nil, schema_id: nil, version: 'latest', **options)
        schema_options = { schema_name: schema_name, schema_id: schema_id, version: version }
        cache_key = schema_cache_key(**schema_options)
        fetch_schema(cache_key, options) do
          registry.get_schema(**schema_options)
        end
      end

      def fetch_schema(cache_key, options = {}, index = 0, &block)
        cache = cache_stores[index]
        cache.fetch(cache_key, options) do
          index += 1
          if cache_stores.size < index
            fetch_schema(cache_key, options, index, &block)
          elsif block_given?
            monitor = monitors[cache_key] ||= Monitor.new
            monitor.synchronize do
              return cache.read(cache_key) if cache.exist?(cache_key)

              block.call
            end
          end
        end
      end

      protected

      attr_reader :cache_stores, :monitor, :monitors

      def schema_cache_key(schema_name: nil, schema_id: nil, version: 'latest')
        schema_id.presence || [schema_name, version].join('-')
      end
    end
  end
end
