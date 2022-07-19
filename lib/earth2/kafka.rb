# frozen_string_literal: true

require 'active_support/dependencies/autoload'
require_relative './kafka/railtie' if defined?(Rails)

module Earth2
  module Kafka
    extend ActiveSupport::Autoload

    autoload :AvroDeserializer
    autoload :AvroSerializer
    autoload :CachedSchemaRegistry
    autoload :GlueDispatcher
    autoload :GlueSchemaRegistry
    autoload :TopicsManager
    autoload :TopicMapper
  end
end
