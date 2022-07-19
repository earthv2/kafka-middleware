# frozen_string_literal: true

require 'active_support/dependencies/autoload'

module Earth2
  module Kafka
    extend ActiveSupport::Autoload

    autoload :AvroDeserializer
    autoload :AvroSerializer
    autoload :CachedSchemaRegistry
    autoload :GlueDispatcher
    autoload :GlueSchemaRegistry
  end
end
