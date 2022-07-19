# frozen_string_literal: true

module Earth2
  module Kafka
    class Railtie < ::Rails::Railtie
      rake_tasks do
        load File.expand_path('../../tasks/kafka.rake', __dir__)
      end
    end
  end
end
