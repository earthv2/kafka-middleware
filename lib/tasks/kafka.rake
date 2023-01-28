# frozen_string_literal: true

namespace :kafka do
  namespace :topics do
    def run_with_defaults(args, &block)
      config_path = ENV.fetch('CONFIG_PATH', Rails.root.join('config/topics.yaml'))
      args.with_defaults(config_path: config_path)

      manager = Earth2::Kafka::TopicsManager.new(args.config_path)
      block.arity.zero? ? manager.instance_eval(&block) : yield(manager)
    end

    task kafka: :environment do
      defined?(Karafka) && require(ENV.fetch('KARAFKA_BOOT_FILE', Rails.root.join('karafka')))
    end

    desc 'Sync kafka topics and their configuration with remote'
    task sync: :kafka do |_, args|
      run_with_defaults(args) do
        sync(args.extras)
      end
    end

    # @example rake kafka:topics:delete[a,b]
    desc 'Delete specific topics'
    task delete: :kafka do |_, args|
      run_with_defaults(args) do
        delete_many(args.extras)
      end
    end

    desc 'Delete all topics'
    task delete_all: :kafka do |_, args|
      run_with_defaults(args) do
        delete_all
      end
    end
  end
end
