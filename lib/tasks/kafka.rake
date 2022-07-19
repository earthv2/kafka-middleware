# frozen_string_literal: true

namespace :kafka do
  task :sync, %i[config_path] => :environment do |_, args|
    args.with_defaults(config_path: ENV.fetch('CONFIG_PATH', Rails.root.join('config/topics.yaml')))
    Earth2::Kafka::TopicsManager.sync(args.config_path)
  end

  task :delete, %i[config_path] => :environment do |_, args|
    args.with_defaults(config_path: ENV.fetch('CONFIG_PATH', Rails.root.join('config/topics.yaml')))
    Earth2::Kafka::TopicsManager.new(args.config_path).delete(args.extras)
  end

  task :delete, %i[config_path] => :environment do |_, args|
    args.with_defaults(config_path: ENV.fetch('CONFIG_PATH', Rails.root.join('config/topics.yaml')))
    Earth2::Kafka::TopicsManager.new(args.config_path).delete_all
  end
end
