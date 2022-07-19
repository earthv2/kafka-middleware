# frozen_string_literal: true

require 'erb'
require 'yaml'
require 'kafka'

module Earth2
  module Kafka
    class TopicsManager
      attr_reader :config_path

      class << self
        def sync(*args)
          new(*args).sync
        end
      end

      def initialize(config_path)
        @config_path = config_path
      end

      def sync
        result = load_file(config_path)
        topics = result.fetch('topics')
        return if topics.nil?

        topics.each do |name, options|
          create_or_alter_topic(name, options.symbolize_keys)
        end
      end

      def delete_many(topics)
        Array(topics).each do |name|
          delete(name)
        end
      end

      def delete(name)
        return unless name.in?(kafka_topics)

        full_name = to_outgoing_topic(name)
        kafka.delete_topic(full_name)
        puts "#{full_name} topic removed"
      end

      def delete_all
        delete(kafka_topics)
      end

      def create_or_alter_topic(name, options)
        if kafka_topics.include?(name)
          return if options.blank?

          alter_partitions_for(name, options.fetch(:num_partitions))
          alter_config_for(name, options.fetch(:config))
        else
          create_topic(name, options)
        end
      end

      def create_topic(name, options)
        full_name = to_outgoing_topic(name)
        puts "Create #{full_name} topic with options:"
        print_options(options)
        kafka.create_topic(full_name, **options)
      end

      def alter_config_for(name, config = {})
        return if config.blank?

        full_name = to_outgoing_topic(name)
        existing_config = kafka.describe_topic(full_name, config.keys)
        return if existing_config == config

        puts "Alter #{full_name} topic config with:"
        print_options(config)
        kafka.alter_topic(full_name, config)
      end

      def alter_partitions_for(name, num_partitions)
        return if num_partitions.nil? || (num_partitions == kafka.partitions_for(name))

        full_name = to_outgoing_topic(name)
        puts "Alter #{full_name} topic with #{num_partitions} partitions"
        kafka.create_partitions_for(full_name, num_partitions: num_partitions)
      end

      def kafka
        @kafka ||= ::Kafka.new(ENV.fetch('KAFKA_BROKER_HOSTS'))
      end

      def kafka_topics
        @kafka_topics ||= kafka.topics.reject { |name| name.start_with?('__') }
      end

      protected

      def to_outgoing_topic(name)
        if defined?(Karafka)
          Karafka::App.config.topic_mapper.outgoing(name)
        else
          name
        end
      end

      def load_file(file_path)
        yaml_content = ERB.new(File.read(file_path)).result
        YAML.safe_load(yaml_content, aliases: true)
      end

      def print_options(options)
        options.each do |name, value|
          puts "- #{name}: #{value}"
        end
      end
    end
  end
end
