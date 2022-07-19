# frozen_string_literal: true

module Earth2
  module Kafka
    class GlueSchemaRegistry
      attr_reader :registry_name

      def initialize(registry_name)
        @registry_name = registry_name
      end

      def get_schema(schema_name: nil, schema_id: nil, version: 'latest')
        params =
          if schema_id.present?
            { schema_version_id: schema_id }
          elsif schema_name.present?
            {
              schema_id: { schema_name: schema_name, registry_name: registry_name },
              schema_version_number: schema_version_number(version)
            }
          end

        response = glue_client.get_schema_version(params)

        {
          'id' => response.schema_version_id,
          'schema' => response.schema_definition
        }
      end

      protected

      def schema_version_number(version)
        is_latest = version == 'latest'

        { latest_version: is_latest }.tap do |h|
          h[:version_number] = version unless is_latest
        end
      end

      def glue_client
        @glue_client ||= Aws::Glue::Client.new
      end
    end
  end
end
