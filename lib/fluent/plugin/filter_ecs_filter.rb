#
# Fluentd ECS Metadata Filter Plugin
#
# Copyright 2015 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
module Fluent
  # Parses ECS data from docker to make fluentd logs more
  # useful.
  class ECSFilter < Filter
    Fluent::Plugin.register_filter('ecs_filter', self)

    config_param :cache_size, :integer, default: 1000
    config_param :cache_ttl, :integer, default: 60 * 60
    config_param :container_id_attr, :string, default: nil
    config_param :task_family_prepend, :string, default: nil
    config_param :merge_json_log, :bool, default: true

    # Get the configuration for the plugin
    def configure(conf)
      super

      require 'docker-api'
      require 'lru_redux'
      require 'oj'
      require 'time'
      require 'vine'

      @cache_ttl = :none if @cache_ttl < 0

      @cache = LruRedux::TTL::ThreadSafeCache.new(@cache_size, @cache_ttl)
    end

    # Gets the log event stream and moifies it. This is where the plugin hooks
    # into the fluentd envent stream.
    def filter_stream(tag, es)
      new_es = MultiEventStream.new
      container_id_from_tag = nil

      if container_id_attr.nil?
        container_id_from_tag = get_container_id_from_tag(tag)
      end

      es.each do |time, record|
        #puts "OMG #{container_id_from_tag}"
        if container_id_from_tag.nil?
          puts "GETTING FROM RECORD"
          container_id = get_container_id_from_record(record)
          puts "GOT CONTAINER ID #{container_id} from record"
        else
          container_id = container_id_from_tag
        end
        next unless container_id
        new_es.add(time, modify_record(record, get_ecs_data(container_id)))
      end
      new_es
    end

    # Injects the ecs data into the record and also merges
    # the json log if that configuration is enabled.
    #
    # ==== Attributes:
    # * +record+ - The log record being processed
    # * +ecs_data+ - The ecs data retrived from the docker container
    #
    # ==== Returns:
    # * A record hash that has ecs data and optinally log data added
    def modify_record(record, ecs_data)
      modified_record = record.merge(ecs_data)
      modified_record = merge_json_log(modified_record) if @merge_json_log
      modified_record
    end

    # Gets the ecs data about a container from the cache or calls the Docker
    # api to retrieve the data about the container and store it in the cache.
    #
    # ==== Attributes:
    # * +container_id+ - The container_id where the log record originated from.
    # ==== Returns:
    # * A hash of data that describes a ecs task
    def get_ecs_data(container_id)
      @cache.getset(container_id) do
        get_container_metadata(container_id)
      end
    end

    # Goes out to docker container to pull ecs data from labels.
    #
    # ==== Attributes:
    # * +id+ - The id of the container to look at for ecs metadata.
    # ==== Returns:
    # * A hash that describes a ecs task gathered from the Docker API
    def get_container_metadata(id)
      task_data = {}
      container = Docker::Container.get(id)
      if container
        labels = container.json['Config']['Labels']
        task_data['task_family']  = labels['com.amazonaws.ecs.task-definition-family']
        task_data['task_family'].prepend(@task_family_prepend) if @task_family_prepend
        task_data['task_version'] = labels['com.amazonaws.ecs.task-definition-version']
        task_data['task_id']      = labels['com.amazonaws.ecs.task-arn'].split('/').last
      end
      task_data
    end

    # Gets the container id from the last element in the tag. If the user has
    # configured container_id_attr the container id can be gathered from the
    # record if it has been inserted there.
    #
    # ==== Attributes:
    # * +tag+ - The tag of the log being processed
    # ==== Returns:
    # * A docker container id
    def get_container_id_from_tag(tag)
      tag.split('.').last
    end

    # If the user has configured container_id_attr the container id can be
    # gathered from the record if it has been inserted there. If no container_id
    # can be found, the record is not processed.
    #
    # ==== Attributes::
    # * +record+ - The record that is being transformed by the filter
    # ==== Returns:
    # * A docker container id
    def get_container_id_from_record(record)
      record.access(@container_id_attr)
    end

    # Look at the log value and if it is valid json then we will parse the json
    # and merge it into the log record.  If a namespace is present then the log
    # record is placed under that key.
    # ==== Attributes:
    # * +record+ - The record we are transforming in the fluentd event stream.
    # ==== Examples
    # # Docker captures stdout and passes it in the 'log' record attribute.
    # # We try to discover is the value of 'log' is json, if it is then we
    # # will parse the json and add the keys and values to the record.
    # ==== Returns:
    # * A record hash that has json log data merged into the record
    def merge_json_log(record)
      if record.key?('log')
        log = record['log'].strip
        namespace = record['namespace']
        if log[0].eql?('{') && log[-1].eql?('}')
          begin
            log_json = Oj.load(log)
            if namespace
              record[namespace] = log_json
            else
              record = log_json.merge(record)
            end
          rescue Oj::ParseError
          end
        end
      end
      record
    end
  end
end
