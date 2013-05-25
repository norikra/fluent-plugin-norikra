module Fluent
  class NorikraOutput < Fluent::BufferedOutput
    # just namespace
  end
end

module Fluent
  class NorikraOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('norikra', self)

    config_set_default :flush_interval, 1 # 1sec

    config_param :norikra, :string, :default => 'localhost:26571'

    #<server>
    attr_reader :execute_server, :execute_server_path

    config_param :remove_tag_prefix, :string, :default => nil

    config_param :target_map_tag, :bool, :default => false
    config_param :target_map_key, :string, :default => nil
    config_param :target_string, :string, :default => nil

    # <default>
    # <target TARGET>

    # <events>
    attr_reader :event_method, :event_tag_generator, :event_sweep_interval

    def initialize
      super
      require_relative 'norikra_target'
      require 'norikra-client'
    end

    def configure(conf)
      super

      @host,@port = @norikra.split(':', 2)
      @port = @port.to_i

      if !@target_map_tag && @target_map_key.nil? && @target_string.nil?
        raise Fluent::ConfigError, 'target naming not specified (target_map_tag/target_map_key/target_string)'
      end
      @target_generator = case
                          when @target_string
                            lambda {|tag,record| @target_string}
                          when @target_map_key
                            lambda {|tag,record| record[@target_map_key]}
                          when @target_map_tag
                            lambda {|tag,record| tag.gsub(/^#{@remove_tag_prefix}(\.)?/, '')}
                          else
                            raise Fluent::ConfigError, "no one way specified to decide target"
                          end
      @target_map = {} # 'target' => instance of Fluent::NorikraOutput::Target
      @query_map = {} # 'query_name' => instance of Fluent::NorikraOutput::Query # for conversion from query_name to tag

      @default_target = ConfigSection.new(Fluent::Config::Element.new('default', nil, {}, []))
      @config_targets = {}
      event_section = nil
      conf.elements.each do |element|
        case element.name
        when 'default'
          @default_target = ConfigSection.new(element)
        when 'target'
          c = ConfigSection.new(element)
          @config_targets[c.target] = c
        when 'server'
          @execute_server = Fluent::Config.bool_value(element['execute_server'])
          @execute_server_path = element['path']
        when 'event'
          event_section = element
        else
          raise Fluent::ConfigError, "unknown configuration section name for this plugin: #{element.name}"
        end
      end

      @event_method = @event_tag_generator = @event_sweep_interval = nil
      if event_section
        @event_method = case event_section['method']
                        when 'sweep' then :sweep
                        when 'listen'
                          raise Fluent::ConfigError, "not implemeneted now"
                        else
                          raise Fluent::ConfigError, "unknown method #{event_section['method']}"
                        end
        unless event_section['tag']
          raise Fluent::ConfigError, "<event> section needs 'tag' configuration"
        end
        tag_prefix = if event_section.has_key?('tag_prefix')
                       event_section['tag_prefix'] + (event_section['tag_prefix'] =~ /\.$/ ? '' : '.')
                     else
                       ''
                     end
        tag_by, tag_arg = event_section['tag'].split(/ +/, 2)
        @event_tag_generator = case tag_by
                               when 'query_name' then lambda{|query_name,record| tag_prefix + query_name}
                               when 'field' then lambda{|query_name,record| tag_prefix + record[tag_arg]}
                               when 'string' then lambda{|query_name,record| tag_prefix + tag_arg}
                               else
                                 raise Fluent::ConfigError, "unknown tag configuration specified:#{event_section['tag']}"
                               end
        @event_sweep_interval = Fluent::Config.time_value(event_section['sweep_interval'] || '10s')
      end

      @mutex = Mutex.new
    end

    def start
      @started = false
      @sending_targets = {}
      @fetch_order = []
      # start norikra server if needed
      # create client instance and check connectivity (what way to wait server execution?)
      # event fetcher thread
    end

    def shutdown
      # stop fetcher
      # stop server if needed
    end

    def server_starter
      
    end

    def register_worker
    end

    def fetch_worker
    end

    def format(tag, time, record)
      target = @target_generator.call(tag, record)
      unless @target_map[target]
        t = Target.new(@default_target + @config_targets[target])
        #TODO: get lock and check target exists or not, and reserver fields, and register queries (and add @query_map)
        @target_map[target] = t
      end
      event = @target_map[target].convert(record)
      [target,event].to_msgpack
    end

    def prepared?(targets)
      @started && targets.reduce(true){|r,t| r && @sending_targets[t]}
    end

    def write(chunk)
      @client = Norikra::Client.new(@host, @port)

      events = {} # target => [event]
      chunk.msgpack_each do |target, event|
        events[target] ||= []
        events[target].push(event)
      end

      unless prepared?(events.keys)
        raise RuntimeError, "norikra server is not ready for this targets: #{events.keys.join(',')}"
      end

      events.keys.each do |target|
        @client.send(target, events)
      end
    end
  end
end
