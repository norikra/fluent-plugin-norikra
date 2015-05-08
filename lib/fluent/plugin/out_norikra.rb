require_relative 'norikra/output'

require 'norikra-client'

module Fluent
  class NorikraOutput < Fluent::BufferedOutput
    include Fluent::NorikraPlugin::OutputMixin

    Fluent::Plugin.register_output('norikra', self)

    config_set_default :flush_interval, 1 # 1sec

    config_param :norikra, :string, :default => 'localhost:26571'

    config_param :connect_timeout, :integer, :default => nil
    config_param :send_timeout, :integer, :default => nil
    config_param :receive_timeout, :integer, :default => nil

    #for OutputMixin
    config_param :remove_tag_prefix, :string, :default => nil
    config_param :target_map_tag, :bool, :default => false
    config_param :target_map_key, :string, :default => nil
    config_param :target_string, :string, :default => nil
    config_param :drop_error_record, :bool, :default => true
    config_param :drop_server_error_record, :bool, :default => false
    config_param :drop_when_shutoff, :bool, :default => false

    # <default>
    # <target TARGET>

    # Define `log` method for v0.10.42 or earlier
    unless method_defined?(:log)
      define_method("log") { $log }
    end

    def configure(conf)
      super

      @host,@port = @norikra.split(':', 2)
      @port = @port.to_i

      if !@target_map_tag && @target_map_key.nil? && @target_string.nil?
        raise Fluent::ConfigError, 'target naming not specified (target_map_tag/target_map_key/target_string)'
      end

      conf.elements.each do |element|
        case element.name
        when 'default', 'target'
          # ignore: processed in OutputMixin
        else
          raise Fluent::ConfigError, "unknown configuration section name for this plugin: #{element.name}"
        end
      end

      setup_output(conf, false) # <query> disabled in <default> and <target TARGET>
    end

    def client(opts={})
      Norikra::Client.new(@host, @port, {
          :connect_timeout => opts[:connect_timeout] || @connect_timeout,
          :send_timeout    => opts[:send_timeout]    || @send_timeout,
          :receive_timeout => opts[:receive_timeout] || @receive_timeout,
        })
    end

    def start
      super
      start_output
    end

    def shutdown
      stop_output
      shutdown_output
    end

    def fetchable?
      true
    end
  end
end
