require_relative 'norikra/input'

require 'norikra-client'

module Fluent
  class NorikraInput < Fluent::Input
    include Fluent::NorikraPlugin::InputMixin

    Fluent::Plugin.register_input('norikra', self)

    config_param :norikra, :string, :default => 'localhost:26571'

    config_param :connect_timeout, :integer, :default => nil
    config_param :send_timeout, :integer, :default => nil
    config_param :receive_timeout, :integer, :default => nil

    # <fetch> tags
    # <fetch>
    #   method event
    #   target QUERY_NAME
    #   interval 5s
    #   tag    query_name
    #   # tag    field FIELDNAME
    #   # tag    string FIXED_STRING
    #   tag_prefix norikra.event     # actual tag: norikra.event.QUERYNAME
    # </fetch>
    # <fetch>
    #   method sweep
    #   target QUERY_GROUP # or unspecified => default
    #   interval 60s
    #   tag field group_by_key
    #   tag_prefix norikra.query
    # </fetch>

    # Define `log` method for v0.10.42 or earlier
    unless method_defined?(:log)
      define_method("log") { $log }
    end

    def configure(conf)
      super

      @host,@port = @norikra.split(':', 2)
      @port = @port.to_i

      conf.elements.each do |element|
        case element.name
        when 'fetch'
          # ignore: processed in InputMixin, and set @fetch_queue
        else
          raise Fluent::ConfigError, "unknown configuration section name for this plugin: #{element.name}"
        end
      end

      setup_input(conf)
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
      start_input
    end

    def shutdown
      stop_input
      shutdown_input
    end

    def fetchable?
      true
    end
  end
end
