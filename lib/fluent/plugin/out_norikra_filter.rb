require 'fluent/output'
require_relative 'norikra/input'
require_relative 'norikra/output'

require 'norikra-client'

module Fluent
  class NorikraFilterOutput < Fluent::BufferedOutput
    include Fluent::NorikraPlugin::InputMixin
    include Fluent::NorikraPlugin::OutputMixin

    Fluent::Plugin.register_output('norikra_filter', self)

    config_set_default :flush_interval, 1 # 1sec

    config_param :norikra, :string, :default => 'localhost:26571'

    config_param :connect_timeout, :integer, :default => nil
    config_param :send_timeout, :integer, :default => nil
    config_param :receive_timeout, :integer, :default => nil

    #<server>
    attr_reader :execute_server, :execute_server_path

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

      if !@target_map_tag && @target_map_key.nil? && @target_string.nil?
        raise Fluent::ConfigError, 'target naming not specified (target_map_tag/target_map_key/target_string)'
      end

      @execute_server = false

      conf.elements.each do |element|
        case element.name
        when 'server'
          @execute_server = true
          @execute_jruby_path = element['jruby']
          @execute_server_path = element['path']
          @execute_server_opts = element['opts']
        end
      end

      setup_output(conf, true) # <query> enabled in <default> and <target TARGET>
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

      @norikra_started = false

      if @execute_server
        @norikra_pid = nil
        @norikra_thread = Thread.new(&method(:server_starter))
        # @norikra_started will be set in server_starter
      else
        @norikra_pid = nil
        @norikra_thread = nil
        @norikra_started = true
      end

      start_output
      start_input
    end

    def shutdown
      stop_output
      stop_input
      Process.kill(:TERM, @norikra_pid) if @execute_server

      shutdown_output
      shutdown_input

      if @execute_server
        begin
          counter = 0
          while !Process.waitpid(@norikra_pid, Process::WNOHANG)
            sleep 1
            break if counter > 3
          end
        rescue Errno::ECHILD
          # norikra server process exited.
        end
      end
    end

    def server_starter
      log.info "starting Norikra server process #{@host}:#{@port}"
      base_options = [@execute_server_path, 'start', '-H', @host, '-P', @port.to_s]
      cmd,options = if @execute_jruby_path
                      [@execute_jruby_path, [@execute_server_path, 'start', '-H', @host, '-P', @port.to_s]]
                    else
                      [@execute_server_path, ['start', '-H', @host, '-P', @port.to_s]]
                    end
      if @execute_server_opts
        options += @execute_server_opts.split(/ +/)
      end
      @norikra_pid = fork do
        ENV.keys.select{|k| k =~ /^(RUBY|GEM|BUNDLE|RBENV|RVM|rvm)/}.each {|k| ENV.delete(k)}
        exec([cmd, 'norikra(fluentd)'], *options)
      end
      connecting = true
      log.info "trying to confirm norikra server status..."
      while connecting
        begin
          log.debug "start to connect norikra server #{@host}:#{@port}"
          client(:connect_timeout => 1, :send_timeout => 1, :receive_timeout => 1).targets
          # discard result: no exceptions is success
          connecting = false
          next
        rescue HTTPClient::TimeoutError
          log.debug "Norikra server test connection timeout. retrying..."
        rescue Errno::ECONNREFUSED
          log.debug "Norikra server test connection refused. retrying..."
        rescue => e
          log.error "unknown error in confirming norikra server, #{e.class}:#{e.message}"
        end
        sleep 3
      end
      log.info "confirmed that norikra server #{@host}:#{@port} started."
      @norikra_started = true
    end

    def fetchable?
      @norikra_started
    end
  end
end
