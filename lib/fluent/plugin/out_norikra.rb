module Fluent
  class NorikraOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('norikra', self)

    config_set_default :flush_interval, 1 # 1sec

    config_param :norikra, :string, :default => 'localhost:26571'

    config_param :connect_timeout, :integer, :default => nil
    config_param :send_timeout, :integer, :default => nil
    config_param :receive_timeout, :integer, :default => nil

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
      require 'norikra/client'
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

      # target map already prepared (opened, and related queries registered)
      @target_map = {} # 'target' => instance of Fluent::NorikraOutput::Target

      # for conversion from query_name to tag
      @query_map = {} # 'query_name' => instance of Fluent::NorikraOutput::Query

      @default_target = ConfigSection.new(Fluent::Config::Element.new('default', nil, {}, []))
      @config_targets = {}

      @execute_server = false

      event_section = nil
      conf.elements.each do |element|
        case element.name
        when 'default'
          @default_target = ConfigSection.new(element)
        when 'target'
          c = ConfigSection.new(element)
          @config_targets[c.target] = c
        when 'server'
          @execute_server = Fluent::Config.bool_value(element['execute'])
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

    def client(opts={})
      Norikra::Client.new(@host, @port, {
          :connect_timeout => opts[:connect_timeout] || @connect_timeout,
          :send_timeout    => opts[:send_timeout]    || @send_timeout,
          :receive_timeout => opts[:receive_timeout] || @receive_timeout,
        })
    end

    def start
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

      # register worker thread
      @register_queue = []
      @register_thread = Thread.new(&method(:register_worker))

      # fetch worker thread
      @fetch_queue = []
      @fetch_thread = Thread.new(&method(:fetch_worker))

      # for sweep
      if @event_method
        @fetch_queue.push(FetchRequest.new(nil, @event_sweep_interval))
      end
    end

    def shutdown
      @register_thread.kill
      @fetch_thread.kill
      Process.kill(:TERM, @norikra_pid) if @execute_server

      @register_thread.join
      @fetch_thread.join
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

    def server_starter
      $log.info "starting Norikra server process #{@host}:#{@port}"
      @norikra_pid = fork do
        require 'pp'
        ENV.keys.select{|k| k =~ /^(RUBY|GEM|BUNDLE|RBENV|RVM|rvm)/}.each {|k| ENV.delete(k)}
        pp ENV
        exec [@execute_server_path, 'norikra(fluentd)'], 'start', '-H', @host, '-P', @port.to_s
      end
      connecting = true
      $log.info "trying to confirm norikra server status..."
      while connecting
        begin
          $log.debug "start to connect norikra server #{@host}:#{@port}"
          client(:connect_timeout => 1, :send_timeout => 1, :receive_timeout => 1).targets
          # discard result: no exceptions is success
          connecting = false
        rescue HTTPClient::TimeoutError
          $log.debug "Norikra server test connection timeout. retrying..."
          sleep 1
        rescue Errno::ECONNREFUSED
          $log.debug "Norikra server test connection refused. retrying..."
          sleep 1
        rescue => e
          $log.error "unknown error in confirming norikra server, #{e.class}:#{e.message}"
          sleep 1
        end
      end
      $log.info "confirmed that norikra server #{@host}:#{@port} started."
      @norikra_started = true
    end

    def register_worker
      while sleep(0.25)
        next unless @norikra_started

        c = client()

        targets = @register_queue.shift(10)
        targets.each do |t|
          next if @target_map[t.name]

          if prepare_target(c, t)
            # success
            @mutex.synchronize do
              t.queries.each do |query|
                @query_map[query.name] = query
                insert_fetch_queue(FetchRequest.new(query)) unless query.tag.empty? || @event_method
              end
              @target_map[t.name] = t
            end
          else
            $log.error "Failed to prepare norikra data for target:#{t.name}"
            @norikra_started.push(t)
          end
        end
      end
    end

    def fetch_worker
      while sleep(1)
        next unless @norikra_started
        next if @fetch_queue.first.nil? || @fetch_queue.first.time > Time.now

        now = Time.now
        while @fetch_queue.first.time <= now
          req = @fetch_queue.shift
          if req.query.nil?
            sweep()
          else
            fetch(req.query)
          end
          insert_fetch_queue(req)
        end
      end
    end

    def format_stream(tag, es)
      tobe_registered_target_names = []

      out = ''

      es.each do |time,record|
        target = @target_generator.call(tag, record)

        t = @target_map[target]
        unless tobe_registered_target_names.include?(target)
          @register_queue.push(Target.new(@default_target + @config_targets[target]))
          tobe_registered_target_names.push(target)
        end

        event = t.filter(record)

        out << [target,event].to_msgpack
      end

      out
    end

    def prepared?(target_names)
      @started && target_names.reduce(true){|r,t| r && @target_map[t]}
    end

    def write(chunk)
      events = {} # target => [event]
      chunk.msgpack_each do |target, event|
        events[target] ||= []
        events[target].push(event)
      end

      unless prepared?(events.keys)
        raise RuntimeError, "norikra server is not ready for this targets: #{events.keys.join(',')}"
      end

      c = clinet()

      events.keys.each do |target|
        c.send(target, events)
      end
    end

    def prepare_targets(client, target)
      # target open and reserve fields
      begin
        unless client.targets.include?(target.name)
          client.open(target.name, target.reserve_fields)
        end

        reserving = target.reserve_fields
        reserved = []
        client.fields(target.name).each do |field|
          if reserving[field['name']]
            reserved.push(field['name'])
            if reserving[field['name']] != field['type']
              $log.warn "field type mismatch, reserving:#{reserving[field['name']]} but reserved:#{field['type']}"
            end
          end
        end

        reserving.each do |fieldname,type|
          client.reserve(target, fieldname, type) unless reserved.include?(fieldname)
        end
      rescue => e
        $log.warn "failed to prepare target:#{target.name}, server:#{@host}:#{@port}, with error:#{e.class.to_s}, message:#{e.message}"
        return false
      end

      # query registration
      begin
        registered = Hash[client.queries.map{|q| [q['name'], q['expression']]}]
        target.queries.each do |query|
          if registered.has_key?(query.name) # query already registered
            if registered[query.name] != query.expression
              $log.warn "query name and expression mismatch, check norikra server status. target query name:#{query.name}"
            end
            next
          end
          client.register(query.name, query.expression)
        end
      rescue => e
        $log.warn "failed to register query, server:#{@host}:#{@port}, with error:#{e.class.to_s}, message:#{e.message}"
      end
    end

    class FetchRequest
      attr_accessor :time, :query
      def initialize(query, interval=nil)
        @query = query
        @interval = interval || query.interval
        @time = Time.now + @interval
      end
      def <=>(other)
        self.time <=> other.time
      end
      def next!
        @time = Time.now + @interval
      end
    end

    def insert_fetch_queue(request)
      @mutex.synchronize do
        request.next!
        next_pos = @fetch_queue.bsearch{|req| req.time > request.time}
        @fetch_queue.insert(next_pos, request)
      end
    end

    def sweep
      begin
        client().sweep.each do |query_name, event_array|
          query = @query_map[query_name]
          event_array.each do |time,event|
            tag = (query && !query.tag.empty?) ? query.tag : @event_tag_generator.call(query_name, event)
            Fluent::Engine.emit(tag, time, event)
          end
        end
      rescue => e
        $log.error "failed to sweep from norikra #{@host}:#{@port}, error:#{e.class.to_s}, message:#{e.message}"
      end
    end

    def fetch(query)
      begin
        client().event(query.name).each do |time,event| # [[time(int from epoch), event], ...]
          Fluent::Engine.emit(query.tag, time, event)
        end
      rescue => e
        $log.error "failed to fetch events for query:#{query.name}, from norikra #{@host}:#{@port}, error:#{e.class.to_s}, message:#{e.message}"
      end
    end
  end
end
