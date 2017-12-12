require 'fluent/input'

require 'norikra-client'

require_relative 'fetch_request'

module Fluent
  class NorikraInput < Fluent::Input
    Fluent::Plugin.register_input('norikra', self)

    config_param :norikra, :string, default: 'localhost:26571'

    config_param :connect_timeout, :integer, default: nil
    config_param :send_timeout, :integer, default: nil
    config_param :receive_timeout, :integer, default: nil

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
          connect_timeout: opts[:connect_timeout] || @connect_timeout,
          send_timeout: opts[:send_timeout] || @send_timeout,
          receive_timeout: opts[:receive_timeout] || @receive_timeout,
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

    module InputMixin
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

      def setup_input(conf)
        @fetch_queue = []

        conf.elements.each do |e|
          next unless e.name == 'fetch'
          method = e['method']
          target = e['target']
          interval_str = e['interval']
          tag = e['tag']
          unless method && interval_str && tag
            raise Fluent::ConfigError, "<fetch> must be specified with method/interval/tag"
          end
          if method == 'event' and target.nil?
            raise Fluent::ConfigError, "<fetch> method 'event' requires 'target' for fetch target query name"
          end

          interval = Fluent::Config.time_value(interval_str)
          tag_type, tag_arg = tag.split(/ /, 2)
          req = FetchRequest.new(method, target, interval, tag_type, tag_arg, e['tag_prefix'])

          @fetch_queue << req
        end

        @fetch_queue_mutex = Mutex.new
      end

      def start_input
        @fetch_worker_running = true
        @fetch_thread = Thread.new(&method(:fetch_worker))
      end

      def stop_input
        @fetch_worker_running = false
      end

      def shutdown_input
        # @fetch_thread.kill
        @fetch_thread.join
      end

      def insert_fetch_queue(request)
        @fetch_queue_mutex.synchronize do
          request.next! if request.time < Time.now
          # if @fetch_queue.size > 0
          #   next_pos = @fetch_queue.bsearch{|req| req.time > request.time}
          #   @fetch_queue.insert(next_pos, request)
          # else
          #   @fetch_queue.push(request)
          # end
          @fetch_queue.push(request)
          @fetch_queue.sort!
        end
      rescue => e
        log.error "unknown log encountered", :error_class => e.class, :message => e.message
      end

      def fetch_worker
        while sleep(1)
          break unless @fetch_worker_running
          next unless fetchable?
          next if @fetch_queue.first.nil? || @fetch_queue.first.time > Time.now

          now = Time.now
          while @fetch_queue.first.time <= now
            req = @fetch_queue.shift

            begin
              data = req.fetch(client())
            rescue => e
              log.error "failed to fetch", :norikra => "#{@host}:#{@port}", :method => req.method, :target => req.target, :error => e.class, :message => e.message
            end

            if data
              data.each do |tag, event_array|
                next unless event_array
                event_array.each do |time,event|
                  begin
                    router.emit(tag, time, event)
                  rescue => e
                    log.error "failed to emit event from norikra query", :norikra => "#{@host}:#{@port}", :error => e.class, :message => e.message, :tag => tag, :record => event
                  end
                end
              end
            end

            insert_fetch_queue(req)
          end
        end
      end
    end
  end
end
