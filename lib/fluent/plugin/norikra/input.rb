require_relative 'fetch_request'

module Fluent::NorikraPlugin
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
        unless method && target && interval_str && tag
          raise ArgumentError, "<fetch> must be specified with method/target/interval/tag"
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
      $log.error "unknown log encountered", :error_class => e.class, :message => e.message
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
            $log.error "failed to sweep", :norikra => "#{@host}:#{@port}", :error => e.class, :message => e.message
          end

          data.each do |tag, event_array|
            event_array.each do |time,event|
              begin
                Fluent::Engine.emit(tag, time, event)
              rescue => e
                $log.error "failed to emit event from norikra query", :norikra => "#{@host}:#{@port}", :error => e.class, :message => e.message, :tag => tag, :record => event
              end
            end
          end

          insert_fetch_queue(req)
        end
      end
    end
  end
end
