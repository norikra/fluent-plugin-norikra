require 'fluent/plugin/input'
require_relative 'norikra/fetch_request'

require 'norikra-client'


module Fluent::Plugin
  class NorikraInput < Fluent::Plugin::Input
    Fluent::Plugin.register_input('norikra', self)

    helpers :timer

    config_param :norikra, :string, default: 'localhost:26571'

    config_param :connect_timeout, :integer, default: nil
    config_param :send_timeout, :integer, default: nil
    config_param :receive_timeout, :integer, default: nil

    config_section :fetch, param_name: :fetches do
      config_param :method, :enum, list: [:event, :sweep]
      config_param :target, :string, default: nil # nil is valid for :event
      config_param :interval, :time
      config_param :tag, :string
      config_param :tag_prefix, :string, default: nil
    end

    def configure(conf)
      super

      @host, @port = @norikra.split(':', 2)
      @port = @port.to_i

      @fetch_queue = []
      @fetches.each do |f|
        if f.method == 'event' && !f.target
          raise Fluent::ConfigError, 'target is not specified with "method event" in <fetch> section'
        end
        tag_type, tag_arg = f.tag.split(/\s+/, 2)
        @fetch_queue << FetchRequest.new(f.method, f.target, f.interval, tag_type, tag_arg, f.tag_prefix)
      end
      # sort to execute most recent request at first (using FetchRequest#<=>)
      @fetch_queue.sort!

      @fetch_queue_mutex = Mutex.new
      @client = Norikra::Client.new(@host, @port, connect_timeout: @connect_timeout, send_timeout: @send_timeout, receive_timeout: @receive_timeout)
    end

    def start
      super
      timer_execute(:in_norikra_worker, 1, &method(:fetch))
    end

    def insert_fetch_queue(request)
      @fetch_queue_mutex.synchronize do
        request.next! if request.time < Time.now
        if @fetch_queue.size > 0
          next_pos = @fetch_queue.bsearch{|req| req.time > request.time}
          @fetch_queue.insert(next_pos, request)
        else
          @fetch_queue.push(request)
        end
      end
    rescue => e
      log.error "unexpected error", error: e
    end

    def fetch
      if @fetch_queue.first.nil? || @fetch_queue.first.time > Time.now
        return
      end

      now = Time.now
      while @fetch_queue.first.time <= now
        req = @fetch_queue.shift

        begin
          data = req.fetch(@client)
        rescue => e
          log.error "failed to fetch", norikra: "#{@host}:#{@port}", method: req.method, target: req.target, error: e
        end

        if data
          data.each do |tag, event_array|
            next unless event_array
            begin
              router.emit_array(tag, event_array)
            rescue => e
              log.error "failed to emit event from norikra query", norikra: "#{@host}:#{@port}", tag: tag, error: e
            end
          end
        end

        insert_fetch_queue(req)
      end
    end
  end
end
