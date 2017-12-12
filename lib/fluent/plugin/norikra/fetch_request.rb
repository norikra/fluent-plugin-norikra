module Fluent::NorikraPlugin
  class FetchRequest
    TAG_TYPES = ['query_name', 'field', 'string']

    attr_accessor :method, :target, :interval, :tag_generator, :tag_prefix
    attr_accessor :time

    def initialize(method, target, interval, tag_type, tag_arg, tag_prefix)
      @method = method
      @target = target
      @interval = interval

      raise ArgumentError, "unknown tag type specifier '#{tag_type}'" unless TAG_TYPES.include?(tag_type.to_s)
      raw_tag_prefix = tag_prefix.to_s
      if (! raw_tag_prefix.empty?) && (! raw_tag_prefix.end_with?('.')) # tag_prefix specified, and ends without dot
        raw_tag_prefix += '.'
      end

      @tag_generator = case tag_type.to_s
                       when 'query_name' then lambda{|query_name,record| raw_tag_prefix + query_name}
                       when 'field'      then lambda{|query_name,record| raw_tag_prefix + (record[tag_arg] || 'NULL')}
                       when 'string'     then lambda{|query_name,record| raw_tag_prefix + tag_arg}
                       else
                         raise "BUG: unknown tag_type: #{tag_type}"
                       end
      @time = Time.now + 1 # should be fetched soon ( 1sec later )
    end

    def <=>(other)
      self.time <=> other.time
    end

    def next!
      @time = Time.now + @interval
    end

    # returns hash: { tag => [[time, record], ...], ... }
    def fetch(client)
      # events { query_name => [[time, record], ...], ... }
      events = case @method
               when :event then event(client)
               when :sweep then sweep(client)
               else
                 raise "BUG: unknown method: #{@method}"
               end

      output = {}

      events.keys.each do |query_name|
        events[query_name].each do |time, record|
          tag = @tag_generator.call(query_name, record)
          output[tag] ||= []
          output[tag] << [time, record]
        end
      end

      output
    end

    def event(client)
      events = client.event(@target) # [[time(int from epoch), event], ...]
      {@target => events}
    end

    def sweep(client)
      client.sweep(@target) # {query_name => event_array, ...}
    end
  end
end
