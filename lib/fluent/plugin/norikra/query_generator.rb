module Fluent::NorikraPlugin
  class QueryGenerator
    attr_reader :fetch_interval

    def initialize(name_template, group, expression_template, tag_template, opts={})
      @name_template = name_template || ''
      @group = group
      @expression_template = expression_template || ''
      @tag_template = tag_template || ''
      if @name_template.empty? || @expression_template.empty?
        raise Fluent::ConfigError, "query's name/expression must be specified"
      end
      @fetch_interval = case
                        when opts['fetch_interval']
                          Fluent::Config.time_value(opts['fetch_interval'])
                        when @expression_template =~ /\.win:time_batch\(([^\)]+)\)/
                          y,mon,w,d,h,m,s,msec = self.class.parse_time_period($1)
                          (h * 3600 + m * 60 + s) / 5
                        else
                          60
                        end
    end

    def generate(name, escaped)
      Fluent::NorikraPlugin::Query.new(
        self.class.replace_target(name, @name_template),
        @group,
        self.class.replace_target(escaped, @expression_template),
        self.class.replace_target(name, @tag_template),
        @fetch_interval
      )
    end

    def self.replace_target(t, str)
      str.gsub('${target}', t)
    end

    def self.parse_time_period(string)
      #### http://esper.codehaus.org/esper-4.9.0/doc/reference/en-US/html/epl_clauses.html#epl-syntax-time-periods
      # time-period : [year-part] [month-part] [week-part] [day-part] [hour-part] [minute-part] [seconds-part] [milliseconds-part]
      # year-part : (number|variable_name) ("years" | "year")
      # month-part : (number|variable_name) ("months" | "month")
      # week-part : (number|variable_name) ("weeks" | "week")
      # day-part : (number|variable_name) ("days" | "day")
      # hour-part : (number|variable_name) ("hours" | "hour")
      # minute-part : (number|variable_name) ("minutes" | "minute" | "min")
      # seconds-part : (number|variable_name) ("seconds" | "second" | "sec")
      # milliseconds-part : (number|variable_name) ("milliseconds" | "millisecond" | "msec")
      m = /^\s*(\d+ years?)? ?(\d+ months?)? ?(\d+ weeks?)? ?(\d+ days?)? ?(\d+ hours?)? ?(\d+ (?:min|minute|minutes))? ?(\d+ (?:sec|second|seconds))? ?(\d+ (?:msec|millisecond|milliseconds))?/.match(string)
      years = (m[1] || '').split(' ',2).first.to_i
      months = (m[2] || '').split(' ',2).first.to_i
      weeks = (m[3] || '').split(' ',2).first.to_i
      days = (m[4] || '').split(' ',2).first.to_i
      hours = (m[5] || '').split(' ',2).first.to_i
      minutes = (m[6] || '').split(' ',2).first.to_i
      seconds = (m[7] || '').split(' ',2).first.to_i
      msecs = (m[8] || '').split(' ',2).first.to_i
      return [years, months, weeks, days, hours, minutes, seconds, msecs]
    end
  end
end
