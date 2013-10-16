class Fluent::NorikraOutput
  class Query
    attr_accessor :name, :expression, :tag, :interval

    def initialize(name, expression, tag, interval)
      @name = name
      @expression = expression
      @tag = tag
      @interval = interval
    end
  end

  class QueryGenerator
    attr_reader :fetch_interval

    def initialize(name_template, expression_template, tag_template, opts={})
      @name_template = name_template || ''
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
      Fluent::NorikraOutput::Query.new(
        self.class.replace_target(name, @name_template),
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

  class RecordFilter
    attr_reader :default_policy, :include_fields, :include_regexp, :exclude_fields, :exclude_regexp

    def initialize(include='', include_regexp='', exclude='', exclude_regexp='')
      include ||= ''
      include_regexp ||= ''
      exclude ||= ''
      exclude_regexp ||= ''

      @default_policy = nil
      if include == '*' && exclude == '*'
        raise Fluent::ConfigError, "invalid configuration, both of 'include' and 'exclude' are '*'"
      end
      if include.empty? && include_regexp.empty? && exclude.empty? && exclude_regexp.empty? # assuming "include *"
        @default_policy = :include
      elsif exclude.empty? && exclude_regexp.empty? || exclude == '*' # assuming "exclude *"
        @default_policy = :exclude
      elsif include.empty? && include_regexp.empty? || include == '*' # assuming "include *"
        @default_policy = :include
      else
        raise Fluent::ConfigError, "unknown default policy. specify 'include *' or 'exclude *'"
      end

      @include_fields = nil
      @include_regexp = nil
      @exclude_fields = nil
      @exclude_regexp = nil

      if @default_policy == :exclude
        @include_fields = include.split(',')
        @include_regexp = Regexp.new(include_regexp) unless include_regexp.empty?
        if @include_fields.empty? && @include_regexp.nil?
          raise Fluent::ConfigError, "no one fields specified. specify 'include' or 'include_regexp'"
        end
      else
        @exclude_fields = exclude.split(',')
        @exclude_regexp = Regexp.new(exclude_regexp) unless exclude_regexp.empty?
      end
    end

    def filter(record)
      if @default_policy == :include
        if @exclude_fields.empty? && @exclude_regexp.nil?
          record
        else
          record = record.dup
          record.keys.each do |f|
            record.delete(f) if @exclude_fields.include?(f) || @exclude_regexp &&  @exclude_regexp.match(f)
          end
          record
        end
      else # default policy exclude
        data = {}
        record.keys.each do |f|
          data[f] = record[f] if @include_fields.include?(f) || @include_regexp && @include_regexp.match(f)
        end
        data
      end
    end
  end

  class ConfigSection
    attr_accessor :target, :target_matcher, :auto_field, :filter_params, :field_definitions, :query_generators

    def initialize(section)
      @target = nil
      @target_matcher = nil
      if section.name == 'default'
        # nil
      elsif section.name == 'target'
        # unescaped target name (tag style with dots)
        @target = section.arg
        @target_matcher = Fluent::GlobMatchPattern.new(section.arg)
      else
        raise ArgumentError, "invalid section for this class, #{section.name}: ConfigSection"
      end

      @auto_field = section['auto_field']

      @filter_params = {
        :include => section['include'],
        :include_regexp => section['include_regexp'],
        :exclude => section['exclude'],
        :exclude_regexp => section['exclude_regexp']
      }
      @field_definitions = {
        :string => (section['field_string'] || '').split(','),
        :boolean => (section['field_boolean'] || '').split(','),
        :int => (section['field_int'] || '').split(','),
        :long => (section['field_long'] || '').split(','),
        :float => (section['field_float'] || '').split(','),
        :double => (section['field_double'] || '').split(',')
      }
      @query_generators = []
      section.elements.each do |element|
        if element.name == 'query'
          opt = {}
          if element.has_key?('fetch_interval')
            opt['fetch_interval'] = element['fetch_interval'].to_i
          end
          @query_generators.push(QueryGenerator.new(element['name'], element['expression'], element['tag'], opt))
        end
      end
    end

    def +(other)
      if other.nil?
        other = self.class.new(Fluent::Config::Element.new('target', 'dummy', {}, []))
      end
      r = self.class.new(Fluent::Config::Element.new('target', (other.target ? other.target : self.target), {}, []))
      r.auto_field = (other.auto_field.nil? ? self.auto_field : other.auto_field)

      others_filter = {}
      other.filter_params.keys.each do |k|
        others_filter[k] = other.filter_params[k] if other.filter_params[k]
      end
      r.filter_params = self.filter_params.merge(others_filter)
      r.field_definitions = {
        :string => self.field_definitions[:string] + other.field_definitions[:string],
        :boolean => self.field_definitions[:boolean] + other.field_definitions[:boolean],
        :int => self.field_definitions[:int] + other.field_definitions[:int],
        :long => self.field_definitions[:long] + other.field_definitions[:long],
        :float => self.field_definitions[:float] + other.field_definitions[:float],
        :double => self.field_definitions[:double] + other.field_definitions[:double]
      }
      r.query_generators = self.query_generators + other.query_generators
      r
    end
  end

  class Target
    attr_accessor :name, :auto_field, :fields, :queries
    attr_reader :escaped_name

    def self.escape(src)
      if src.nil? || src.empty?
        return 'FluentdGenerated'
      end

      dst = src.gsub(/[^_a-zA-Z0-9]/, '_')
      unless dst =~ /^[a-zA-Z]([_a-zA-Z0-9]*[a-zA-Z0-9])?$/
        unless dst =~ /^[a-zA-Z]/
          dst = 'Fluentd' + dst
        end
        unless dst =~ /[a-zA-Z0-9]$/
          dst = dst + 'Generated'
        end
      end
      dst
    end

    def initialize(target, config)
      @name = target
      @escaped_name = self.class.escape(@name)
      @auto_field = config.auto_field.nil? ? true : config.auto_field

      @filter = RecordFilter.new(*([:include, :include_regexp, :exclude, :exclude_regexp].map{|s| config.filter_params[s]}))
      @fields = config.field_definitions
      @queries = config.query_generators.map{|g| g.generate(@name, @escaped_name)}
    end

    def filter(record)
      @filter.filter(record)
    end

    def reserve_fields
      f = {}
      @fields.keys.each do |type_sym|
        @fields[type_sym].each do |fieldname|
          f[fieldname] = type_sym.to_s
        end
      end
      f
    end
  end
end
