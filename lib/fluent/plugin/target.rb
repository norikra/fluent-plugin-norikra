class Fluent::NorikraOutput
  class Query
    attr_accessor :name, :expression, :tag

    def initialize(name, expression, tag)
      @name = name
      @expression = expression
      @tag = tag
    end
  end

  class QueryGenerator
    def initialize(name_template, expression_template, tag_template, opts={})
      @name_template = name_template
      @expression_template = expression_template
      @tag_template = tag_template
      @fetch_interval = case
                        when opts['fetch_interval']
                          Fluent::Config.time_value(opts['fetch_interval'])
                        when @expression_template =~ /\.win:time_batch\(([^\)])\)/
                          y,mon,w,d,h,m,s,msec = self.class.parse_time_period($1)
                          (h * 3600 + m * 60 + s) / 5
                        else
                          60
                        end
    end

    def generate(target)
      Fluent::NorikraOutput::Query.new(
        self.class.replace_target(target, @name_template),
        self.class.replace_target(target, @expression_template),
        self.class.replace_target(target, @tag_template)
      )
    end

    def self.replace_target(target, str)
      str.gsub('${target}', target)
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
      string =~ /^(\d+ years?)? ?(\d+ months?)? ?(\d+ weeks?)? ?(\d+ days?)? ?(\d+ hours?)? ?(\d+ (?:min|minute|minutes))? ?(\d+ (?:sec|second|seconds))? ?(\d+ (?:msec|millisecond|milliseconds))?/
      years = ($1 || '').split(/ /,2).first.to_i
      months = ($2 || '').split(/ /,2).first.to_i
      weeks = ($3 || '').split(/ /,2).first.to_i
      days = ($4 || '').split(/ /,2).first.to_i
      hours = ($5 || '').split(/ /,2).first.to_i
      minutes = ($6 || 0).split(/ /,2).first.to_i
      seconds = ($7 || 0).split(/ /,2).first.to_i
      msecs = ($8 || 0).split(/ /,2).first.to_i
      return [years, months, weeks, days, hours, minutes, seconds, msecs]
    end
  end

  class RecordFilter
    def initialize(include, include_regexp, exclude, exclude_regexp)
      @default_policy = nil
      if include = '*' && exclude = '*'
        raise Fluent::ConfigError, "invalid configuration, both of 'include' and 'exclude' are '*'"
      end
      if include.nil? && include_regexp.nil? && exclude.nil? && exclude_regexp.nil? # assuming "include *"
        @default_policy = :include
      elsif include.nil? && include_regexp.nil? || include = '*' # assuming "include *"
        @default_policy = :include
      elsif exclude.nil? && exclude_regexp.nil? || exclude = '*' # assuming "exclude *"
        @default_policy = :exclude
      else
        raise Fluent::ConfigError, "unknown default policy. specify 'include *' or 'exclude *'"
      end
      @include_fields = nil
      @include_regexp = nil
      @exclude_fields = nil
      @exclude_regexp = nil
      if @default_policy == :exclude
        @include_fields = include.split(',') if include
        @include_regexp = Regexp.new(include_regexp) if include_regexp
        if @include_fields.nil? && @include_regexp.nil?
          raise Fluent::ConfigError, "no one fields specified. specify 'include' or 'include_regexp'"
        end
      else
        @exclude_fields = exclude_fields.split(',')
        @exclude_regexp = Regexp.new(exclude_regexp)
      end
    end

    def filter(record)
      if @default_policy == :include
        if @exclude_fields.nil? && @exclude_regexp.nil?
          record
        else
          record = record.dup
          record.keys.each do |f|
            record.delete(f) if @exclude_fields.include?(f) || @exclude_regexp.match(f)
          end
          record
        end
      else # default exclude
        data = {}
        record.keys.each do |f|
          data[f] = record[f] if @include_fields.include?(f) || @include_regexp.match(f)
        end
        data
      end
    end
  end

  class ConfigSection
    attr_accessor :filter_params, :field_definitions, :query_generators

    def initialize(section)
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
          @query_generators.push(QueryGenerator.new(element['name'], element['expression'], element['tag']))
        end
      end
    end

    def +(other)
      return self unless other

      r = self.class.new({})
      r.filter_params = self.filter_params.merge(other.filter_params)
      r.field_definitions = {
        :string => self.field_definitions[:string] + other.field_definitions[:string],
        :boolean => self.field_definitions[:boolean] + other.field_definitions[:boolean],
        :int => self.field_definitions[:int] + other.field_definitions[:int],
        :long => self.field_definitions[:long] + other.field_definitions[:long],
        :float => self.field_definitions[:float] + other.field_definitions[:float],
        :double => self.field_definitions[:double] + other.field_definitions[:double]
      }
      r.query_generators = self.query_generators + other.query_generators
    end
  end

  class Target
    attr_accessor :target, :filter, :fields, :queries

    def initialize(target, config)
      @target = target
      @filter = RecordFilter.new(*([:include, :include_regexp, :exclude, :exclude_regexp].map{|s| config.filter_params[s]}))
      @fields = config.field_definitions
      @queries = config.query_generators.map{|g| g.generate(target)}
    end

    def convert(record)
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
