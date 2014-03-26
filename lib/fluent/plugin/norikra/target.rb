module Fluent::NorikraPlugin
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
      @escape_fieldname = config.escape_fieldname

      @filter = RecordFilter.new(*([:include, :include_regexp, :exclude, :exclude_regexp].map{|s| config.filter_params[s]}))
      @fields = config.field_definitions
      @queries = config.query_generators.map{|g| g.generate(@name, @escaped_name)}
    end

    def filter(record)
      r = @filter.filter(record)
      if @escape_fieldname
        escape_recursive(r)
      else
        r
      end
    end

    def escape_recursive(record)
      return record unless record.is_a?(Hash) || record.is_a?(Array)
      return record.map{|v| escape_recursive(v) } if record.is_a?(Array)

      # Hash
      r = {}
      record.keys.each do |key|
        k = if key =~ /[^$_a-zA-Z0-9]/
              key.gsub(/[^$_a-zA-Z0-9]/, '_')
            else
              key
            end
        v = escape_recursive(record[key])
        r[k] = v
      end
      r
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
