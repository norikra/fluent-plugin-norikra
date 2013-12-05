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
