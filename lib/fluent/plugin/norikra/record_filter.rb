module Fluent::NorikraPlugin
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
end
