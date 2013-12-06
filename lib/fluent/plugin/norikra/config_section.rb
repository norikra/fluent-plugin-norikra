module Fluent::NorikraPlugin
  class ConfigSection
    attr_accessor :target, :target_matcher, :auto_field, :filter_params, :field_definitions, :query_generators

    def initialize(section, enable_auto_query=true)
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

      @auto_field = Fluent::Config.bool_value(section['auto_field'])

      @filter_params = {
        :include => section['include'],
        :include_regexp => section['include_regexp'],
        :exclude => section['exclude'],
        :exclude_regexp => section['exclude_regexp']
      }
      @field_definitions = {
        :string => (section['field_string'] || '').split(','),
        :boolean => (section['field_boolean'] || '').split(','),
        :integer => (section['field_integer'] || '').split(','),
        :float => (section['field_float'] || '').split(','),
      }

      @query_generators = []
      section.elements.each do |element|
        if element.name == 'query' && enable_auto_query
          opt = {}
          if element.has_key?('fetch_interval')
            opt['fetch_interval'] = Fluent::Config.time_value(element['fetch_interval'])
          end
          @query_generators.push(QueryGenerator.new(element['name'], element['group'], element['expression'], element['tag'], opt))
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
        :integer => self.field_definitions[:integer] + other.field_definitions[:integer],
        :float => self.field_definitions[:float] + other.field_definitions[:float],
      }
      r.query_generators = self.query_generators + other.query_generators
      r
    end
  end
end
