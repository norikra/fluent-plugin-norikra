module Fluent::NorikraPlugin
  class Query
    attr_accessor :name, :group, :expression, :tag, :interval

    def initialize(name, group, expression, tag, interval)
      @name = name
      @group = group
      @expression = expression
      @tag = tag
      @interval = interval
    end
  end
end
