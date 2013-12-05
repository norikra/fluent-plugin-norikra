module Fluent::NorikraPlugin
  class Query
    attr_accessor :name, :expression, :tag, :interval

    def initialize(name, expression, tag, interval)
      @name = name
      @expression = expression
      @tag = tag
      @interval = interval
    end
  end
end
