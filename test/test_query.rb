require 'helper'
require 'fluent/plugin/norikra_target'

class QueryTest < Test::Unit::TestCase
  def test_init
    q = Fluent::NorikraOutput::Query.new('name', 'expression', 'tag')
    assert_equal 'name', q.name
    assert_equal 'expression', q.expression
    assert_equal 'tag', q.tag
  end
end
