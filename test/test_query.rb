require 'helper'
require 'fluent/plugin/norikra/query'

class QueryTest < Test::Unit::TestCase
  def test_init
    q = Fluent::NorikraPlugin::Query.new('name', nil, 'expression', 'tag', 10)
    assert_equal 'name', q.name
    assert_nil q.group
    assert_equal 'expression', q.expression
    assert_equal 'tag', q.tag
    assert_equal 10, q.interval

    q = Fluent::NorikraPlugin::Query.new('name', 'group', 'expression', 'tag', 10)
    assert_equal 'group', q.group
  end
end
