require 'helper'
require 'fluent/plugin/norikra/query_generator'

class QueryGeneratorTest < Test::Unit::TestCase
  def setup
    @this = Fluent::NorikraPlugin::QueryGenerator
  end

  def test_replace_target
    expected = 'SELECT * FROM replaced.win:time_batch(10 hours) WHERE x=1'
    assert_equal expected, @this.replace_target('replaced', 'SELECT * FROM ${target}.win:time_batch(10 hours) WHERE x=1')
  end

  def test_parse_time_period
    assert_equal [0,0,0,0,0,0,0,0], @this.parse_time_period('')

    assert_equal [10,0,0,0,0,0,0,0], @this.parse_time_period(' 10 year')
    assert_equal [0,11,0,0,0,0,0,0], @this.parse_time_period('11 month')
    assert_equal [0,233,0,0,0,0,0,0], @this.parse_time_period('233 months')
    assert_equal [0,0,10,0,0,0,0,0], @this.parse_time_period('10 weeks')
    assert_equal [0,0,0,1,0,0,0,0], @this.parse_time_period('1 day')
    assert_equal [0,0,0,201,0,0,0,0], @this.parse_time_period('201 days')
    assert_equal [0,0,0,0,1,0,0,0], @this.parse_time_period('1 hour')
    assert_equal [0,0,0,0,11,0,0,0], @this.parse_time_period('11 hours')
    assert_equal [0,0,0,0,0,2,0,0], @this.parse_time_period('2 minutes')
    assert_equal [0,0,0,0,0,1,0,0], @this.parse_time_period('1 min')
    assert_equal [0,0,0,0,0,133,0,0], @this.parse_time_period('133 minute')
    assert_equal [0,0,0,0,0,0,12,0], @this.parse_time_period('12 sec')
    assert_equal [0,0,0,0,0,0,1,0], @this.parse_time_period('1 second')
    assert_equal [0,0,0,0,0,0,256,0], @this.parse_time_period(' 256 seconds')
    assert_equal [0,0,0,0,0,0,0,1], @this.parse_time_period('1 msec')
    assert_equal [0,0,0,0,0,0,0,111], @this.parse_time_period('111 milliseconds')

    assert_equal [1,12,4,365,23,59,60,0], @this.parse_time_period('1 year 12 months 4 weeks 365 days 23 hours 59 min 60 seconds')
  end

  def test_generate
    g = @this.new('query_${target}', 'SELECT * FROM ${target}.win:time_batch( 10 min ) WHERE x=1', 'tag.${target}')
    q = g.generate('test', 'test')
    assert_equal 'query_test', q.name
    assert_equal 'SELECT * FROM test.win:time_batch( 10 min ) WHERE x=1', q.expression
    assert_equal 'tag.test', q.tag
  end

  def test_fetch_interval
    g = @this.new('query_${target}', 'SELECT * FROM ${target}.win:time_batch( 12 min ) WHERE x=1', 'tag.${target}')
    assert_equal (12*60/5), g.fetch_interval
  end
end
