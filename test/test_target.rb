require 'helper'
require 'fluent/plugin/norikra_target'

class TargetTest < Test::Unit::TestCase
  def setup
    @this = Fluent::NorikraOutput::Target
  end

  def test_target_name_escape
    assert_equal 'target1', @this.escape('target1')
    assert_equal 'target1_subtarget1', @this.escape('target1.subtarget1')
    assert_equal 'test_tag_foo', @this.escape('test.tag.foo')

    assert_equal 'FluentdGenerated', @this.escape('')
    assert_equal 'Fluentd_Generated', @this.escape(':')
    assert_equal 'a', @this.escape('a')
    assert_equal 'Fluentd_a', @this.escape('_a')
    assert_equal 'a_Generated', @this.escape('a_')
  end

  Q1 = Fluent::Config::Element.new('query', nil, {
      'name' => 'q1_${target}',
      'expression' => 'SELECT * FROM ${target}.win:time_batch(10 min) WHERE q1',
      'tag' => 'q1.${target}'
    }, [])
  Q2 = Fluent::Config::Element.new('query', nil, {
      'name' => 'q2_${target}',
      'expression' => 'SELECT * FROM ${target}.win:time_batch(50 min) WHERE q2.length() > 0',
      'tag' => 'q2.${target}'
    }, [])
  C1 = Fluent::Config::Element.new('default', nil, {
      'include' => '*',
      'exclude' => 'flag',
      'exclude_regexp' => 'f_.*',
      'field_string' => 's1,s2,s3',
      'field_boolean' => 'bool1,bool2',
      'field_int' => 'i1,i2,i3,i4',
      'field_long' => 'num1,num2',
      'field_float' => 'f1,f2',
      'field_double' => 'd'
    }, [Q1,Q2])
  S1 = Fluent::NorikraOutput::ConfigSection.new(C1)

  Q3 = Fluent::Config::Element.new('query', nil, {
      'name' => 'q3_test',
      'expression' => 'SELECT * FROM ${target}.win:time_batch(30 min) WHERE q3="/"',
      'tag' => 'q3.test'
    }, [])
  C2 = Fluent::Config::Element.new('target', 'test', {
      'exclude_regexp' => '(f|g)_.*',
      'field_double' => 'd1,d2,d3,d4'
    }, [Q3])
  S2 = Fluent::NorikraOutput::ConfigSection.new(C2)

  def test_instanciate
    t = @this.new('test', S1 + S2)

    assert_equal 'test', t.name
    assert_equal({
        :string => %w(s1 s2 s3), :boolean => %w(bool1 bool2), :int => %w(i1 i2 i3 i4), :long => %w(num1 num2),
        :float => %w(f1 f2), :double => %w(d d1 d2 d3 d4)
      }, t.fields)
    assert_equal 3, t.queries.size

    r = t.filter({'x'=>1,'y'=>'y','z'=>'zett','flag'=>true,'f_x'=>'true','g_1'=>'g'})
    assert_equal 3, r.size
    assert_equal({'x'=>1,'y'=>'y','z'=>'zett'}, r)

    # reserve_fields
    assert_equal({
        's1' => 'string', 's2' => 'string', 's3' => 'string',
        'bool1' => 'boolean', 'bool2' => 'boolean',
        'i1' => 'int', 'i2' => 'int', 'i3' => 'int', 'i4' => 'int', 'num1' => 'long', 'num2' => 'long',
        'f1' => 'float', 'f2' => 'float',
        'd' => 'double', 'd1' => 'double', 'd2' => 'double', 'd3' => 'double', 'd4' => 'double'
      }, t.reserve_fields)
  end

  def test_queries
    t = @this.new('test.service', S1 + S2)

    assert_equal 3, t.queries.size

    assert_equal 'q1_test.service', t.queries[0].name
    assert_equal 'SELECT * FROM test_service.win:time_batch(10 min) WHERE q1', t.queries[0].expression
    assert_equal 'q1.test.service', t.queries[0].tag

    assert_equal 'q2_test.service', t.queries[1].name
    assert_equal 'SELECT * FROM test_service.win:time_batch(50 min) WHERE q2.length() > 0', t.queries[1].expression
    assert_equal 'q2.test.service', t.queries[1].tag

    assert_equal 'q3_test', t.queries[2].name
    assert_equal 'SELECT * FROM test_service.win:time_batch(30 min) WHERE q3="/"', t.queries[2].expression
    assert_equal 'q3.test', t.queries[2].tag
  end
end
