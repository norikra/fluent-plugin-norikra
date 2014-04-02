require 'helper'
require 'fluent/plugin/norikra/target'

class TargetTest < Test::Unit::TestCase
  def setup
    @this = Fluent::NorikraPlugin::Target
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
      'field_integer' => 'i1,i2,i3,i4,num1,num2',
      'field_float' => 'f1,f2,d',
    }, [Q1,Q2])
  S1 = Fluent::NorikraPlugin::ConfigSection.new(C1)

  Q3 = Fluent::Config::Element.new('query', nil, {
      'name' => 'q3_test',
      'expression' => 'SELECT * FROM ${target}.win:time_batch(30 min) WHERE q3="/"',
      'tag' => 'q3.test'
    }, [])
  C2 = Fluent::Config::Element.new('target', 'test', {
      'time_key' => 'timestamp',
      'exclude_regexp' => '(f|g)_.*',
      'field_float' => 'd1,d2,d3,d4'
    }, [Q3])
  S2 = Fluent::NorikraPlugin::ConfigSection.new(C2)

  def test_instanciate
    t = @this.new('test', S1 + S2)

    assert_equal 'test', t.name
    assert_equal({
        :string => %w(s1 s2 s3), :boolean => %w(bool1 bool2), :integer => %w(i1 i2 i3 i4 num1 num2 timestamp),
        :float => %w(f1 f2 d d1 d2 d3 d4)
      }, t.fields)
    assert_equal 3, t.queries.size

    now = Time.now.to_i

    r = t.filter(now, {'x'=>1,'y'=>'y','z'=>'zett','flag'=>true,'f_x'=>'true','g_1'=>'g'})
    assert_equal 4, r.size
    assert_equal({'x'=>1,'y'=>'y','z'=>'zett','timestamp'=>(now*1000)}, r)

    # reserve_fields
    assert_equal({
        's1' => 'string', 's2' => 'string', 's3' => 'string',
        'bool1' => 'boolean', 'bool2' => 'boolean',
        'i1' => 'integer', 'i2' => 'integer', 'i3' => 'integer', 'i4' => 'integer', 'num1' => 'integer', 'num2' => 'integer',
        'f1' => 'float', 'f2' => 'float',
        'd' => 'float', 'd1' => 'float', 'd2' => 'float', 'd3' => 'float', 'd4' => 'float',
        'timestamp' => 'integer', # time_key
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

  C3 = Fluent::Config::Element.new('target', 'test', {
      'escape_fieldname' => 'no',
    }, [])
  S3 = Fluent::NorikraPlugin::ConfigSection.new(C3)
  C4 = Fluent::Config::Element.new('target', 'test', {
      'escape_fieldname' => 'yes',
    }, [])
  S4 = Fluent::NorikraPlugin::ConfigSection.new(C4)

  def test_escape_fieldname
    now = Time.now.to_i

    t = @this.new('test.service', S3)
    r = t.filter(now, {'a 1' => '1', 'b 2' => 2, 'c-1' => { 'd/1' => '1', 'd 2' => '2' }, 'f' => [1, 2, {'g+1' => 3}] })
    assert_equal '1', r['a 1']
    assert_equal 2,   r['b 2']
    assert_equal '1', r['c-1']['d/1']
    assert_equal '2', r['c-1']['d 2']
    assert_equal 1,   r['f'][0]
    assert_equal 2,   r['f'][1]
    assert_equal 3,   r['f'][2]['g+1']

    assert_nil r['a_1']
    assert_nil r['b_2']
    assert_nil r['c_1']
    assert_nil r['f'][2]['g_1']

    t = @this.new('test.service', S4)
    r = t.filter(now, {'a 1' => '1', 'b 2' => 2, 'c-1' => { 'd/1' => '1', 'd 2' => '2' }, 'f' => [1, 2, {'g+1' => 3}] })
    assert_nil r['a 1']
    assert_nil r['b 2']
    assert_nil r['c-1']
    assert_equal 1, r['f'][0]
    assert_equal 2, r['f'][1]
    assert_nil r['f'][2]['g+1']

    assert_equal '1', r['a_1']
    assert_equal 2,   r['b_2']
    assert_equal '1', r['c_1']['d_1']
    assert_equal '2', r['c_1']['d_2']
    assert_equal 3,   r['f'][2]['g_1']
  end
end
