require 'helper'
require 'fluent/plugin/norikra_target'

class TargetTest < Test::Unit::TestCase
  def setup
    @this = Fluent::NorikraOutput::Target
  end

  def test_instanciate
        q1 = Fluent::Config::Element.new('query', nil, {
        'name' => 'q1_${target}',
        'expression' => 'SELECT * FROM ${target}.win:time_batch(10 min) WHERE q1',
        'tag' => 'q1.${target}'
      }, [])
    q2 = Fluent::Config::Element.new('query', nil, {
        'name' => 'q2_${target}',
        'expression' => 'SELECT * FROM ${target}.win:time_batch(50 min) WHERE q2.length() > 0',
        'tag' => 'q2.${target}'
      }, [])
    c1 = Fluent::Config::Element.new('default', nil, {
        'include' => '*',
        'exclude' => 'flag',
        'exclude_regexp' => 'f_.*',
        'field_string' => 's1,s2,s3',
        'field_boolean' => 'bool1,bool2',
        'field_int' => 'i1,i2,i3,i4',
        'field_long' => 'num1,num2',
        'field_float' => 'f1,f2',
        'field_double' => 'd'
      }, [q1,q2])
    s1 = Fluent::NorikraOutput::ConfigSection.new(c1)

    q3 = Fluent::Config::Element.new('query', nil, {
        'name' => 'q3_test',
        'expression' => 'SELECT * FROM ${target}.win:time_batch(30 min) WHERE q3="/"',
        'tag' => 'q3.test'
      }, [])
    c2 = Fluent::Config::Element.new('target', 'test', {
        'exclude_regexp' => '(f|g)_.*',
        'field_double' => 'd1,d2,d3,d4'
      }, [q3])
    s2 = Fluent::NorikraOutput::ConfigSection.new(c2)

    t = @this.new('test', s1 + s2)

    assert_equal 'test', t.target
    assert_equal({
        :string => %w(s1 s2 s3), :boolean => %w(bool1 bool2), :int => %w(i1 i2 i3 i4), :long => %w(num1 num2),
        :float => %w(f1 f2), :double => %w(d d1 d2 d3 d4)
      }, t.fields)
    assert_equal 3, t.queries.size

    r = t.filter.filter({'x'=>1,'y'=>'y','z'=>'zett','flag'=>true,'f_x'=>'true','g_1'=>'g'})
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
end
