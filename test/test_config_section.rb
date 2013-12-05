require 'helper'
require 'fluent/plugin/norikra/config_section'

class ConfigSectionTest < Test::Unit::TestCase
  def setup
    @this = Fluent::NorikraPlugin::ConfigSection
  end

  def test_init_default
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
    s1 = @this.new(c1)

    assert_nil s1.target
    assert_equal({:include => '*', :include_regexp => nil, :exclude => 'flag', :exclude_regexp => 'f_.*'}, s1.filter_params)
    assert_equal({
        :string => %w(s1 s2 s3), :boolean => %w(bool1 bool2), :int => %w(i1 i2 i3 i4), :long => %w(num1 num2),
        :float => %w(f1 f2), :double => %w(d)
      }, s1.field_definitions)
    assert_equal 2, s1.query_generators.size
    assert_equal (10 * 60 / 5), s1.query_generators.map(&:fetch_interval).sort.first
  end

  def test_init_target
    q3 = Fluent::Config::Element.new('query', nil, {
        'name' => 'q3_test2',
        'expression' => 'SELECT * FROM ${target}.win:time_batch(30 min) WHERE q3="/"',
        'tag' => 'q3.test2'
      }, [])
    c2 = Fluent::Config::Element.new('target', 'test2', {
        'exclude_regexp' => '(f|g)_.*',
        'field_double' => 'd1,d2,d3,d4'
      }, [q3])
    s2 = @this.new(c2)

    assert_equal 'test2', s2.target
    assert_equal({:include => nil, :include_regexp => nil, :exclude => nil, :exclude_regexp => '(f|g)_.*'}, s2.filter_params)
    assert_equal({:string => [], :boolean => [], :int => [], :long => [], :float => [], :double => %w(d1 d2 d3 d4)}, s2.field_definitions)
    assert_equal 1, s2.query_generators.size
    assert_equal (30 * 60 / 5), s2.query_generators.map(&:fetch_interval).sort.first
  end

  def test_init_target_query_only
    q4 = Fluent::Config::Element.new('query', nil, {
        'name' => 'q4_test3',
        'expression' => 'SELECT * FROM ${target}.win:time_batch(30 min) WHERE q4 > 10',
        'tag' => 'q4.test3',
        'fetch_interval' => '1s'
      }, [])
    c3 = Fluent::Config::Element.new('target', 'test3', {}, [q4])
    s3 = @this.new(c3)

    assert_equal 'test3', s3.target
    assert_equal({:include => nil, :include_regexp => nil, :exclude => nil, :exclude_regexp => nil}, s3.filter_params)
    assert_equal({:string => [], :boolean => [], :int => [], :long => [], :float => [], :double => []}, s3.field_definitions)
    assert_equal 1, s3.query_generators.size
  end

  def test_init_target_without_query
    c4 = Fluent::Config::Element.new('target', 'test4', {
        'field_int' => 'status'
      }, [])
    s4 = @this.new(c4)

    assert_equal 'test4', s4.target
    assert_equal({:include => nil, :include_regexp => nil, :exclude => nil, :exclude_regexp => nil}, s4.filter_params)
    assert_equal({:string => [], :boolean => [], :int => ['status'], :long => [], :float => [], :double => []}, s4.field_definitions)
    assert_equal 0, s4.query_generators.size
  end

  def test_init_target_blank
    c5 = Fluent::Config::Element.new('target', 'test5', {}, [])
    s5 = @this.new(c5)

    assert_equal 'test5', s5.target
    assert_equal({:include => nil, :include_regexp => nil, :exclude => nil, :exclude_regexp => nil}, s5.filter_params)
    assert_equal({:string => [], :boolean => [], :int => [], :long => [], :float => [], :double => []}, s5.field_definitions)
    assert_equal 0, s5.query_generators.size
  end

  def test_join
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
    s1 = @this.new(c1)

    q3 = Fluent::Config::Element.new('query', nil, {
        'name' => 'q3_test',
        'expression' => 'SELECT * FROM ${target}.win:time_batch(30 min) WHERE q3="/"',
        'tag' => 'q3.test'
      }, [])
    c2 = Fluent::Config::Element.new('target', 'test', {
        'exclude_regexp' => '(f|g)_.*',
        'field_double' => 'd1,d2,d3,d4'
      }, [q3])
    s2 = @this.new(c2)

    s = s1 + s2

    assert_equal 'test', s.target
    assert_equal({:include => '*', :include_regexp => nil, :exclude => 'flag', :exclude_regexp => '(f|g)_.*'}, s.filter_params)
    assert_equal({
        :string => %w(s1 s2 s3), :boolean => %w(bool1 bool2), :int => %w(i1 i2 i3 i4), :long => %w(num1 num2),
        :float => %w(f1 f2), :double => %w(d d1 d2 d3 d4)
      }, s.field_definitions)
    assert_equal 3, s.query_generators.size
    assert_equal (10 * 60 / 5), s.query_generators.map(&:fetch_interval).sort.first
  end

  def test_join_with_nil
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
    s1 = @this.new(c1)

    s = s1 + nil

    assert_equal 'dummy', s.target
    assert_equal s1.filter_params, s.filter_params
    assert_equal s1.field_definitions, s.field_definitions
    assert_equal s1.query_generators.size, s.query_generators.size
    assert_equal s1.query_generators.map(&:fetch_interval).sort.first, s.query_generators.map(&:fetch_interval).sort.first
  end
end
