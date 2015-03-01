require 'helper'
require 'fluent/plugin/norikra/record_filter'

class RecordFilterTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @this = Fluent::NorikraPlugin::RecordFilter
  end

  def test_errors
    assert_raise(Fluent::ConfigError){ @this.new('*', '', '*', '') }
    assert_raise(Fluent::ConfigError){ @this.new('x', 'y', 'z', 'p') }
    assert_raise(Fluent::ConfigError){ @this.new('','', '*','') }
  end

  def test_init
    f = @this.new()
    assert_equal :include, f.default_policy
    assert_equal [], f.exclude_fields
    assert_nil f.exclude_regexp
  end

  def test_filter_default # return record itself
    f = @this.new(nil,nil,nil,nil)
    r = {'x'=>1,'y'=>2,'z'=>'3'}
    assert_equal r, f.filter(r)
    assert_equal r.object_id, f.filter(r).object_id

    f = @this.new('','','','')
    r = {'x'=>1,'y'=>2,'z'=>'3'}
    assert_equal r, f.filter(r)
    assert_equal r.object_id, f.filter(r).object_id
  end

  def test_filter_exclude_keys
    f = @this.new('*',nil,'x,y,z')
    r = {'a'=>'1','b'=>'2','c'=>'3','x'=>1,'y'=>2,'z'=>3}
    assert_equal 3, f.filter(r).size
    assert_equal({'a'=>'1','b'=>'2','c'=>'3'}, f.filter(r))
    assert_equal 6, r.size # check original record not to be broken
  end

  def test_filter_exclude_regexp
    f = @this.new('*',nil,nil,'f_.*')
    r = {'a'=>'1','b'=>'2','c'=>'3','f_x'=>1,'f_y'=>2,'f_z'=>3}
    assert_equal 3, f.filter(r).size
    assert_equal({'a'=>'1','b'=>'2','c'=>'3'}, f.filter(r))
    assert_equal 6, r.size # check original record not to be broken
  end

  def test_filter_excludes
    f = @this.new('*',nil,'b,c','f_.*')
    r = {'a'=>'1','b'=>'2','c'=>'3','f_x'=>1,'f_y'=>2,'f_z'=>3}
    assert_equal 1, f.filter(r).size
    assert_equal({'a'=>'1'}, f.filter(r))
    assert_equal 6, r.size # check original record not to be broken
  end

  def test_filter_include_keys
    f = @this.new('a,b,c',nil,'*','')
    r = {'a'=>'1','b'=>'2','c'=>'3','x'=>1,'y'=>2,'z'=>3}
    assert_equal 3, f.filter(r).size
    assert_equal({'a'=>'1','b'=>'2','c'=>'3'}, f.filter(r))
    assert_equal 6, r.size # check original record not to be broken
  end

  def test_filter_include_regexp
    f = @this.new('','f_','*','')
    r = {'f_a'=>'1','f_b'=>'2','f_c'=>'3','x'=>1,'y'=>2,'z'=>3}
    assert_equal 3, f.filter(r).size
    assert_equal({'f_a'=>'1','f_b'=>'2','f_c'=>'3'}, f.filter(r))
    assert_equal 6, r.size # check original record not to be broken
  end

  def test_filter_includes
    f = @this.new('y,z','f_','*','')
    r = {'f_a'=>'1','f_b'=>'2','f_c'=>'3','x'=>1,'y'=>2,'z'=>3}
    assert_equal 5, f.filter(r).size
    assert_equal({'f_a'=>'1','f_b'=>'2','f_c'=>'3','y'=>2,'z'=>3}, f.filter(r))
    assert_equal 6, r.size # check original record not to be broken
  end
end
