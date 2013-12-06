require 'helper'

class NorikraInputTest < Test::Unit::TestCase
  CONFIG = %[
  ]

  def create_driver(conf=CONFIG, tag='test')
    Fluent::Test::InputTestDriver.new(Fluent::NorikraInput).configure(conf)
  end

  def test_init
    create_driver
  end
end
