require 'helper'

class NorikraOutputTest < Test::Unit::TestCase
  CONFIG = %[
    target_map_tag yes
  ]

  def create_driver(conf=CONFIG, tag='test')
    Fluent::Test::OutputTestDriver.new(Fluent::NorikraOutput, tag).configure(conf)
  end

  def test_init
    create_driver
  end
end
