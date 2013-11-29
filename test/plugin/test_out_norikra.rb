require 'helper'

class NorikraFilterOutputTest < Test::Unit::TestCase
  CONF = %[
    target_map_tag yes
  ]

  def create_driver(conf=CONFIG, tag='test')
    Fluent::Test::OutputTestDriver.new(Fluent::NorikraFilterOutput, tag).configure(conf)
  end
end
