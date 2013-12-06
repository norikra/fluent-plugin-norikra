source {
  type :forward
}

match('test.*') {
  type :norikra
  norikra 'localhost:26571'

  remove_tag_prefix 'test'
  target_map_tag true

  default {
    include '*'
    exclude 'hhmmss'
  }

  target('data') {
    field_string 'name'
    field_integer 'age'
  }
}

source {
  type :norikra

  fetch {
    method :sweep
    # target => nil (group: default)
    tag 'field target'
    tag_prefix 'norikra.query'
    interval 3
  }

  fetch {
    method :event
    target 'data_count'
    tag 'string norikra.count.data'
    interval 5
  }
}

match('fluent.**') {
  type :null
}

match('**') {
  type :stdout
}
