source {
  type :forward
}

home_dir = ::Object::ENV['HOME']

match('test.*') {
  type :norikra_filter
  norikra 'localhost:26571'
  server {
    path "#{home_dir}/.rbenv/versions/jruby-1.7.8/bin/norikra"
  }

  remove_tag_prefix 'test'
  target_map_tag true

  default {
    query {
      name "count_${target}"
      expression "SELECT '${target}' as target,count(*) AS cnt FROM ${target}.win:time_batch(30 sec)"
      group "testing"
      tag "count.x.${target}"
    }
  }

  fetch {
    method :sweep
    tag 'field target'
    tag_prefix 'count'
    interval 5
  }
}

match('fluent.*') {
  type :null
}

match('**') {
  type :stdout
}
