source {
  type "forward"
}

home_dir = ::Object::ENV['HOME']

match('event.*') {
  type "norikra_filter"
  norikra "localhost:26571"
  server {
    path "#{home_dir}/.rbenv/versions/jruby-1.7.8/bin/norikra"
  }
  remove_tag_prefix "event"
  target_map_tag true
}
