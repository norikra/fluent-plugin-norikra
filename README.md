# fluent-plugin-norikra

Fluentd plugins to send/receive events to/from Norikra server.

Norikra is an open source server software provides "Stream Processing" with SQL, written in JRuby, runs on JVM, licensed under GPLv2.
For more details, see: http://norikra.github.io/ .

fluent-plugin-norikra has 3 plugins: in\_norikra, out\_norikra and out\_norikra\_filter.
 * in\_norikra
   * fetch events of query results from Norikra server
 * out\_norikra
   * send events to Norikra server
 * out\_norikra\_filter
   * launch Norikra server as child process dynamically, as needed
   * use Norikra server as event filter (like out\_exec\_filter)
   * register/execute queries for targets newly incoming

# Setup

`fluent-plugin-norikra` works with Norikra server, on same server with Fluentd, or anywhere reachable over network from Fluentd.
For Norikra server setup, see: http://norikra.github.io/ .

NOTES:
 * Fluentd and fluent-plugin-norikra requires CRuby (MatzRuby).
 * Norikra requires JRuby.

To use out\_norikra\_filter with dynamic Norikra server launching, check actual path of command `norikra` under installed JRuby tree. (ex: `$HOME/.rbenv/versions/jruby-1.7.8/bin/norikra`)

To use this plugin:
  1. run `gem install fluent-plugin-norikra` or `fluent-gem install fluent-plugin-norikra` to install plugin
  1. edit configuration files
  1. execute fluentd

# Configuration

For variations, see `example` directory.

## NorikraOutput

Sends events to remote Norikra server. Minimal configurations are:
```apache
<match data.*>
  type    norikra
  norikra norikra.server.local:26571
  
  remove_tag_prefix data
  target_map_tag    true  # fluentd's tag 'data.event' -> norikra's target 'event'
</match>
```

NorikraOutput plugin opens Norikra's target for newly incoming tags. You can specify fields to include/exclude, and specify types of each fields, for each targets (and all targets by `default`). Definitions in `<target TARGET_NAME>` overwrites `<default>` specifications.
```apache
<match data.*>
  type    norikra
  norikra norikra.server.local:26571
  
  target_map_tag    true  # fluentd's tag -> norikra's target
  remove_tag_prefix data
  # other options:
  #   target_map_key KEY_NAME  # use specified key's value as target in fluentd event
  #   target_string  STRING    # use fixed target name specified
  
  <default>
    include *     # send all fields values to norikra
    exclude time  # exclude 'time' field from sending event values
       # AND/OR 'include_regexp' and 'exclude_regexp' available
    field_integer seq    # field 'seq' defined as integer for all targets
    escape_fieldname yes # Escape field name special chars (non alphabetical or numerical names) with underscore('_')
                         #  This is friendly for query access (ex: field.key1.cpu_total)
                         #  Default: no
  </default>
  
  <target users>
    field_string  name,address
    field_integer age
    field_float   height,weight
    field_boolean superuser
  </target>
</match>
```

With default setting, all fields are defined as 'string', so you must use `field_xxxx` parameters for numerical processing in query (For more details, see Norikra and Esper's documents).

If fluentd's events has so many variations of sets of fields, you can specify not to include fields automatically, with `auto_field` option:
```apache
<match data.*>
  type    norikra
  norikra norikra.server.local:26571
  
  target_map_tag    true  # fluentd's tag 'data.event' -> norikra's target 'event'
  remove_tag_prefix data
  
  <default>
    auto_field false  # norikra includes fields only used in queries.
  </default>
</match>
```

Fields which are referred in queries are automatically registered on norikra server in spite of `auto_field false`.

** NOTE: <default> and <target> sections in NorikraOutput ignores <query> sections. see NorikraFilterOutput **

## NorikraInput

Fetch events from Norikra server, and emits these into Fluentd itself. NorikraInput uses Norikra's API `event` (for queries), and `sweep` (for query groups).

Minimal configurations:
```apache
<source>
  type    norikra
  norikra norikra.server.local:26571
  <fetch>
    method     sweep
    # target QUERY_GROUP_NAME  # not specified => default query group
    tag        query_name
    tag_prefix norikra.query
    # other options:
    #  tag field FIELDNAME : tag by value with specified field name in output event
    #  tag string STRING   : fixed string specified
    interval 3s  # interval to call api
  </fetch>
</source>
```

Available `<fetch>` methods are `event` and `sweep`. `target` parameter is handled as query name for `event`, and as query group name for `sweep`.
```apache
<source>
  type    norikra
  norikra norikra.server.local:26571
  <fetch>
    method   event
    target   data_count_1hour
    tag      string data.count.1hour
    interval 60m
  </fetch>
  <fetch>
    method   event
    target   data_count_5min
    tag      string data.count.5min
    interval 5m
  </fetch>
  <fetch>
    method     sweep
    target     count_queries
    tag  field target_name
    tag_prefix data.count.all
    interval 15s
  </fetch>
</source>
```

## NorikraFilterOutput

NorikraFilterOutput has all features of both of NorikraInput and NorikraOutput, and also has additional features:
  * execute Norikra server
  * runs queries for newly incoming targets.

If you runs Norikra as standalone process, better configurations are to use NorikraInput and NorikraOutput separately. NorikraFilterOutput is for simple aggregations and filterings.

Configuration example to receive tags like `event.foo` and send norikra's target `foo`, and get count of its records per minute, and per hour with built-in Norikra server:
```apache
<match event.*>
  type    norikra_filter
  <server>
    path    /home/username/.rbenv/versions/jruby-1.7.4/bin/norikra
    # opts  -Xmx2g  # options of 'norikra start'
  </server>
  
  remove_tag_prefix event
  target_map_tag    yes
  
  <default>
    <query>
	  name       count_min_${target}
      group      count_query_group # or default when omitted
	  expression SELECT count(*) AS cnt FROM ${target}.win:time_batch(1 minute)
	  tag        count.min.${target}
	</query>
    <query>
	  name       count_hour_${target}
      group      count_query_group
	  expression SELECT count(*) AS cnt FROM ${target}.win:time_batch(1 hour)
	  tag        count.hour.${target}
	</query>
  </default>
</match>
```

Results of queries automatically registered by NorikraFilterOutput with `tag` parameter, will be fetched automatically by this plugin, and re-emitted into Fluentd itself.

Other all options are available as same as NorikraInput and NorikraOutput. `<default>`, `<target>` and `<fetch>` sections, `auto_field`, `include|exclude` and `field_xxxx` specifiers for targets and parameters for `<fetch>` sections.

### Input event data filtering

If you want send known fields only, specify `exclude *` and `include` or `include_regexp` like this:

    <default>
      exclude *
      include         path,status,method,bytes,rhost,referer,agent,duration
      include_pattern ^(query_|header_).*
      
      # ...
    </default>

Or you can specify to include as default, and exclude known some fields:

    <default>
      include *
      exclude         user_secret
      include_pattern ^(header_).*
      
      # ...
    </default>

NOTE: These configurations of `<target>` section overwrites of configurations in `<default>` section.

### Target mapping

Norikra's target (like table name) can be generated from:

 * tag
   * one target per one tag
   * `target_map_tag yes`
 * value of specified field
   * targets from values in specified field of record, dynamically
   * `target_map_key foo`
 * fixed string (in configuration file)
   * all records are sent in single target
   * `target_string from_fluentd`

# TODO

* write abou these topics
  * error logs for new target, success logs of retry

# Copyright

* Copyright (c) 2013- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, version 2.0

