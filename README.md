# fluent-plugin-norikra

Fluentd output plugin to send events to norikra server, and to fetch events (and re-send on fluentd network) from norikra server.

With NorikraOutput, we can:

 * execute Norikra server as built-in process dynamically
 * generate Norikra's target automatically with Fluentd's tags
 * register queries automatically with Fluentd's tags and messages
 * get all events on Norikra and emit on Fluentd network automatically

# Setup

At first, install JRuby and Norikra on your host if you are not using stand-alone Norikra servers.

1. install latest jruby
  * (rbenv) `rbenv install jruby-1.7.4`
  * (rvm) `rvm install jruby-1.7.4`
  * or other tools you want.
2. swith to jruby, and install Norikra
  * `gem install norikra`
3. check and note `jruby` and `norikra-server`
  * `which jruby`
  * `which norikra-server`
4. switch CRuby (with Fluentd), and install this plugin
  * `gem install fluent-plugin-norikra` (or use `fluent-gem`)
5. configure Fluentd, and execute.
  * and write `path` configuration of `<server>` section (if you want)

# Configuration

For variations, see `example` directory.

## NorikraOutput

With built-in Norikra server, to receive tags like `event.foo` and send norikra's target `foo`, and get count of its records per minute, and per hour.

    <match event.*>
      type norikra
      norikra localhost:26571 # this is default
      <server>
        execute yes
		jruby   /home/user/.rbenv/versions/jruby-1.7.4/bin/jruby
        path    /home/user/.rbenv/versions/jruby-1.7.4/bin/norikra
		opts    -Xmx2g
      </server>
      
      remove_tag_prefix event
      target_map_tag    yes
      
      <default>
	    <query>
		  name       count_min_${target}
		  expression SELECT count(*) AS cnt FROM ${target}.win:time_batch(1 minute)
		  tag        count.min.${target}
		</query>
	    <query>
		  name       count_hour_${target}
		  expression SELECT count(*) AS cnt FROM ${target}.win:time_batch(1 hour)
		  tag        count.hour.${target}
		</query>
      </default>
    </match>

With default setting, all fields are defined as 'string', so you must use `cast` for numerical processing in query (For more details, see Norikra and Esper's documents).

If you know some field's types of records, you can define types of these fields. This plugin will define field types before it send records into Norikra server.

    <match event.*>
      type norikra
      norikra localhost:26571 # this is default
      <server>
        execute yes
		jruby   /home/user/.rbenv/versions/jruby-1.7.4/bin/jruby
        path    /home/user/.rbenv/versions/jruby-1.7.4/bin/norikra
		opts    -Xmx2g
      </server>
      
      remove_tag_prefix event
      target_map_tag    yes
      
      <default>
        field_int    amount
        field_long   size
        field_double price
        
	    <query>
		  name       sales_${target}
		  expression SELECT price * amount AS  FROM ${target}.win:time_batch(1 minute) WHERE size > 0
		  tag        sales.min.${target}
		</query>
      </default>
    </match>

Additional field definitions and query registrations should be written in `<target TARGET_NAME>` sections.

    <default>
      ... # for all of access logs
    </default>
    <target login>
      field_string protocol # like 'oauth', 'openid', ...
	  field_int    proto_num # integer means internal id of protocols
	  <query>
	    name       protocol
		expression SELECT protocol, count(*) AS cnt FROM ${target}.win:time_batch(1 hour) WHERE proto_num != 0 GROUP BY protocol
		tag        login.counts
	  </query>
    </target>
    <target other_action>
	  ...
    </target>
	# ...

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

### Event sweeping

Norikra server accepts queries and events from everywhere other than Fluentd. This plugin can get events from these queries/events.

To gather all events of Norikra server, including queries from outside of Fluentd configurations, write `<event>` section.

    <events>
      method sweep
      tag    query_name
      # tag    field FIELDNAME
      # tag    string FIXED_STRING
      tag_prefix norikra.event     # actual tag: norikra.event.QUERYNAME
      sweep_interval 5s
    </events>

NOTE: 'sweep' get all events from Norikra, and other clients cannot get these events. Take care for other clients.

# FAQ

* TODO: write this section
  * `fetch_interval`
  * error logs for new target, success logs of retry

# TODO

* TODO: write this section

# Copyright

* Copyright (c) 2013- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, version 2.0

