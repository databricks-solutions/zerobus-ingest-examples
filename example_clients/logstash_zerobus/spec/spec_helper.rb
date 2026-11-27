# encoding: utf-8
# Loaded by the test runner after Logstash's core (spec/run_specs.rb), so logstash-core is on the
# load path. The client files have no Logstash dependency.
$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
# Test-only codec stand-in, found before the (unactivatable) bundled logstash-codec-plain.
$LOAD_PATH.unshift(File.expand_path("support", __dir__))
require "logstash/outputs/zerobus/client"
