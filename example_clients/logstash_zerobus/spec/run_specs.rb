# Runs rspec for the plugin on top of the Logstash tarball's own jars and bundled gems,
# without Bundler (the tarball has no development gems, so bundler setup would fail).
# Usage, from the plugin folder: jruby spec/run_specs.rb [rspec args]
# Set LOGSTASH_HOME to the unpacked Logstash 9.5.6 folder (see the README Tests section).
ls_home = ENV.fetch("LOGSTASH_HOME", "/opt/logstash")

$LOAD_PATH.unshift(File.join(ls_home, "logstash-core-plugin-api", "lib"))
$LOAD_PATH.unshift(File.join(ls_home, "logstash-core", "lib"))
require "logstash-core"
require "logstash/environment"
require "rspec/core"
$LOAD_PATH.unshift(File.join(Dir.pwd, "lib"))
$LOAD_PATH.unshift(File.join(Dir.pwd, "spec"))
args = ARGV.empty? ? ["spec"] : ARGV
exit RSpec::Core::Runner.run(args)
