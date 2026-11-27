# encoding: utf-8
Gem::Specification.new do |s|
  s.name          = "logstash-output-zerobus"
  s.version       = "0.1.0"
  s.licenses      = ["Apache-2.0"]
  s.summary       = "Logstash output plugin for Zerobus Ingest (REST), one table per output"
  s.description   = "Writes Logstash events to one Databricks Zerobus Ingest table through the REST insert endpoint, with a table-scoped OAuth client-credentials token."
  s.authors       = ["Databricks"]
  s.homepage      = "https://github.com/databricks-solutions/zerobus-ingest-examples"
  s.require_paths = ["lib"]

  s.files = Dir["lib/**/*", "spec/**/*", "*.gemspec", "Gemfile", "README.md", "LICENSE*"]
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Only the Logstash plugin API at runtime. Logstash 9.5.6 bundles 2.1.16.
  s.add_runtime_dependency "logstash-core-plugin-api", "= 2.1.16"
  # Test-only. rspec is pinned to the version in Logstash 9.5.6's Gemfile.lock.
  s.add_development_dependency "rspec", "= 3.13.2"
end
