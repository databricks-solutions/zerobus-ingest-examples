# encoding: utf-8
# Test-only stand-in for logstash-codec-plain. The tarball's copy cannot be activated outside
# Bundler (it needs the logstash-core gem spec). The output never uses a codec in these specs.
require "logstash/codecs/base"

module LogStash
  module Codecs
    class Plain < LogStash::Codecs::Base
      config_name "plain"

      def decode(data, &block)
        yield LogStash::Event.new("message" => data)
      end

      def encode(event)
        @on_event.call(event, event.get("message").to_s) if @on_event
      end
    end
  end
end
