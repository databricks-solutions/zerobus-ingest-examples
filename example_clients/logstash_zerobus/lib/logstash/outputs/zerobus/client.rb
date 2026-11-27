# encoding: utf-8
require "json"
require "net/http"
require "openssl"
require "uri"

module LogStash
  module Outputs
    module ZerobusRest
      # Raised by the client. `status` is the HTTP status when there was one.
      class Error < StandardError
        attr_reader :status

        def initialize(message, status = nil)
          super(message)
          @status = status
        end
      end

      # The token endpoint refused the client (bad id/secret, wrong grant). Not retried.
      class AuthError < Error; end

      # Retry later: network failure, 408/425/429/5xx, or the token was rejected on insert.
      class TransientError < Error; end

      # 4xx for this payload. Not retried as-is; the plugin splits the batch to find the bad record.
      class RejectedError < Error; end

      # Request shapes from the Zerobus Ingest REST API docs.
      module Protocol
        SCOPE = "all-apis".freeze
        UNITY_CATALOG_PRIVILEGES = "unity_catalog_privileges".freeze

        module_function

        def resource(workspace_id)
          "api://databricks/workspaces/#{workspace_id}/zerobusDirectWriteApi"
        end

        # One table per token: USE CATALOG, USE SCHEMA, and SELECT+MODIFY on the table with the
        # zerobuswrite operation. Keep privileges to SELECT and MODIFY; other values fail the request.
        def authorization_details(table)
          catalog, schema, _name = table.split(".", 3)
          JSON.generate([
            { "type" => UNITY_CATALOG_PRIVILEGES, "privileges" => ["USE CATALOG"],
              "object_type" => "CATALOG", "object_full_path" => catalog },
            { "type" => UNITY_CATALOG_PRIVILEGES, "privileges" => ["USE SCHEMA"],
              "object_type" => "SCHEMA", "object_full_path" => "#{catalog}.#{schema}" },
            { "type" => UNITY_CATALOG_PRIVILEGES, "privileges" => ["SELECT", "MODIFY"],
              "object_type" => "TABLE", "object_full_path" => table,
              "operations" => ["zerobuswrite"] }
          ])
        end

        def insert_path(table)
          "/zerobus/v1/tables/#{table}/insert"
        end

        def basic_auth(client_id, client_secret)
          "Basic " + ["#{client_id}:#{client_secret}"].pack("m0")
        end
      end

      # Plain HTTP(S) with timeouts. Network errors become TransientError. Kept separate so the
      # specs can stub it.
      class Transport
        NETWORK_ERRORS = [
          Net::OpenTimeout, Net::ReadTimeout, SocketError, EOFError, Net::HTTPBadResponse,
          Errno::ECONNRESET, Errno::ECONNREFUSED, Errno::EPIPE, Errno::ETIMEDOUT, Errno::EHOSTUNREACH
        ].freeze

        def initialize(open_timeout: 10, read_timeout: 60)
          @open_timeout = open_timeout
          @read_timeout = read_timeout
        end

        # Returns [status_code, body_string].
        def post(uri, headers:, body:)
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = (uri.scheme == "https")
          http.open_timeout = @open_timeout
          http.read_timeout = @read_timeout
          request = Net::HTTP::Post.new(uri.request_uri)
          headers.each { |name, value| request[name] = value }
          request.body = body
          response = http.request(request)
          [response.code.to_i, response.body.to_s]
        rescue *NETWORK_ERRORS => e
          raise TransientError.new("network error (#{e.class})")
        end
      end

      # Fetches a table-scoped token with the OAuth client-credentials grant and keeps it until
      # `refresh_margin` seconds before it expires.
      class TokenProvider
        DEFAULT_LIFETIME = 3600

        def initialize(workspace_url:, workspace_id:, table:, client_id:, client_secret:,
                       transport:, refresh_margin: 300, clock: -> { Time.now.to_f })
          @token_url = "#{workspace_url.chomp('/')}/oidc/v1/token"
          @workspace_id = workspace_id
          @table = table
          @client_id = client_id
          @client_secret = client_secret
          @transport = transport
          @refresh_margin = refresh_margin
          @clock = clock
          @token = nil
          @expires_at = 0.0
          @lock = Mutex.new
        end

        def token
          @lock.synchronize do
            refresh_locked if @token.nil? || @clock.call >= @expires_at - @refresh_margin
            @token
          end
        end

        # Drop the cached token, for example after the insert endpoint answered 401.
        def invalidate
          @lock.synchronize { @token = nil }
        end

        private

        def refresh_locked
          form = URI.encode_www_form(
            "grant_type" => "client_credentials",
            "scope" => Protocol::SCOPE,
            "resource" => Protocol.resource(@workspace_id),
            "authorization_details" => Protocol.authorization_details(@table)
          )
          status, text = @transport.post(
            URI(@token_url),
            headers: {
              "Content-Type" => "application/x-www-form-urlencoded",
              "Authorization" => Protocol.basic_auth(@client_id, @client_secret)
            },
            body: form
          )
          case status
          when 200
            data = parse_json(text)
            token = data["access_token"]
            raise AuthError.new("token response had no access_token", status) if token.nil? || token.empty?
            @token = token
            @expires_at = @clock.call + (data["expires_in"] || DEFAULT_LIFETIME).to_f
          when 400, 401, 403
            raise AuthError.new("token endpoint refused the client (HTTP #{status}): #{snippet(text)}", status)
          when 408, 425, 429, 500..599
            raise TransientError.new("token endpoint unavailable (HTTP #{status})", status)
          else
            raise AuthError.new("unexpected HTTP #{status} from the token endpoint", status)
          end
        end

        def parse_json(text)
          JSON.parse(text)
        rescue JSON::ParserError
          raise AuthError.new("token endpoint returned non-JSON", 200)
        end

        def snippet(text)
          text.to_s[0, 200]
        end
      end

      # Sends one JSON array per request to the REST insert endpoint. Retries transient failures
      # with exponential backoff; the caller handles RejectedError.
      class Client
        RETRYABLE_STATUS = [408, 425, 429].freeze
        MAX_BACKOFF = 30.0

        def initialize(endpoint:, table:, token_provider:, transport:, max_retries: 5,
                       retry_backoff: 1.0, sleeper: ->(seconds) { Kernel.sleep(seconds) })
          @endpoint = endpoint
          @table = table
          @token_provider = token_provider
          @transport = transport
          @max_retries = max_retries
          @retry_backoff = retry_backoff
          @sleeper = sleeper
        end

        def insert(records)
          attempt = 0
          begin
            post_once(records)
          rescue TransientError
            attempt += 1
            raise if attempt > @max_retries
            @sleeper.call(backoff_for(attempt))
            retry
          end
          nil
        end

        private

        def post_once(records)
          token = @token_provider.token
          uri = URI("#{@endpoint}#{Protocol.insert_path(@table)}")
          status, text = @transport.post(
            uri,
            headers: {
              "Content-Type" => "application/json",
              "Authorization" => "Bearer #{token}"
            },
            body: JSON.generate(records)
          )
          case status
          when 200
            nil
          when 401
            @token_provider.invalidate
            raise TransientError.new("insert rejected the token (HTTP 401); refreshing it", status)
          when 403
            raise AuthError.new("insert forbidden (HTTP 403): #{snippet(text)}", status)
          when *RETRYABLE_STATUS, 500..599
            raise TransientError.new("insert failed (HTTP #{status}): #{snippet(text)}", status)
          else
            raise RejectedError.new("insert rejected (HTTP #{status}): #{snippet(text)}", status)
          end
        end

        def backoff_for(attempt)
          [@retry_backoff * (2**(attempt - 1)), MAX_BACKOFF].min
        end

        def snippet(text)
          text.to_s[0, 200]
        end
      end
    end
  end
end
