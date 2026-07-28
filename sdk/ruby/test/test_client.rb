# frozen_string_literal: true

require "minitest/autorun"
require "glypha_store"
require_relative "fake_server"

class ClientTest < Minitest::Test
  def test_put_get_ping_erase_and_structured_errors
    server = FakeServer.new
    client = GlyphaStore::Client.connect(
      GlyphaStore::ClientConfig.defaults.tap { |c| c.port = server.port }
    )
    key = "binary\x00key".b
    value = "value\x00\xff".b
    assert client.put(key, value).committed?
    assert_equal value, client.get(key)
    assert_equal "hello".b, client.ping("hello".b)
    assert client.erase(key).committed?
    err = assert_raises(GlyphaStore::Error) { client.get(key) }
    assert_equal GlyphaStore::Category::NOT_FOUND, err.category
    assert_equal GlyphaStore::Protocol::Status::NOT_FOUND, err.wire_status
    assert_equal GlyphaStore::Retryability::NEW_ATTEMPT, err.retryability
    assert_equal "get", err.operation
    client.close
  ensure
    server&.join
  end

  def test_internal_error_mutation_is_indeterminate
    server = FakeServer.new(internal_error_on_put: true)
    client = GlyphaStore::Client.connect(
      GlyphaStore::ClientConfig.defaults.tap { |c| c.port = server.port }
    )
    result = client.put("key".b, "value".b)
    assert result.indeterminate?
    assert_equal GlyphaStore::Category::INTERNAL, result.error.category
    assert_equal GlyphaStore::Protocol::Status::INTERNAL_ERROR, result.error.wire_status
    assert_equal GlyphaStore::Retryability::RECONCILE_FIRST, result.error.retryability
    assert_equal "put", result.error.operation
    client.close
  ensure
    server&.join
  end

  def test_permission_denied_status_eight
    server = FakeServer.new(deny_data_plane: true)
    client = GlyphaStore::Client.connect(
      GlyphaStore::ClientConfig.defaults.tap { |c| c.port = server.port }
    )
    err = assert_raises(GlyphaStore::Error) { client.get("key".b) }
    assert_equal GlyphaStore::Category::PERMISSION_DENIED, err.category
    assert_equal GlyphaStore::Protocol::Status::PERMISSION_DENIED, err.wire_status
    assert_equal GlyphaStore::Retryability::NEVER, err.retryability
    result = client.put("key".b, "value".b)
    assert result.rejected?
    assert_equal GlyphaStore::Category::PERMISSION_DENIED, result.error.category
    assert_equal GlyphaStore::Protocol::Status::PERMISSION_DENIED, result.error.wire_status
    client.close
  ensure
    server&.join
  end

  def test_overloaded_retryability_is_never
    err = GlyphaStore::Error.overloaded("server is overloaded")
    assert_equal GlyphaStore::Category::OVERLOADED, err.category
    assert_equal GlyphaStore::Retryability::NEVER, err.retryability
    assert_equal GlyphaStore::Retryability::NEVER,
                 GlyphaStore::Error.retryability_for(GlyphaStore::Category::OVERLOADED, false, false)
  end

  def test_pipeline_put_get
    server = FakeServer.new
    client = GlyphaStore::Client.connect(
      GlyphaStore::ClientConfig.defaults.tap { |c| c.port = server.port }
    )
    key = "pipe".b
    value = "v".b
    responses = client.execute_pipeline(
      [
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::PUT, key: key, value: value),
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::GET, key: key)
      ]
    )
    assert_equal 2, responses.length
    assert responses[0].succeeded?
    assert responses[1].succeeded?
    assert_equal value, responses[1].value
    client.close
  ensure
    server&.join
  end

  def test_batch_groups_workers
    server = FakeServer.new(workers: 2)
    client = GlyphaStore::Client.connect(
      GlyphaStore::ClientConfig.defaults.tap { |c| c.port = server.port }
    )
    assert_equal 2, client.worker_count
    # Find keys for each worker
    keys = [nil, nil]
    candidate = 0
    while keys.any?(&:nil?)
      key = format("k%08d", candidate).b
      w = client.worker_for(key)
      keys[w] ||= key
      candidate += 1
    end
    responses = client.execute_batch(
      [
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::PUT, key: keys[0], value: "a".b),
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::PUT, key: keys[1], value: "b".b),
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::GET, key: keys[0]),
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::GET, key: keys[1])
      ]
    )
    assert responses[0].succeeded?
    assert responses[1].succeeded?
    assert_equal "a".b, responses[2].value
    assert_equal "b".b, responses[3].value
    client.close
  ensure
    server&.join
  end

  def test_rejects_non_positive_timeout
    server = FakeServer.new
    client = GlyphaStore::Client.connect(
      GlyphaStore::ClientConfig.defaults.tap { |c| c.port = server.port }
    )
    err = assert_raises(GlyphaStore::Error) { client.get("k".b, timeout: 0) }
    assert_equal GlyphaStore::Category::INVALID_ARGUMENT, err.category
    client.close
  ensure
    server&.join
  end

  def test_pipeline_disconnect_classifies_mutations
    server = FakeServer.new(drop_after_mutation: true)
    client = GlyphaStore::Client.connect(
      GlyphaStore::ClientConfig.defaults.tap { |c| c.port = server.port }
    )
    responses = client.execute_pipeline(
      [
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::PUT, key: "k".b, value: "v".b),
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::GET, key: "k".b),
        GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::ERASE, key: "k".b)
      ]
    )
    # After PUT succeeds and server drops, GET/ERASE unresolved; if drop happens after PUT reply,
    # first may succeed. Fake drops after writing PUT OK — subsequent receive fails.
    assert_equal GlyphaStore::PipelineOutcome::SUCCEEDED, responses[0].outcome
    assert_equal GlyphaStore::PipelineOutcome::FAILED, responses[1].outcome
    assert_equal GlyphaStore::PipelineOutcome::INDETERMINATE, responses[2].outcome
    client.close
  ensure
    server&.join
  end

  def test_tls_config_requires_cert_and_key_pair
    err = assert_raises(GlyphaStore::Error) do
      GlyphaStore::Client.connect(
        GlyphaStore::ClientConfig.defaults.tap do |c|
          c.port = 1
          c.tls = true
          c.cert_file = "only-cert.pem"
        end
      )
    end
    assert_equal GlyphaStore::Category::INVALID_ARGUMENT, err.category
  end

  def test_build_ssl_context_requires_tls_1_3
    skip "TLS 1.3 unavailable in this Ruby/OpenSSL" unless GlyphaStore::Tls.tls13_available?

    context = GlyphaStore::Tls.build_ssl_context(
      GlyphaStore::ClientConfig.defaults.tap do |c|
        c.tls = true
        c.insecure_skip_verify = true
      end
    )
    assert_equal OpenSSL::SSL::TLS1_3_VERSION, context.min_version
    assert_equal OpenSSL::SSL::TLS1_3_VERSION, context.max_version
    assert_equal OpenSSL::SSL::VERIFY_NONE, context.verify_mode
  end

  def test_sync_and_async_tls_ping
    skip "TLS 1.3 unavailable in this Ruby/OpenSSL" unless GlyphaStore::Tls.tls13_available?

    material = self_signed_material
    skip "openssl CLI unavailable for ephemeral certs" if material.nil?

    cert_path, key_path = material
    server_context = OpenSSL::SSL::SSLContext.new
    server_context.min_version = OpenSSL::SSL::TLS1_3_VERSION
    server_context.max_version = OpenSSL::SSL::TLS1_3_VERSION
    server_context.cert = OpenSSL::X509::Certificate.new(File.binread(cert_path))
    server_context.key = OpenSSL::PKey.read(File.binread(key_path))
    server = FakeServer.new(ssl_context: server_context)
    config = GlyphaStore::ClientConfig.defaults.tap do |c|
      c.port = server.port
      c.tls = true
      c.tls_ca = cert_path
      c.server_name = "localhost"
    end
    client = GlyphaStore::Client.connect(config)
    assert_equal "tls-ping".b, client.ping("tls-ping".b)
    client.close

    require "glypha_store/async_client"
    Async do
      async_client = GlyphaStore::AsyncClient.connect(config)
      assert_equal "tls-async".b, async_client.ping("tls-async".b)
      async_client.close
    end
  ensure
    server&.join
  end

  def self_signed_material
    require "tmpdir"
    require "open3"
    directory = Dir.mktmpdir("glyphastore-rb-tls-")
    cert_path = File.join(directory, "server.crt")
    key_path = File.join(directory, "server.key")
    _out, _err, status = Open3.capture3(
      "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
      "-keyout", key_path, "-out", cert_path, "-days", "1", "-subj", "/CN=localhost"
    )
    return nil unless status.success?

    [cert_path, key_path]
  rescue Errno::ENOENT
    nil
  end
end
