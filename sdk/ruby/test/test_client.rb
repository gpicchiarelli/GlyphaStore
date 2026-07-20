# frozen_string_literal: true

require "minitest/autorun"
require "socket"
require "timeout"
require_relative "../lib/glypha_store"

class FakeServer
  attr_reader :port

  def initialize(workers: 1, internal_error_on_put: false, drop_after_mutation: false)
    @workers = workers
    @internal_error_on_put = internal_error_on_put
    @drop_after_mutation = drop_after_mutation
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.addr[1]
    @thread = Thread.new { accept_loop }
  end

  def join
    @server.close
    @thread.join(2)
  end

  private

  def accept_loop
    loop do
      client = @server.accept
      Thread.new { handle(client) }
    rescue IOError, Errno::EBADF
      break
    end
  end

  def handle(socket)
    bound = nil
    store = {}
    loop do
      prefix = read_exact(socket, 4)
      size = prefix.unpack1("L<")
      frame = prefix + read_exact(socket, size - 4)
      request = GlyphaStore::Protocol.decode_request(frame)
      case request.opcode
      when GlyphaStore::Protocol::Opcode::INIT
        reply(socket, status: GlyphaStore::Protocol::Status::OK, request_id: request.request_id,
                      value: GlyphaStore::Protocol::IDENTITY, owner_worker: GlyphaStore::Protocol::NO_WORKER,
                      worker_count: @workers, routing_epoch: 9)
      when GlyphaStore::Protocol::Opcode::BIND_WORKER
        bound = request.target_worker
        reply(socket, status: GlyphaStore::Protocol::Status::OK, request_id: request.request_id,
                      owner_worker: bound, worker_count: @workers, routing_epoch: 9)
      when GlyphaStore::Protocol::Opcode::PUT
        if @internal_error_on_put
          reply(socket, status: GlyphaStore::Protocol::Status::INTERNAL_ERROR, request_id: request.request_id,
                        owner_worker: bound, worker_count: @workers, routing_epoch: 9)
        else
          store[request.key] = request.value
          reply(socket, status: GlyphaStore::Protocol::Status::OK, request_id: request.request_id,
                        owner_worker: bound, worker_count: @workers, routing_epoch: 9)
          if @drop_after_mutation
            socket.close
            return
          end
        end
      when GlyphaStore::Protocol::Opcode::GET
        if store.key?(request.key)
          reply(socket, status: GlyphaStore::Protocol::Status::OK, request_id: request.request_id,
                        value: store[request.key], owner_worker: bound, worker_count: @workers, routing_epoch: 9)
        else
          reply(socket, status: GlyphaStore::Protocol::Status::NOT_FOUND, request_id: request.request_id,
                        owner_worker: bound, worker_count: @workers, routing_epoch: 9)
        end
      when GlyphaStore::Protocol::Opcode::ERASE
        store.delete(request.key)
        reply(socket, status: GlyphaStore::Protocol::Status::OK, request_id: request.request_id,
                      owner_worker: bound, worker_count: @workers, routing_epoch: 9)
      when GlyphaStore::Protocol::Opcode::PING
        reply(socket, status: GlyphaStore::Protocol::Status::OK, request_id: request.request_id,
                      value: request.value, owner_worker: bound || 0, worker_count: @workers, routing_epoch: 9)
      else
        reply(socket, status: GlyphaStore::Protocol::Status::UNSUPPORTED, request_id: request.request_id,
                      owner_worker: bound || 0, worker_count: @workers, routing_epoch: 9)
      end
    end
  rescue EOFError, Errno::EPIPE, Errno::ECONNRESET, IOError
    nil
  ensure
    begin
      socket.close
    rescue StandardError
      nil
    end
  end

  def read_exact(socket, size)
    data = "".b
    while data.bytesize < size
      chunk = socket.read(size - data.bytesize)
      raise EOFError if chunk.nil? || chunk.empty?

      data << chunk.b
    end
    data
  end

  def reply(socket, status:, request_id:, value: "".b, owner_worker:, worker_count:, routing_epoch:)
    frame = GlyphaStore::Protocol.encode_response(
      status, request_id, value: value, owner_worker: owner_worker,
      worker_count: worker_count, routing_epoch: routing_epoch
    )
    socket.write(frame)
  end
end

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
end
