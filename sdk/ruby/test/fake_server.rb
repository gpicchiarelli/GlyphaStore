# frozen_string_literal: true

require "socket"
require "openssl"
require "glypha_store"

class FakeServer
  attr_reader :port
  attr_accessor :routing

  def initialize(workers: 1, internal_error_on_put: false, drop_after_mutation: false,
                 deny_data_plane: false, ssl_context: nil)
    @workers = workers
    @internal_error_on_put = internal_error_on_put
    @drop_after_mutation = drop_after_mutation
    @deny_data_plane = deny_data_plane
    @ssl_context = ssl_context
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

  def handle(raw)
    socket = raw
    if @ssl_context
      begin
        ssl = OpenSSL::SSL::SSLSocket.new(raw, @ssl_context)
        ssl.sync_close = true
        ssl.accept
        socket = ssl
      rescue OpenSSL::SSL::SSLError, SystemCallError, IOError
        begin
          raw.close
        rescue StandardError
          nil
        end
        return
      end
    end
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
                      value: GlyphaStore::Protocol.encode_init_identity(@routing || GlyphaStore::Protocol::WorkerRouting.new), owner_worker: GlyphaStore::Protocol::NO_WORKER,
                      worker_count: @workers, routing_epoch: 9)
      when GlyphaStore::Protocol::Opcode::BIND_WORKER
        bound = request.target_worker
        reply(socket, status: GlyphaStore::Protocol::Status::OK, request_id: request.request_id,
                      owner_worker: bound, worker_count: @workers, routing_epoch: 9)
      when GlyphaStore::Protocol::Opcode::PUT
        if @deny_data_plane
          reply(socket, status: GlyphaStore::Protocol::Status::PERMISSION_DENIED, request_id: request.request_id,
                        owner_worker: bound, worker_count: @workers, routing_epoch: 9)
        elsif @internal_error_on_put
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
        if @deny_data_plane
          reply(socket, status: GlyphaStore::Protocol::Status::PERMISSION_DENIED, request_id: request.request_id,
                        owner_worker: bound, worker_count: @workers, routing_epoch: 9)
        elsif store.key?(request.key)
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
  rescue EOFError, Errno::EPIPE, Errno::ECONNRESET, IOError, OpenSSL::SSL::SSLError
    nil
  ensure
    begin
      socket&.close
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
