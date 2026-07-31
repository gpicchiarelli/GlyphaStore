# frozen_string_literal: true

begin
  require "async"
  require "async/barrier"
  require "async/semaphore"
rescue LoadError
  raise LoadError, "GlyphaStore::AsyncClient requires the async gem (gem install async)"
end

require "socket"
require "timeout"
require_relative "error"
require_relative "protocol"
require_relative "client"

module GlyphaStore
  # Fiber-aware async client (wire v2 + client-semantics v1).
  #
  # Must be used inside an +Async+ reactor. Cancellation / task stop poisons the
  # in-flight Worker connection (§6.3). Optional dependency: +async+ gem.
  #
  #   require "glypha_store/async_client"
  #   Async do
  #     client = GlyphaStore::AsyncClient.connect(config)
  #     client.get("key".b)
  #     client.close
  #   end
  class AsyncClient
    class Connection
      attr_reader :worker
      attr_accessor :socket, :input

      def initialize(worker)
        @worker = worker
        @mutex = Async::Semaphore.new(1)
        @socket = nil
        @input = "".b
      end

      def synchronize(&block)
        @mutex.acquire(&block)
      end

      def reset!
        if @socket
          begin
            @socket.close
          rescue StandardError
            nil
          end
        end
        @socket = nil
        @input = "".b
      end
    end

    attr_reader :worker_count, :routing_epoch, :routing

    def self.connect(config = ClientConfig.defaults)
      config = Client.send(:merge_config, config)
      Client.send(:validate_config!, config)
      client = new(config)
      client.send(:bootstrap_all!)
      client
    end

    def initialize(config)
      @config = config
      @connections = []
      @worker_count = 0
      @routing_epoch = 0
      @routing = Protocol::WorkerRouting.new
      @request_id = 1
      @request_id_mutex = Async::Semaphore.new(1)
      @healthy = true
    end

    def healthy?
      @healthy
    end

    def worker_for(key)
      raise Error.unavailable("client is not connected") if @worker_count.zero?

      Protocol.worker_for(key, @worker_count, @routing)
    end

    def get(key, timeout: nil)
      read(Protocol::Opcode::GET, key, "".b, timeout: timeout)
    end

    def ping(payload = "".b, timeout: nil)
      read(Protocol::Opcode::PING, "".b, payload, timeout: timeout)
    end

    def put(key, value, expire_at_ns: 0, timeout: nil)
      mutate(Protocol::Opcode::PUT, key, value, expire_at_ns, timeout: timeout)
    end

    def erase(key, timeout: nil)
      mutate(Protocol::Opcode::ERASE, key, "".b, 0, timeout: timeout)
    end

    def execute_pipeline(requests, timeout: nil)
      execute_pipeline_deadline(requests, resolve_deadline(timeout))
    end

    def execute_batch(requests, timeout: nil)
      return [] if requests.nil? || requests.empty?
      raise Error.unavailable("client is closed or routing metadata changed") unless healthy?

      deadline = resolve_deadline(timeout)
      groups = Hash.new { |h, k| h[k] = [] }
      requests.each_with_index do |request, index|
        unless [PipelineOpcode::GET, PipelineOpcode::PUT, PipelineOpcode::ERASE].include?(request.opcode)
          raise Error.invalid_argument("batch request contains an invalid opcode")
        end
        if [PipelineOpcode::GET, PipelineOpcode::ERASE].include?(request.opcode) &&
           (!request.value.to_s.empty? || request.expire_at_ns != 0)
          raise Error.invalid_argument("GET and ERASE batch requests cannot carry PUT fields")
        end
        worker = worker_for(request.key)
        if groups[worker].length >= @config.maximum_pipeline_requests
          raise Error.invalid_argument("batch exceeds the configured per-Worker request limit")
        end
        groups[worker] << [index, request]
      end

      responses = Array.new(requests.length) { PipelineResponse.new(outcome: PipelineOutcome::FAILED) }
      if groups.length == 1
        groups.each_value do |items|
          reqs = items.map { |(_, r)| r }
          resps = execute_pipeline_deadline(reqs, deadline)
          items.each_with_index { |(index, _), i| responses[index] = resps[i] }
        end
        return responses
      end

      barrier = Async::Barrier.new
      first_error = nil
      error_mutex = Async::Semaphore.new(1)
      groups.each_value do |items|
        barrier.async do
          reqs = items.map { |(_, r)| r }
          resps = execute_pipeline_deadline(reqs, deadline)
          items.each_with_index { |(index, _), i| responses[index] = resps[i] }
        rescue Error => e
          error_mutex.acquire
          begin
            first_error ||= e
          ensure
            error_mutex.release
          end
        end
      end
      begin
        barrier.wait
      ensure
        barrier.stop
      end
      raise first_error if first_error

      responses
    end

    def close
      @healthy = false
      @connections.each(&:reset!)
    end

    private

    def bootstrap_all!
      first = Connection.new(0)
      worker_count, routing_epoch, routing = bootstrap!(first, nil)
      @worker_count = worker_count
      @routing_epoch = routing_epoch
      @routing = routing
      @connections << first
      expected = [worker_count, routing_epoch, routing]
      (1...worker_count).each do |worker|
        conn = Connection.new(worker)
        bootstrap!(conn, expected)
        @connections << conn
      end
    rescue StandardError
      close
      raise
    end

    def resolve_deadline(timeout)
      budget = @config.request_timeout
      unless timeout.nil?
        raise Error.invalid_argument("request timeout must be positive") if timeout <= 0

        budget = timeout
      end
      raise Error.invalid_argument("request timeout must be positive") if budget <= 0

      Process.clock_gettime(Process::CLOCK_MONOTONIC) + budget
    end

    def remaining_timeout(deadline)
      left = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise Error.transport("request deadline expired") if left <= 0

      left
    end

    def next_request_id
      @request_id_mutex.acquire do
        current = @request_id
        @request_id = current == 0xFFFF_FFFF_FFFF_FFFF ? 1 : current + 1
        current
      end
    end

    def mark_unhealthy!
      @healthy = false
    end

    def dial!
      addr = Socket.sockaddr_in(@config.port, @config.host)
      socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      begin
        Timeout.timeout(@config.connect_timeout) { socket.connect(addr) }
      rescue Timeout::Error, SystemCallError, IOError => e
        begin
          socket.close
        rescue StandardError
          nil
        end
        raise Error.unavailable("could not connect to GlyphaStore: #{e.message}")
      end
      socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      return socket unless @config.tls

      begin
        Tls.wrap_socket(socket, @config)
      rescue Error
        begin
          socket.close
        rescue StandardError
          nil
        end
        raise
      end
    end

    def bootstrap!(conn, expected)
      conn.reset!
      conn.socket = dial!
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @config.request_timeout
      init_id = next_request_id
      frame = Protocol.encode_request(Protocol::Opcode::INIT, init_id)
      response = exchange!(conn, frame, deadline)
      if response.status != Protocol::Status::OK ||
         response.request_id != init_id ||
         response.worker_count.zero? || response.worker_count > 256 ||
         response.routing_epoch.zero?
        conn.reset!
        raise Error.protocol("server INIT response is inconsistent")
      end
      begin
        routing = Protocol.decode_init_identity(response.value)
      rescue ArgumentError
        conn.reset!
        raise Error.protocol("server INIT response is inconsistent")
      end
      meta = [response.worker_count, response.routing_epoch, routing]
      if expected && meta != expected
        conn.reset!
        raise Error.unavailable("server routing metadata changed during bootstrap")
      end
      bind_id = next_request_id
      bind_frame = Protocol.encode_request(
        Protocol::Opcode::BIND_WORKER, bind_id, target_worker: conn.worker
      )
      bound = exchange!(conn, bind_frame, deadline)
      if bound.status != Protocol::Status::OK ||
         bound.request_id != bind_id ||
         bound.owner_worker != conn.worker ||
         bound.worker_count != response.worker_count ||
         bound.routing_epoch != response.routing_epoch
        conn.reset!
        raise Error.protocol("server BIND_WORKER response is inconsistent")
      end
      meta
    rescue Client::SendFailure => e
      conn.reset!
      raise Error.unavailable(e.error.message)
    rescue Error => e
      conn.reset!
      raise Error.unavailable(e.message) if e.category == Category::TRANSPORT

      raise
    end

    def ensure_connected!(conn)
      return if conn.socket

      bootstrap!(conn, [@worker_count, @routing_epoch, @routing])
    end

    def wait_readable!(socket, timeout)
      raise Error.transport("request deadline expired") unless socket.wait_readable(timeout)
    end

    def wait_writable!(socket, timeout)
      raise Error.transport("request deadline expired") unless socket.wait_writable(timeout)
    end

    def send!(conn, frame, deadline)
      sent = 0
      begin
        while sent < frame.bytesize
          timeout = remaining_timeout(deadline)
          n = conn.socket.write_nonblock(frame.byteslice(sent, frame.bytesize - sent), exception: false)
          if n == :wait_writable
            wait_writable!(conn.socket, timeout)
            next
          end
          if n.nil? || n.zero?
            raise Client::SendFailure.new(error: Error.transport("socket closed during send"), bytes_sent: sent)
          end

          sent += n
        end
      rescue Error => e
        raise Client::SendFailure.new(error: e, bytes_sent: sent)
      rescue SystemCallError, IOError => e
        msg = e.is_a?(Errno::ETIMEDOUT) ? "request deadline expired" : "request send failed: #{e.message}"
        raise Client::SendFailure.new(error: Error.transport(msg), bytes_sent: sent)
      end
      nil
    end

    def receive_response!(conn, deadline)
      loop do
        available = conn.input.bytesize
        if available >= 4
          frame_size = conn.input.unpack1("L<")
          if frame_size < Protocol::RESPONSE_HEADER_BYTES || frame_size > @config.maximum_frame_bytes
            raise Error.protocol("server response size is outside client limits")
          end
          if available >= frame_size
            frame = conn.input.byteslice(0, frame_size)
            conn.input = conn.input.byteslice(frame_size..-1).to_s.b
            begin
              return Protocol.decode_response(frame, @config.maximum_frame_bytes)
            rescue ArgumentError => e
              raise Error.protocol(e.message)
            end
          end
        end

        timeout = remaining_timeout(deadline)
        chunk = conn.socket.read_nonblock(64 * 1024, exception: false)
        if chunk == :wait_readable
          wait_readable!(conn.socket, timeout)
          next
        end
        raise Error.transport("server closed the connection") if chunk.nil? || chunk.empty?

        conn.input << chunk.b
      rescue EOFError
        raise Error.transport("server closed the connection")
      rescue SystemCallError, IOError => e
        raise Error.transport("request deadline expired") if e.is_a?(Errno::ETIMEDOUT)

        raise Error.transport("response receive failed: #{e.message}")
      end
    end

    def exchange!(conn, frame, deadline)
      completed = false
      begin
        send!(conn, frame, deadline)
        response = receive_response!(conn, deadline)
        completed = true
        response
      ensure
        # Poison on cancel / interrupt mid-exchange so a late frame cannot attach.
        conn.reset! unless completed
      end
    end

    def validate_response!(response, request_id, worker)
      raise Error.protocol("server response request ID does not match") if response.request_id != request_id

      if response.worker_count != @worker_count || response.routing_epoch != @routing_epoch
        mark_unhealthy!
        raise Error.unavailable("server routing metadata changed")
      end
      if response.owner_worker != worker && response.status != Protocol::Status::WRONG_OWNER
        mark_unhealthy!
        raise Error.protocol("server response came from the wrong Worker")
      end
    end

    def annotate!(error, op, request_id, worker)
      error.enrich(operation: op, request_id: request_id, worker: worker, routing_epoch: @routing_epoch)
      error
    end

    def promote_send_failure(sf, op, request_id, worker, mutation:)
      err = sf.error
      err.enrich(
        bytes_sent: sf.bytes_sent,
        operation: op,
        request_id: request_id,
        worker: worker,
        routing_epoch: @routing_epoch,
        mutation_outcome: if mutation
                            sf.bytes_sent.zero? ? MutationOutcome::REJECTED : MutationOutcome::INDETERMINATE
                          end
      )
      err
    end

    def read(opcode, key, value, timeout:)
      raise Error.unavailable("client is closed or routing metadata changed") unless healthy?

      deadline = resolve_deadline(timeout)
      worker = opcode == Protocol::Opcode::PING ? 0 : worker_for(key)
      conn = @connections[worker]
      op = opcode == Protocol::Opcode::PING ? "ping" : "get"
      last = Error.unavailable("request was not attempted")
      conn.synchronize do
        raise Error.unavailable("client closed before read admission") unless healthy?

        2.times do
          begin
            ensure_connected!(conn)
          rescue Error => e
            last = e
            raise last unless healthy?

            conn.reset!
            next
          end
          request_id = next_request_id
          begin
            frame = Protocol.encode_request(opcode, request_id, key: key, value: value)
          rescue ArgumentError => e
            raise Error.invalid_argument(e.message)
          end
          begin
            response = exchange!(conn, frame, deadline)
          rescue Client::SendFailure => e
            last = promote_send_failure(e, op, request_id, worker, mutation: false)
            conn.reset!
            raise last if last.category == Category::UNAVAILABLE && !healthy?

            next
          rescue Error => e
            last = annotate!(e, op, request_id, worker)
            conn.reset!
            raise last if last.category == Category::UNAVAILABLE && !healthy?

            next
          end
          begin
            validate_response!(response, request_id, worker)
          rescue Error => e
            conn.reset!
            annotated = annotate!(e, op, request_id, worker)
            raise annotated if annotated.category == Category::PROTOCOL
            raise annotated if annotated.category == Category::UNAVAILABLE && !healthy?

            last = annotated
            next
          end
          if response.status != Protocol::Status::OK
            mark_unhealthy! if [Protocol::Status::WRONG_OWNER, Protocol::Status::NOT_BOUND].include?(response.status)
            raise annotate!(Error.from_status(response.status), op, request_id, worker)
          end
          return response.value
        end
        raise last
      end
    end

    def mutate(opcode, key, value, expire_at_ns, timeout:)
      op = opcode == Protocol::Opcode::PUT ? "put" : "erase"
      unless healthy?
        return MutationResult.new(
          outcome: MutationOutcome::REJECTED,
          error: Error.unavailable("client is closed or routing metadata changed")
            .enrich(operation: op, mutation_outcome: MutationOutcome::REJECTED)
        )
      end
      begin
        deadline = resolve_deadline(timeout)
        worker = worker_for(key)
      rescue Error => e
        return MutationResult.new(
          outcome: MutationOutcome::REJECTED,
          error: e.enrich(operation: op, mutation_outcome: MutationOutcome::REJECTED)
        )
      end
      conn = @connections[worker]
      conn.synchronize do
        unless healthy?
          return MutationResult.new(
            outcome: MutationOutcome::REJECTED,
            error: Error.unavailable("client closed before mutation admission")
              .enrich(operation: op, worker: worker, routing_epoch: @routing_epoch,
                      mutation_outcome: MutationOutcome::REJECTED)
          )
        end
        2.times do |attempt|
          begin
            ensure_connected!(conn)
          rescue Error => e
            return MutationResult.new(
              outcome: MutationOutcome::REJECTED,
              error: e.enrich(operation: op, worker: worker, routing_epoch: @routing_epoch,
                              mutation_outcome: MutationOutcome::REJECTED)
            )
          end
          request_id = next_request_id
          begin
            frame = Protocol.encode_request(
              opcode, request_id, key: key, value: value, expire_at_ns: expire_at_ns
            )
          rescue ArgumentError => e
            return MutationResult.new(
              outcome: MutationOutcome::REJECTED,
              error: Error.invalid_argument(e.message)
                .enrich(operation: op, request_id: request_id, worker: worker,
                        routing_epoch: @routing_epoch, mutation_outcome: MutationOutcome::REJECTED)
            )
          end
          begin
            response = exchange!(conn, frame, deadline)
          rescue Client::SendFailure => e
            conn.reset!
            promoted = promote_send_failure(e, op, request_id, worker, mutation: true)
            if e.bytes_sent.zero?
              next if attempt.zero?

              return MutationResult.new(outcome: MutationOutcome::REJECTED, error: promoted)
            end
            return MutationResult.new(outcome: MutationOutcome::INDETERMINATE, error: promoted)
          rescue Error => e
            conn.reset!
            return MutationResult.new(
              outcome: MutationOutcome::INDETERMINATE,
              error: annotate!(e, op, request_id, worker)
                .enrich(mutation_outcome: MutationOutcome::INDETERMINATE)
            )
          end
          begin
            validate_response!(response, request_id, worker)
          rescue Error => e
            conn.reset!
            return MutationResult.new(
              outcome: MutationOutcome::INDETERMINATE,
              error: annotate!(e, op, request_id, worker)
                .enrich(mutation_outcome: MutationOutcome::INDETERMINATE)
            )
          end
          if response.status == Protocol::Status::OK
            unless response.value.empty?
              conn.reset!
              return MutationResult.new(
                outcome: MutationOutcome::INDETERMINATE,
                error: Error.protocol("mutation response value must be empty")
                  .enrich(operation: op, request_id: request_id, worker: worker,
                          routing_epoch: @routing_epoch,
                          mutation_outcome: MutationOutcome::INDETERMINATE)
              )
            end
            return MutationResult.new(outcome: MutationOutcome::COMMITTED)
          end
          status_err = annotate!(Error.from_status(response.status), op, request_id, worker)
          if response.status == Protocol::Status::INTERNAL_ERROR
            return MutationResult.new(
              outcome: MutationOutcome::INDETERMINATE,
              error: status_err.enrich(mutation_outcome: MutationOutcome::INDETERMINATE)
            )
          end
          mark_unhealthy! if [Protocol::Status::WRONG_OWNER, Protocol::Status::NOT_BOUND].include?(response.status)
          return MutationResult.new(
            outcome: MutationOutcome::REJECTED,
            error: status_err.enrich(mutation_outcome: MutationOutcome::REJECTED)
          )
        end
        MutationResult.new(
          outcome: MutationOutcome::REJECTED,
          error: Error.unavailable("could not send mutation")
            .enrich(operation: op, worker: worker, routing_epoch: @routing_epoch,
                    mutation_outcome: MutationOutcome::REJECTED)
        )
      end
    end

    def execute_pipeline_deadline(requests, deadline)
      return [] if requests.nil? || requests.empty?
      raise Error.unavailable("client is closed or routing metadata changed") unless healthy?
      if requests.length > @config.maximum_pipeline_requests
        raise Error.invalid_argument("pipeline exceeds the configured request limit")
      end

      worker = nil
      needed = 0
      requests.each do |request|
        unless [PipelineOpcode::GET, PipelineOpcode::PUT, PipelineOpcode::ERASE].include?(request.opcode)
          raise Error.invalid_argument("pipeline request contains an invalid opcode")
        end
        owner = worker_for(request.key)
        worker = owner if worker.nil?
        raise Error.invalid_argument("every pipeline key must route to the same Worker") if owner != worker

        if [PipelineOpcode::GET, PipelineOpcode::ERASE].include?(request.opcode) &&
           (!request.value.to_s.empty? || request.expire_at_ns != 0)
          raise Error.invalid_argument("GET and ERASE pipeline requests cannot carry PUT fields")
        end
        frame_len = Protocol.request_frame_size(request.key, request.value.to_s.b)
        if frame_len > @config.maximum_frame_bytes
          raise Error.invalid_argument("pipeline request exceeds the configured frame limit")
        end
        if frame_len > @config.maximum_pipeline_bytes - needed
          raise Error.invalid_argument("pipeline exceeds the configured aggregate byte limit")
        end
        needed += frame_len
      end

      responses = Array.new(requests.length) { PipelineResponse.new(outcome: PipelineOutcome::FAILED) }
      conn = @connections[worker]
      conn.synchronize do
        raise Error.unavailable("client closed before pipeline admission") unless healthy?

        ensure_connected!(conn)
        output = "".b
        metadata = []
        requests.each do |request|
          request_id = next_request_id
          begin_at = output.bytesize
          begin
            frame = Protocol.encode_request(
              request.opcode, request_id,
              key: request.key, value: request.value.to_s.b, expire_at_ns: request.expire_at_ns || 0
            )
          rescue ArgumentError => e
            raise Error.invalid_argument(e.message)
          end
          output << frame
          metadata << { opcode: request.opcode, request_id: request_id, begin: begin_at }
        end

        mark_unresolved = lambda do |first, err, bytes_sent|
          (first...metadata.length).each do |index|
            opcode = metadata[index][:opcode]
            mutation_arrived = [PipelineOpcode::PUT, PipelineOpcode::ERASE].include?(opcode) &&
                               bytes_sent > metadata[index][:begin]
            responses[index] = PipelineResponse.new(
              outcome: mutation_arrived ? PipelineOutcome::INDETERMINATE : PipelineOutcome::FAILED,
              error: err
            )
          end
        end

        completed = false
        begin
          begin
            send!(conn, output, deadline)
          rescue Client::SendFailure => e
            conn.reset!
            mark_unresolved.call(0, e.error, e.bytes_sent)
            completed = true
            return responses
          rescue Error => e
            conn.reset!
            mark_unresolved.call(0, e, 0)
            completed = true
            return responses
          end

          metadata.each_with_index do |item, index|
            begin
              response = receive_response!(conn, deadline)
            rescue Error => e
              sent_bytes = output.bytesize
              conn.reset!
              mark_unresolved.call(index, e, sent_bytes)
              completed = true
              return responses
            end
            begin
              validate_response!(response, item[:request_id], worker)
            rescue Error => e
              sent_bytes = output.bytesize
              conn.reset!
              mark_unresolved.call(index, e, sent_bytes)
              completed = true
              return responses
            end
            if response.status == Protocol::Status::OK
              if [PipelineOpcode::PUT, PipelineOpcode::ERASE].include?(item[:opcode]) && !response.value.empty?
                sent_bytes = output.bytesize
                conn.reset!
                mark_unresolved.call(
                  index, Error.protocol("mutation response value must be empty"), sent_bytes
                )
                completed = true
                return responses
              end
              responses[index] = PipelineResponse.new(outcome: PipelineOutcome::SUCCEEDED, value: response.value)
              next
            end
            err = Error.from_status(response.status)
            outcome = PipelineOutcome::FAILED
            if [PipelineOpcode::PUT, PipelineOpcode::ERASE].include?(item[:opcode]) &&
               response.status == Protocol::Status::INTERNAL_ERROR
              outcome = PipelineOutcome::INDETERMINATE
            end
            responses[index] = PipelineResponse.new(outcome: outcome, error: err)
            mark_unhealthy! if [Protocol::Status::WRONG_OWNER, Protocol::Status::NOT_BOUND].include?(response.status)
          end
          completed = true
          responses
        ensure
          conn.reset! unless completed
        end
      end
    end
  end
end
