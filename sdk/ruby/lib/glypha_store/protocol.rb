# frozen_string_literal: true

module GlyphaStore
  # Canonical wire-protocol v2 codec and Worker routing.
  module Protocol
    VERSION = 2
    REQUEST_HEADER_BYTES = 40
    RESPONSE_HEADER_BYTES = 40
    MAX_FRAME_BYTES = 2 * 1024 * 1024
    NO_WORKER = 0xFFFF_FFFF
    IDENTITY = "GlyphaStore/2"

    module Opcode
      INIT = 1
      PING = 2
      GET = 3
      PUT = 4
      ERASE = 5
      BIND_WORKER = 6
    end

    module Status
      OK = 0
      INVALID_REQUEST = 1
      UNSUPPORTED = 2
      INTERNAL_ERROR = 3
      NOT_FOUND = 4
      OVERLOADED = 5
      WRONG_OWNER = 6
      NOT_BOUND = 7
    end

    Request = Struct.new(
      :opcode, :request_id, :expire_at_ns, :target_worker, :key, :value,
      keyword_init: true
    )
    Response = Struct.new(
      :status, :request_id, :owner_worker, :worker_count, :routing_epoch, :value,
      keyword_init: true
    )

    REQUEST_HEADER = "L<S<CCQ<L<L<Q<L<L<"
    RESPONSE_HEADER = "L<S<S<Q<L<L<L<L<Q<"

    module_function

    def request_frame_size(key, value)
      REQUEST_HEADER_BYTES + key.bytesize + value.bytesize
    end

    def encode_request(opcode, request_id, key: "".b, value: "".b, expire_at_ns: 0, target_worker: NO_WORKER)
      key = binary!(key)
      value = binary!(value)
      validate_request_fields!(opcode, key, value, expire_at_ns, target_worker)
      u64!(request_id, "request_id")
      u64!(expire_at_ns, "expire_at_ns")
      u32!(target_worker, "target_worker")
      frame_size = request_frame_size(key, value)
      raise ArgumentError, "request exceeds the protocol frame limit" if frame_size > MAX_FRAME_BYTES

      [
        frame_size, VERSION, opcode, 0, request_id,
        key.bytesize, value.bytesize, expire_at_ns, target_worker, 0
      ].pack(REQUEST_HEADER) + key + value
    end

    def decode_request(frame, maximum_frame_bytes = MAX_FRAME_BYTES)
      frame = binary!(frame)
      raise ArgumentError, "request is shorter than its header" if frame.bytesize < REQUEST_HEADER_BYTES

      frame_size, version, opcode, flags, request_id, key_size, value_size,
        expire_at_ns, target_worker, reserved = frame.unpack(REQUEST_HEADER)
      if frame_size != frame.bytesize || frame_size > maximum_frame_bytes
        raise ArgumentError, "request frame extent is invalid"
      end
      raise ArgumentError, "request protocol version is unsupported" if version != VERSION
      raise ArgumentError, "request canonical fields are invalid" if flags != 0 || reserved != 0
      if REQUEST_HEADER_BYTES + key_size + value_size != frame_size
        raise ArgumentError, "request payload extent is invalid"
      end
      raise ArgumentError, "request opcode is unknown" unless valid_opcode?(opcode)

      key_start = REQUEST_HEADER_BYTES
      value_start = key_start + key_size
      Request.new(
        opcode: opcode,
        request_id: request_id,
        expire_at_ns: expire_at_ns,
        target_worker: target_worker,
        key: frame.byteslice(key_start, key_size).b,
        value: frame.byteslice(value_start, value_size).b
      )
    end

    def encode_response(status, request_id, value: "".b, owner_worker: NO_WORKER,
                        worker_count: 0, routing_epoch: 0)
      raise ArgumentError, "status is not defined by wire protocol v2" unless valid_status?(status)

      value = binary!(value)
      u64!(request_id, "request_id")
      u32!(owner_worker, "owner_worker")
      u32!(worker_count, "worker_count")
      u64!(routing_epoch, "routing_epoch")
      frame_size = RESPONSE_HEADER_BYTES + value.bytesize
      raise ArgumentError, "response exceeds the protocol frame limit" if frame_size > MAX_FRAME_BYTES

      [
        frame_size, VERSION, status, request_id, value.bytesize,
        owner_worker, worker_count, 0, routing_epoch
      ].pack(RESPONSE_HEADER) + value
    end

    def decode_response(frame, maximum_frame_bytes = MAX_FRAME_BYTES)
      frame = binary!(frame)
      raise ArgumentError, "response is shorter than its header" if frame.bytesize < RESPONSE_HEADER_BYTES

      frame_size, version, status, request_id, value_size, owner_worker,
        worker_count, reserved, routing_epoch = frame.unpack(RESPONSE_HEADER)
      if frame_size != frame.bytesize || frame_size > maximum_frame_bytes
        raise ArgumentError, "response frame extent is invalid"
      end
      raise ArgumentError, "response protocol version is unsupported" if version != VERSION
      raise ArgumentError, "response reserved field is noncanonical" if reserved != 0
      if RESPONSE_HEADER_BYTES + value_size != frame_size
        raise ArgumentError, "response value extent is invalid"
      end
      raise ArgumentError, "response status is unknown" unless valid_status?(status)

      Response.new(
        status: status,
        request_id: request_id,
        owner_worker: owner_worker,
        worker_count: worker_count,
        routing_epoch: routing_epoch,
        value: frame.byteslice(RESPONSE_HEADER_BYTES, value_size).to_s.b
      )
    end

    def fnv1a64(key)
      key = binary!(key)
      value = 14_695_981_039_346_656_037
      key.each_byte do |byte|
        value ^= byte
        value = (value * 1_099_511_628_211) & 0xFFFF_FFFF_FFFF_FFFF
      end
      value
    end

    def worker_for(key, worker_count)
      raise ArgumentError, "worker_count must be positive" if worker_count <= 0

      fnv1a64(key) % worker_count
    end

    def valid_opcode?(opcode)
      opcode.between?(Opcode::INIT, Opcode::BIND_WORKER)
    end

    def valid_status?(status)
      status.between?(Status::OK, Status::NOT_BOUND)
    end

    def binary!(payload)
      case payload
      when String
        payload.b
      when nil
        "".b
      else
        raise TypeError, "key and value must be strings"
      end
    end

    def u64!(value, field)
      raise ArgumentError, "#{field} is outside unsigned 64-bit range" unless value.is_a?(Integer) && value.between?(0, 0xFFFF_FFFF_FFFF_FFFF)

      value
    end

    def u32!(value, field)
      raise ArgumentError, "#{field} is outside unsigned 32-bit range" unless value.is_a?(Integer) && value.between?(0, 0xFFFF_FFFF)

      value
    end

    def validate_request_fields!(opcode, key, value, expire_at_ns, target_worker)
      case opcode
      when Opcode::INIT
        if !key.empty? || !value.empty? || expire_at_ns != 0 || target_worker != NO_WORKER
          raise ArgumentError, "INIT request cannot carry key, value, expiry, or target_worker"
        end
      when Opcode::PING
        if !key.empty? || expire_at_ns != 0 || target_worker != NO_WORKER
          raise ArgumentError, "PING request cannot carry key, expiry, or target_worker"
        end
      when Opcode::GET
        if !value.empty? || expire_at_ns != 0 || target_worker != NO_WORKER
          raise ArgumentError, "GET request cannot carry value, expiry, or target_worker"
        end
      when Opcode::PUT
        raise ArgumentError, "PUT request cannot carry target_worker" if target_worker != NO_WORKER
      when Opcode::ERASE
        if !value.empty? || expire_at_ns != 0 || target_worker != NO_WORKER
          raise ArgumentError, "ERASE request cannot carry value, expiry, or target_worker"
        end
      when Opcode::BIND_WORKER
        if !key.empty? || !value.empty? || expire_at_ns != 0
          raise ArgumentError, "BIND_WORKER request cannot carry key, value, or expiry"
        end
        raise ArgumentError, "BIND_WORKER request requires an explicit target_worker" if target_worker == NO_WORKER
      else
        raise ArgumentError, "opcode is not defined by wire protocol v2"
      end
    end
    private_class_method :binary!, :u64!, :u32!, :validate_request_fields!
  end
end
