# frozen_string_literal: true

require "minitest/autorun"
require "pathname"
require "glypha_store"

class ProtocolTest < Minitest::Test
  FIXTURES = Pathname.new(__dir__).join("fixtures")

  def fixture(name)
    FIXTURES.join(name).read.split.map { |token| token.to_i(16) }.pack("C*")
  end

  def frames(corpus)
    output = []
    offset = 0
    while offset < corpus.bytesize
      size = corpus.byteslice(offset, 4).unpack1("L<")
      output << corpus.byteslice(offset, size)
      offset += size
    end
    output
  end

  def test_request_encoder_matches_fixtures
    expected = frames(fixture("wire_requests_v2.hex"))
    encoded = [
      GlyphaStore::Protocol.encode_request(GlyphaStore::Protocol::Opcode::INIT, 1),
      GlyphaStore::Protocol.encode_request(GlyphaStore::Protocol::Opcode::PING, 2, value: "\x00ping\xff".b),
      GlyphaStore::Protocol.encode_request(GlyphaStore::Protocol::Opcode::GET, 3, key: "get\x00key".b),
      GlyphaStore::Protocol.encode_request(
        GlyphaStore::Protocol::Opcode::PUT, 4,
        key: "put\x00key".b, value: "\x10\x20\xff".b, expire_at_ns: 123_456_789
      ),
      GlyphaStore::Protocol.encode_request(GlyphaStore::Protocol::Opcode::ERASE, 5, key: "erase-key".b),
      GlyphaStore::Protocol.encode_request(
        GlyphaStore::Protocol::Opcode::BIND_WORKER, 6, target_worker: 2
      ),
      GlyphaStore::Protocol.encode_request(GlyphaStore::Protocol::Opcode::HEALTH, 7),
      GlyphaStore::Protocol.encode_request(GlyphaStore::Protocol::Opcode::READY, 8),
      GlyphaStore::Protocol.encode_request(GlyphaStore::Protocol::Opcode::STATS, 9)
    ]
    assert_equal expected, encoded
  end

  def test_request_decoder_round_trips
    expected = frames(fixture("wire_requests_v2.hex"))
    reencoded = expected.map do |frame|
      decoded = GlyphaStore::Protocol.decode_request(frame)
      GlyphaStore::Protocol.encode_request(
        decoded.opcode, decoded.request_id,
        key: decoded.key, value: decoded.value,
        expire_at_ns: decoded.expire_at_ns, target_worker: decoded.target_worker
      )
    end
    assert_equal expected, reencoded
  end

  def test_response_round_trips
    expected = frames(fixture("wire_responses_v2.hex"))
    reencoded = expected.map do |frame|
      decoded = GlyphaStore::Protocol.decode_response(frame)
      GlyphaStore::Protocol.encode_response(
        decoded.status, decoded.request_id,
        value: decoded.value, owner_worker: decoded.owner_worker,
        worker_count: decoded.worker_count, routing_epoch: decoded.routing_epoch
      )
    end
    assert_equal expected, reencoded
  end

  def test_worker_routing_is_deterministic
    key = "session\x0042".b
    assert_equal GlyphaStore::Protocol.fnv1a64(key) % 4, GlyphaStore::Protocol.worker_for(key, 4)
    assert_equal GlyphaStore::Protocol.worker_for(key, 4), GlyphaStore::Protocol.worker_for(key, 4)
  end

  def test_rejects_noncanonical_reserved
    frame = GlyphaStore::Protocol.encode_request(GlyphaStore::Protocol::Opcode::PING, 1, value: "x".b)
    mutated = frame.dup
    mutated.setbyte(36, 1)
    assert_raises(ArgumentError) { GlyphaStore::Protocol.decode_request(mutated) }
  end
end
