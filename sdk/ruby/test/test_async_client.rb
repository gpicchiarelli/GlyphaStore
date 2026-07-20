# frozen_string_literal: true

require "minitest/autorun"
require "glypha_store"
require "glypha_store/async_client"
require_relative "fake_server"

class AsyncClientTest < Minitest::Test
  def test_async_put_get_pipeline
    server = FakeServer.new
    Async do
      config = GlyphaStore::ClientConfig.defaults
      config.port = server.port
      client = GlyphaStore::AsyncClient.connect(config)
      key = "async\x00key".b
      value = "payload".b
      assert client.put(key, value).committed?
      assert_equal value, client.get(key)
      responses = client.execute_pipeline(
        [
          GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::PUT, key: key, value: "v2".b),
          GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::GET, key: key)
        ]
      )
      assert responses[1].succeeded?
      assert_equal "v2".b, responses[1].value
      client.close
    end
  ensure
    server&.join
  end

  def test_async_batch_two_workers
    server = FakeServer.new(workers: 2)
    Async do
      config = GlyphaStore::ClientConfig.defaults
      config.port = server.port
      client = GlyphaStore::AsyncClient.connect(config)
      keys = [nil, nil]
      candidate = 0
      while keys.any?(&:nil?)
        key = format("ak%08d", candidate).b
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
      assert_equal "a".b, responses[2].value
      assert_equal "b".b, responses[3].value
      client.close
    end
  ensure
    server&.join
  end

  def test_async_cancel_poisons_connection
    server = FakeServer.new
    Async do
      config = GlyphaStore::ClientConfig.defaults
      config.port = server.port
      client = GlyphaStore::AsyncClient.connect(config)
      task = Async do
        client.get("missing".b)
      end
      task.stop
      begin
        task.wait
      rescue Async::Stop
        # expected
      end
      err = assert_raises(GlyphaStore::Error) { client.get("missing".b) }
      assert_equal GlyphaStore::Category::NOT_FOUND, err.category
      client.close
    end
  ensure
    server&.join
  end
end
