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
        # expected for non-mutation cancel
      end
      err = assert_raises(GlyphaStore::Error) { client.get("missing".b) }
      assert_equal GlyphaStore::Category::NOT_FOUND, err.category
      client.close
    end
  ensure
    server&.join
  end

  def test_async_cancel_after_put_send_is_indeterminate
    server = FakeServer.new(stall_on_put: true)
    Async do
      config = GlyphaStore::ClientConfig.defaults
      config.port = server.port
      config.request_timeout = 5.0
      client = GlyphaStore::AsyncClient.connect(config)
      result = nil
      task = Async do
        result = client.put("key".b, "value".b)
      end
      # Allow the PUT frame to leave the client and block on receive.
      Async::Task.current.sleep(0.05)
      task.stop
      task.wait
      assert result
      assert_equal GlyphaStore::MutationOutcome::INDETERMINATE, result.outcome
      assert result.error
      assert_operator result.error.bytes_sent, :>, 0
      assert_equal GlyphaStore::Retryability::RECONCILE_FIRST, result.error.retryability
      assert_equal GlyphaStore::MutationOutcome::INDETERMINATE, result.error.mutation_outcome
      # Poisoned Worker connection must still admit a fresh GET after cancel.
      err = assert_raises(GlyphaStore::Error) { client.get("missing".b) }
      assert_equal GlyphaStore::Category::NOT_FOUND, err.category
      client.close
    end
  ensure
    server&.join
  end

  def test_async_pipeline_cancel_after_send_classifies_mutations
    server = FakeServer.new(stall_on_put: true)
    Async do
      config = GlyphaStore::ClientConfig.defaults
      config.port = server.port
      config.request_timeout = 5.0
      client = GlyphaStore::AsyncClient.connect(config)
      responses = nil
      task = Async do
        responses = client.execute_pipeline(
          [
            GlyphaStore::PipelineRequest.new(
              opcode: GlyphaStore::PipelineOpcode::PUT, key: "k".b, value: "v".b
            ),
            GlyphaStore::PipelineRequest.new(opcode: GlyphaStore::PipelineOpcode::GET, key: "k".b)
          ]
        )
      end
      Async::Task.current.sleep(0.05)
      task.stop
      task.wait
      assert responses
      assert_equal GlyphaStore::PipelineOutcome::INDETERMINATE, responses[0].outcome
      assert responses[0].error
      assert_operator responses[0].error.bytes_sent, :>, 0
      assert_equal GlyphaStore::Retryability::RECONCILE_FIRST, responses[0].error.retryability
      assert_equal GlyphaStore::MutationOutcome::INDETERMINATE, responses[0].error.mutation_outcome
      assert_equal GlyphaStore::PipelineOutcome::FAILED, responses[1].outcome
      client.close
    end
  ensure
    server&.join
  end

  def test_async_batch_cancel_preserves_sibling_results
    # Worker-0 PUT succeeds; Worker-1 stalls. Outer cancel must return the slot
    # vector (sibling committed + stalled indeterminate), not bare Async::Stop.
    server = FakeServer.new(workers: 2, stall_on_put_workers: [1])
    Async do
      config = GlyphaStore::ClientConfig.defaults
      config.port = server.port
      config.request_timeout = 5.0
      client = GlyphaStore::AsyncClient.connect(config)
      keys = [nil, nil]
      candidate = 0
      while keys.any?(&:nil?)
        key = format("bcancel%08d", candidate).b
        w = client.worker_for(key)
        keys[w] ||= key
        candidate += 1
      end
      responses = nil
      task = Async do
        responses = client.execute_batch(
          [
            GlyphaStore::PipelineRequest.new(
              opcode: GlyphaStore::PipelineOpcode::PUT, key: keys[0], value: "a".b
            ),
            GlyphaStore::PipelineRequest.new(
              opcode: GlyphaStore::PipelineOpcode::PUT, key: keys[1], value: "b".b
            )
          ]
        )
      end
      Async::Task.current.sleep(0.05)
      task.stop
      task.wait
      assert responses
      assert_equal 2, responses.length
      assert_equal GlyphaStore::PipelineOutcome::SUCCEEDED, responses[0].outcome
      assert_equal GlyphaStore::PipelineOutcome::INDETERMINATE, responses[1].outcome
      assert responses[1].error
      assert_operator responses[1].error.bytes_sent, :>, 0
      assert_equal GlyphaStore::Retryability::RECONCILE_FIRST, responses[1].error.retryability
      assert_equal GlyphaStore::MutationOutcome::INDETERMINATE, responses[1].error.mutation_outcome
      assert_equal "a".b, client.get(keys[0])
      client.close
    end
  ensure
    server&.join
  end

  def test_async_internal_error_mutation_stamps_bytes_sent
    server = FakeServer.new(internal_error_on_put: true)
    Async do
      config = GlyphaStore::ClientConfig.defaults
      config.port = server.port
      client = GlyphaStore::AsyncClient.connect(config)
      result = client.put("key".b, "value".b)
      assert result.indeterminate?
      assert_equal GlyphaStore::Retryability::RECONCILE_FIRST, result.error.retryability
      assert_equal GlyphaStore::MutationOutcome::INDETERMINATE, result.error.mutation_outcome
      assert_operator result.error.bytes_sent, :>, 0
      client.close
    end
  ensure
    server&.join
  end
end
