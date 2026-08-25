# Test suites, crash harnesses, CLI contract tests, and check target.
if(BUILD_TESTING)
    add_executable(glyphastore_tests
        ${CMAKE_SOURCE_DIR}/src/experimental/paired_reactor.cpp
        ${CMAKE_SOURCE_DIR}/src/experimental/paired_shard.cpp
        ${CMAKE_SOURCE_DIR}/tests/test_main.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/compaction_intent_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/compaction_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/adaptive_batch_sizer_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/index_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/index_hash_seed_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/worker_routing_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/hot_record_table_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/paired_shard_prototype_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/pair_read_generation_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/manifest_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/mutation_slot_pool_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/mutation_state_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/completion_policy_recovery_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/maintenance_controller_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/maintenance_controller_pressure_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/namespace_audit_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/persistence_filesystem_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/segment_file_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/store_verify_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/store_backup_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/store_migrate_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/store_repair_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/cli_arguments_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/daemon_config_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/daemon_log_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/security_audit_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/openbsd_sandbox_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/authz_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/peercred_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/abuse_limits_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/bounded_mpsc_queue_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/connection_handoff_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/client_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/client_error_taxonomy_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/client_tls_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/durable_flush_coordinator_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/latency_histogram_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/record_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/resource_limits_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/server_protocol_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/segment_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/segment_header_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/segment_catalog_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/tls_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/trusted_read_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/vacuum_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/worker_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/format_compatibility_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/unit/released_artifact_compat_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/compaction_builder_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/compaction_recovery_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/concurrency_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/runtime_equivalence_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/optimization_regression_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/paired_store_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/persistence_recovery_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/persistence_recovery_compaction_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/persistence_recovery_crash_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/persistence_recovery_test_support.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/paired_reactor_prototype_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/rebuild_vacuum_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/server_reactor_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/server_reactor_durable_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/server_reactor_security_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/integration/store_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/property/history_property_tests.cpp
        ${CMAKE_SOURCE_DIR}/tests/concurrency/linearizability_tests.cpp
    )
    target_link_libraries(glyphastore_tests
        PRIVATE GlyphaStore::server GlyphaStore::client glyphastore_cli_support glyphastore_project_options
    )
    if(GLYPHASTORE_TLS_ENABLED)
        target_link_libraries(glyphastore_tests PRIVATE GlyphaStore::tls)
    endif()
    target_include_directories(glyphastore_tests PRIVATE
        "${CMAKE_SOURCE_DIR}/src"
        "${CMAKE_SOURCE_DIR}/tests"
    )
    target_compile_definitions(glyphastore_tests
        PRIVATE GLYPHASTORE_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
    )
    glyphastore_set_warnings(glyphastore_tests)
    glyphastore_enable_hardening(glyphastore_tests)
    glyphastore_enable_ipo(glyphastore_tests)
    add_test(NAME glyphastore_tests COMMAND glyphastore_tests)
    set_target_properties(glyphastore_tests PROPERTIES FOLDER "Quality")
    glyphastore_xcode_scheme(glyphastore_tests)

    if(NOT CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" AND NOT GLYPHASTORE_ENABLE_TSAN)
        # FreeBSD libc++ rejects replaced sized/aligned operator delete as non-usual when
        # paired with __builtin_operator_delete (GS residual: allocation fault suite is
        # Linux/macOS primary). TSan's runtime already owns operator new/delete, so the
        # fault-injection overrides cannot link under -fsanitize=thread.
        add_executable(glyphastore_allocation_fault_tests
            ${CMAKE_SOURCE_DIR}/tests/quality/allocation_fault_tests.cpp
        )
        target_link_libraries(glyphastore_allocation_fault_tests
            PRIVATE GlyphaStore::server glyphastore_project_options
        )
        target_include_directories(glyphastore_allocation_fault_tests PRIVATE
            "${CMAKE_SOURCE_DIR}/src"
            "${CMAKE_SOURCE_DIR}/tests"
        )
        glyphastore_set_warnings(glyphastore_allocation_fault_tests)
        glyphastore_enable_hardening(glyphastore_allocation_fault_tests)
        glyphastore_enable_ipo(glyphastore_allocation_fault_tests)
        set_target_properties(glyphastore_allocation_fault_tests PROPERTIES FOLDER "Quality")
        add_test(NAME glyphastore_allocation_fault_tests COMMAND glyphastore_allocation_fault_tests)
        set_tests_properties(glyphastore_allocation_fault_tests PROPERTIES
            TIMEOUT 180
            LABELS "fault-injection"
        )
        glyphastore_xcode_scheme(glyphastore_allocation_fault_tests)
    endif()

    add_executable(glyphastore_crash_persistence ${CMAKE_SOURCE_DIR}/tests/crash/crash_persistence.cpp)
    target_link_libraries(glyphastore_crash_persistence PRIVATE GlyphaStore::core glyphastore_project_options)
    target_include_directories(glyphastore_crash_persistence PRIVATE "${CMAKE_SOURCE_DIR}/tests")
    glyphastore_set_warnings(glyphastore_crash_persistence)
    glyphastore_enable_hardening(glyphastore_crash_persistence)
    glyphastore_enable_ipo(glyphastore_crash_persistence)
    set_target_properties(glyphastore_crash_persistence PROPERTIES FOLDER "Quality")
    add_test(NAME glyphastore_crash_sync COMMAND glyphastore_crash_persistence --mode matrix)
    add_test(NAME glyphastore_crash_periodic COMMAND glyphastore_crash_persistence --mode periodic-matrix)
    add_test(NAME glyphastore_crash_group COMMAND glyphastore_crash_persistence --mode group-matrix)
    # Sync matrix is large; TSan instrumentation of recovery/encode paths routinely
    # exceeds 10 minutes on hosted/macOS runners even when making forward progress.
    set_tests_properties(glyphastore_crash_sync PROPERTIES TIMEOUT 2400 LABELS "crash")
    set_tests_properties(glyphastore_crash_periodic glyphastore_crash_group PROPERTIES TIMEOUT 600
                         LABELS "crash")

    add_executable(glyphastore_crash_daemon ${CMAKE_SOURCE_DIR}/tests/crash/crash_daemon.cpp)
    target_link_libraries(glyphastore_crash_daemon
        PRIVATE GlyphaStore::client GlyphaStore::core GlyphaStore::server glyphastore_project_options
    )
    glyphastore_set_warnings(glyphastore_crash_daemon)
    glyphastore_enable_hardening(glyphastore_crash_daemon)
    glyphastore_enable_ipo(glyphastore_crash_daemon)
    set_target_properties(glyphastore_crash_daemon PROPERTIES FOLDER "Quality")
    add_test(NAME glyphastore_crash_daemon_sync
        COMMAND glyphastore_crash_daemon --daemon $<TARGET_FILE:glyphastored> --storage durable-sync)
    add_test(NAME glyphastore_crash_daemon_group
        COMMAND glyphastore_crash_daemon --daemon $<TARGET_FILE:glyphastored> --storage durable-group)
    add_test(NAME glyphastore_crash_daemon_periodic
        COMMAND glyphastore_crash_daemon --daemon $<TARGET_FILE:glyphastored> --storage durable-periodic)
    add_test(NAME glyphastore_crash_daemon_pre_commit
        COMMAND glyphastore_crash_daemon --daemon $<TARGET_FILE:glyphastored> --storage durable-sync
                --checkpoint pre-commit)
    add_test(NAME glyphastore_crash_daemon_erase
        COMMAND glyphastore_crash_daemon --daemon $<TARGET_FILE:glyphastored> --storage durable-sync
                --checkpoint post-ack-erase)
    set_tests_properties(
        glyphastore_crash_daemon_sync
        glyphastore_crash_daemon_group
        glyphastore_crash_daemon_periodic
        glyphastore_crash_daemon_pre_commit
        glyphastore_crash_daemon_erase
        PROPERTIES TIMEOUT 120 LABELS "crash"
    )

    add_executable(glyphastore_crash_backup ${CMAKE_SOURCE_DIR}/tests/crash/crash_backup.cpp)
    target_link_libraries(glyphastore_crash_backup PRIVATE GlyphaStore::core glyphastore_project_options)
    target_include_directories(glyphastore_crash_backup PRIVATE "${CMAKE_SOURCE_DIR}/tests")
    glyphastore_set_warnings(glyphastore_crash_backup)
    glyphastore_enable_hardening(glyphastore_crash_backup)
    glyphastore_enable_ipo(glyphastore_crash_backup)
    set_target_properties(glyphastore_crash_backup PROPERTIES FOLDER "Quality")
    add_test(NAME glyphastore_crash_backup COMMAND glyphastore_crash_backup)
    set_tests_properties(glyphastore_crash_backup PROPERTIES TIMEOUT 120 LABELS "crash")

    add_executable(glyphastore_crash_backup_wire ${CMAKE_SOURCE_DIR}/tests/crash/crash_backup_wire.cpp)
    target_link_libraries(glyphastore_crash_backup_wire
        PRIVATE GlyphaStore::client GlyphaStore::core GlyphaStore::server glyphastore_project_options
    )
    target_include_directories(glyphastore_crash_backup_wire PRIVATE "${CMAKE_SOURCE_DIR}/tests")
    glyphastore_set_warnings(glyphastore_crash_backup_wire)
    glyphastore_enable_hardening(glyphastore_crash_backup_wire)
    glyphastore_enable_ipo(glyphastore_crash_backup_wire)
    set_target_properties(glyphastore_crash_backup_wire PROPERTIES FOLDER "Quality")
    add_test(NAME glyphastore_crash_backup_wire COMMAND glyphastore_crash_backup_wire)
    set_tests_properties(glyphastore_crash_backup_wire PROPERTIES TIMEOUT 180 LABELS "crash")

    add_executable(glyphastore_crash_backup_daemon ${CMAKE_SOURCE_DIR}/tests/crash/crash_backup_daemon.cpp)
    target_link_libraries(glyphastore_crash_backup_daemon
        PRIVATE GlyphaStore::client GlyphaStore::core GlyphaStore::server glyphastore_project_options
    )
    target_include_directories(glyphastore_crash_backup_daemon PRIVATE "${CMAKE_SOURCE_DIR}/tests")
    glyphastore_set_warnings(glyphastore_crash_backup_daemon)
    glyphastore_enable_hardening(glyphastore_crash_backup_daemon)
    glyphastore_enable_ipo(glyphastore_crash_backup_daemon)
    set_target_properties(glyphastore_crash_backup_daemon PROPERTIES FOLDER "Quality")
    add_test(NAME glyphastore_crash_backup_daemon
             COMMAND glyphastore_crash_backup_daemon --daemon $<TARGET_FILE:glyphastored>)
    set_tests_properties(glyphastore_crash_backup_daemon PROPERTIES TIMEOUT 180 LABELS "crash"
                         DEPENDS glyphastored)
endif()

if(BUILD_TESTING)
    add_test(NAME glyphastore_cli_daemon_help COMMAND glyphastored --help)
    set_tests_properties(glyphastore_cli_daemon_help PROPERTIES
        PASS_REGULAR_EXPRESSION "GlyphaStore native binary key-value server"
    )
    add_test(NAME glyphastore_cli_daemon_version COMMAND glyphastored --version)
    set_tests_properties(glyphastore_cli_daemon_version PROPERTIES
        PASS_REGULAR_EXPRESSION "glyphastored ${PROJECT_VERSION}"
    )
    add_test(NAME glyphastore_cli_inspect_help COMMAND glyphastore_inspect_segment --help)
    set_tests_properties(glyphastore_cli_inspect_help PROPERTIES
        PASS_REGULAR_EXPRESSION "Inspect a GlyphaStore durable Segment file"
    )
    add_test(NAME glyphastore_cli_verify_help COMMAND glyphastore_verify_store --help)
    set_tests_properties(glyphastore_cli_verify_help PROPERTIES
        PASS_REGULAR_EXPRESSION "Verify a GlyphaStore durable data directory"
    )
    add_test(NAME glyphastore_cli_backup_help COMMAND glyphastore_backup_store --help)
    set_tests_properties(glyphastore_cli_backup_help PROPERTIES
        PASS_REGULAR_EXPRESSION "Offline backup"
    )
    add_test(NAME glyphastore_cli_migrate_help COMMAND glyphastore_migrate_store --help)
    set_tests_properties(glyphastore_cli_migrate_help PROPERTIES
        PASS_REGULAR_EXPRESSION "Offline Worker reshard"
    )
    add_test(NAME glyphastore_cli_repair_help COMMAND glyphastore_repair_store --help)
    set_tests_properties(glyphastore_cli_repair_help PROPERTIES
        PASS_REGULAR_EXPRESSION "Offline repair"
    )
    add_test(NAME glyphastore_cli_rebuild_help COMMAND glyphastore_rebuild_index --help)
    set_tests_properties(glyphastore_cli_rebuild_help PROPERTIES
        PASS_REGULAR_EXPRESSION "Offline Index rebuild is not supported"
    )
    add_test(NAME glyphastore_cli_rebuild_not_implemented
        COMMAND glyphastore_rebuild_index -- /tmp/does-not-matter.glypha
    )
    set_tests_properties(glyphastore_cli_rebuild_not_implemented PROPERTIES WILL_FAIL TRUE)
    add_test(NAME glyphastore_cli_rejects_invalid_workers COMMAND glyphastored --workers 257)
    set_tests_properties(glyphastore_cli_rejects_invalid_workers PROPERTIES WILL_FAIL TRUE)
    add_test(NAME glyphastore_cli_daemon_rejects_missing_data_directory
        COMMAND glyphastored --storage-mode durable-sync
    )
    set_tests_properties(glyphastore_cli_daemon_rejects_missing_data_directory PROPERTIES WILL_FAIL TRUE)
    add_test(NAME glyphastore_cli_daemon_rejects_durable_options_in_volatile_mode
        COMMAND glyphastored --data-dir unused
    )
    set_tests_properties(glyphastore_cli_daemon_rejects_durable_options_in_volatile_mode
        PROPERTIES WILL_FAIL TRUE
    )
    add_test(NAME glyphastore_cli_daemon_rejects_resource_flags_in_volatile_mode
        COMMAND glyphastored --max-store-bytes 1GiB
    )
    set_tests_properties(glyphastore_cli_daemon_rejects_resource_flags_in_volatile_mode
        PROPERTIES WILL_FAIL TRUE
    )
    add_test(NAME glyphastore_cli_daemon_rejects_zero_group_max_records
        COMMAND glyphastored --storage-mode durable-group --data-dir /tmp/glyphastore-cli-group
                --group-max-records 0
    )
    set_tests_properties(glyphastore_cli_daemon_rejects_zero_group_max_records
        PROPERTIES WILL_FAIL TRUE
    )
    add_test(NAME glyphastore_cli_daemon_rejects_zero_durable_mutation_queue
        COMMAND glyphastored --durable-mutation-queue-capacity 0
    )
    set_tests_properties(glyphastore_cli_daemon_rejects_zero_durable_mutation_queue
        PROPERTIES WILL_FAIL TRUE
    )
    add_test(NAME glyphastore_cli_daemon_rejects_unknown_config_key
        COMMAND glyphastored --config "${CMAKE_SOURCE_DIR}/tests/fixtures/daemon-unknown-key.conf"
    )
    set_tests_properties(glyphastore_cli_daemon_rejects_unknown_config_key
        PROPERTIES WILL_FAIL TRUE
    )
    add_test(NAME glyphastore_cli_daemon_dump_config
        COMMAND glyphastored --dump-config --port 0
    )
    set_tests_properties(glyphastore_cli_daemon_dump_config PROPERTIES
        PASS_REGULAR_EXPRESSION "GlyphaStore/config.*port=0"
    )
    set_tests_properties(
        glyphastore_cli_daemon_help
        glyphastore_cli_daemon_version
        glyphastore_cli_daemon_rejects_missing_data_directory
        glyphastore_cli_daemon_rejects_durable_options_in_volatile_mode
        glyphastore_cli_daemon_rejects_resource_flags_in_volatile_mode
        glyphastore_cli_daemon_rejects_zero_group_max_records
        glyphastore_cli_daemon_rejects_zero_durable_mutation_queue
        glyphastore_cli_daemon_rejects_unknown_config_key
        glyphastore_cli_daemon_dump_config
        glyphastore_cli_inspect_help
        glyphastore_cli_rebuild_help
        glyphastore_cli_rejects_invalid_workers
        PROPERTIES TIMEOUT 10
    )
endif()

if(BUILD_TESTING)
    set(glyphastore_check_dependencies
        glyphastore_tests
        glyphastore_crash_persistence
        glyphastore_crash_daemon
        glyphastore_crash_backup
        glyphastore_crash_backup_wire
        glyphastore_crash_backup_daemon
        glyphastored
        glyphastore_inspect_segment
        glyphastore_verify_store
        glyphastore_backup_store
        glyphastore_migrate_store
        glyphastore_repair_store
        glyphastore_rebuild_index
    )
    if(TARGET glyphastore_benchmarks)
        list(APPEND glyphastore_check_dependencies
            glyphastore_benchmarks
            glyphastore_server_benchmarks
        )
    endif()
    add_custom_target(check
        COMMAND "${CMAKE_CTEST_COMMAND}" -C "$<CONFIG>" --output-on-failure
        DEPENDS ${glyphastore_check_dependencies}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
        USES_TERMINAL
        COMMENT "Running GlyphaStore tests with CTest"
    )
    set_target_properties(check PROPERTIES FOLDER "Quality")
    glyphastore_xcode_scheme(check)
endif()
