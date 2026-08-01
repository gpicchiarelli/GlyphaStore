cmake_minimum_required(VERSION 3.25)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" GLYPHASTORE_VERSION)
string(STRIP "${GLYPHASTORE_VERSION}" GLYPHASTORE_VERSION)
if(NOT GLYPHASTORE_VERSION MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
    message(FATAL_ERROR "VERSION must contain exactly one semantic version such as 1.2.3")
endif()

project(GlyphaStore VERSION "${GLYPHASTORE_VERSION}" LANGUAGES CXX)

find_package(Threads REQUIRED)

if(DEFINED CMAKE_CXX_STANDARD AND CMAKE_CXX_STANDARD LESS 23)
    message(FATAL_ERROR "GlyphaStore requires ISO C++23 or newer")
endif()
if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 23)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
include(ProjectOptions)
include(CompilerWarnings)
include(Sanitizers)
include(Hardening)
include(XcodeSupport)
include(FindGlyphaStoreTls)

glyphastore_project_options()
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set(CMAKE_PREDEFINED_TARGETS_FOLDER "CMake")

if(GLYPHASTORE_ENABLE_TLS STREQUAL "ON")
    if(NOT GLYPHASTORE_TLS_FOUND)
        message(FATAL_ERROR
            "GLYPHASTORE_ENABLE_TLS=ON but no TLS library was found. "
            "On OpenBSD install/use system LibreSSL; elsewhere provide OpenSSL 3.x "
            "(set OPENSSL_ROOT_DIR if needed).")
    endif()
    set(GLYPHASTORE_TLS_ENABLED TRUE)
elseif(GLYPHASTORE_ENABLE_TLS STREQUAL "OFF")
    set(GLYPHASTORE_TLS_ENABLED FALSE)
elseif(GLYPHASTORE_ENABLE_TLS STREQUAL "AUTO")
    set(GLYPHASTORE_TLS_ENABLED ${GLYPHASTORE_TLS_FOUND})
else()
    message(FATAL_ERROR "GLYPHASTORE_ENABLE_TLS must be AUTO, ON, or OFF")
endif()

if(GLYPHASTORE_TLS_ENABLED)
    message(STATUS "GlyphaStore TLS: enabled (${GLYPHASTORE_TLS_BACKEND})")
else()
    if(GLYPHASTORE_ENABLE_TLS STREQUAL "AUTO")
        message(STATUS
            "GlyphaStore TLS: disabled (no LibreSSL/OpenSSL 3.x found; "
            "cleartext daemon only. Set OPENSSL_ROOT_DIR or GLYPHASTORE_ENABLE_TLS=ON)")
    else()
        message(STATUS "GlyphaStore TLS: disabled (GLYPHASTORE_ENABLE_TLS=OFF)")
    endif()
endif()

add_library(glyphastore_cli_support
    src/cli/arguments.cpp
)
target_compile_features(glyphastore_cli_support PUBLIC cxx_std_23)
target_include_directories(glyphastore_cli_support
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
target_link_libraries(glyphastore_cli_support PRIVATE glyphastore_project_options)
glyphastore_set_warnings(glyphastore_cli_support)
glyphastore_enable_hardening(glyphastore_cli_support)
set_target_properties(glyphastore_cli_support PROPERTIES FOLDER "Libraries/Internal")

add_library(glyphastore_core
    src/core/worker_routing.cpp
    src/index/index.cpp
    src/index/index_hash_seed.cpp
    src/index/key_arena.cpp
    src/index/swiss_table.cpp
    src/persistence/bootstrap.cpp
    src/persistence/compaction/layout.cpp
    src/persistence/compaction/manifest_transition.cpp
    src/persistence/compaction/planner.cpp
    src/persistence/compaction/resource_estimator.cpp
    src/persistence/compaction_builder.cpp
    src/persistence/compaction_intent.cpp
    src/persistence/filesystem.cpp
    src/persistence/manifest.cpp
    src/persistence/namespace_audit.cpp
    src/persistence/durable_flush_coordinator.cpp
    src/persistence/recovery/recovery.cpp
    src/persistence/recovery/recovery_budget.cpp
    src/persistence/recovery/recovery_catalog.cpp
    src/persistence/recovery/recovery_index_builder.cpp
    src/persistence/recovery/recovery_scanner.cpp
    src/persistence/resource_limits.cpp
    src/persistence/runtime_catalog.cpp
    src/persistence/segment_file.cpp
    src/persistence/store_verify.cpp
    src/persistence/store_backup.cpp
    src/persistence/store_migrate.cpp
    src/persistence/store_repair.cpp
    src/segment/crc32c.cpp
    src/segment/global_manager.cpp
    src/segment/record.cpp
    src/segment/segment.cpp
    src/segment/segment_header.cpp
    src/store/maintenance.cpp
    src/store/paired/mutation_slot_pool.cpp
    src/store/paired/read_generation.cpp
    src/store/paired/shard_pair_runtime.cpp
    src/store/store.cpp
    src/vacuum/vacuum.cpp
    src/worker/pool.cpp
    src/worker/topology.cpp
    src/worker/worker.cpp
)
add_library(GlyphaStore::core ALIAS glyphastore_core)
set_target_properties(glyphastore_core PROPERTIES EXPORT_NAME core)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set_source_files_properties(src/persistence/segment_file.cpp PROPERTIES COMPILE_DEFINITIONS _GNU_SOURCE)
endif()

target_compile_features(glyphastore_core PUBLIC cxx_std_23)
target_include_directories(glyphastore_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(glyphastore_core
    PUBLIC Threads::Threads
    PRIVATE $<BUILD_INTERFACE:glyphastore_project_options>
)
if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD|OpenBSD")
    # FindThreads may legitimately resolve pthread symbols from libc and leave
    # the compiler/linker flag empty, while BSD still requires explicit mode.
    target_compile_options(glyphastore_core PUBLIC -pthread)
    target_link_options(glyphastore_core PUBLIC -pthread)
endif()
glyphastore_set_warnings(glyphastore_core)
glyphastore_enable_hardening(glyphastore_core)
glyphastore_enable_ipo(glyphastore_core)
set_target_properties(glyphastore_core PROPERTIES FOLDER "Libraries")

add_library(glyphastore_wire
    src/server/protocol.cpp
)
add_library(GlyphaStore::wire ALIAS glyphastore_wire)
set_target_properties(glyphastore_wire PROPERTIES EXPORT_NAME wire)
target_compile_features(glyphastore_wire PUBLIC cxx_std_23)
target_include_directories(glyphastore_wire
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(glyphastore_wire PRIVATE $<BUILD_INTERFACE:glyphastore_project_options>)
glyphastore_set_warnings(glyphastore_wire)
glyphastore_enable_hardening(glyphastore_wire)
glyphastore_enable_ipo(glyphastore_wire)
set_target_properties(glyphastore_wire PROPERTIES FOLDER "Libraries")

add_library(glyphastore_tls_layer
    src/server/tls.cpp
)
add_library(GlyphaStore::tls_layer ALIAS glyphastore_tls_layer)
set_target_properties(glyphastore_tls_layer PROPERTIES EXPORT_NAME tls_layer)
target_compile_features(glyphastore_tls_layer PUBLIC cxx_std_23)
target_include_directories(glyphastore_tls_layer
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(glyphastore_tls_layer
    PUBLIC GlyphaStore::core
    PRIVATE $<BUILD_INTERFACE:glyphastore_project_options>
)
if(GLYPHASTORE_TLS_ENABLED)
    target_compile_definitions(glyphastore_tls_layer
        PUBLIC GLYPHASTORE_HAS_TLS=1
        PRIVATE "GLYPHASTORE_TLS_BACKEND_NAME=\"${GLYPHASTORE_TLS_BACKEND}\""
    )
    target_link_libraries(glyphastore_tls_layer PUBLIC GlyphaStore::tls)
else()
    target_compile_definitions(glyphastore_tls_layer PUBLIC GLYPHASTORE_HAS_TLS=0)
endif()
glyphastore_set_warnings(glyphastore_tls_layer)
glyphastore_enable_hardening(glyphastore_tls_layer)
glyphastore_enable_ipo(glyphastore_tls_layer)
set_target_properties(glyphastore_tls_layer PROPERTIES FOLDER "Libraries")

add_library(glyphastore_client
    src/client/client.cpp
)
add_library(GlyphaStore::client ALIAS glyphastore_client)
set_target_properties(glyphastore_client PROPERTIES EXPORT_NAME client)
target_compile_features(glyphastore_client PUBLIC cxx_std_23)
target_include_directories(glyphastore_client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(glyphastore_client
    PRIVATE GlyphaStore::wire GlyphaStore::tls_layer $<BUILD_INTERFACE:glyphastore_project_options>
)
glyphastore_set_warnings(glyphastore_client)
glyphastore_enable_hardening(glyphastore_client)
glyphastore_enable_ipo(glyphastore_client)
set_target_properties(glyphastore_client PROPERTIES FOLDER "Libraries")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(GLYPHASTORE_POLLER_SOURCE src/server/poller_epoll.cpp)
elseif(APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
    set(GLYPHASTORE_POLLER_SOURCE src/server/poller_kqueue.cpp)
else()
    message(FATAL_ERROR "glyphastored requires epoll on Linux or kqueue on macOS/FreeBSD/OpenBSD")
endif()

add_library(glyphastore_server_core
    src/server/abuse_limits.cpp
    src/server/authz.cpp
    src/server/connection_handoff.cpp
    src/server/daemon_config.cpp
    src/server/daemon_log.cpp
    src/server/disk_read_executor.cpp
    src/server/pair_writer.cpp
    src/server/openbsd_sandbox.cpp
    src/server/peercred.cpp
    src/server/reactor.cpp
    src/server/reactor_factory.cpp
    src/server/security_audit.cpp
    src/server/server.cpp
    src/server/server_builder.cpp
    src/server/server_health.cpp
    src/server/server_lifecycle.cpp
    src/server/server_stats.cpp
    src/server/socket.cpp
    src/server/thread_affinity.cpp
    src/server/wakeup.cpp
    ${GLYPHASTORE_POLLER_SOURCE}
)
add_library(GlyphaStore::server ALIAS glyphastore_server_core)
set_target_properties(glyphastore_server_core PROPERTIES EXPORT_NAME server)
target_compile_features(glyphastore_server_core PUBLIC cxx_std_23)
target_include_directories(glyphastore_server_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(glyphastore_server_core
    PUBLIC GlyphaStore::core GlyphaStore::wire GlyphaStore::tls_layer
    PRIVATE glyphastore_cli_support $<BUILD_INTERFACE:glyphastore_project_options>
)
target_compile_definitions(glyphastore_server_core PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_server_core)
glyphastore_enable_hardening(glyphastore_server_core)
glyphastore_enable_ipo(glyphastore_server_core)
set_target_properties(glyphastore_server_core PROPERTIES FOLDER "Libraries")

add_executable(glyphastored src/server/daemon_main.cpp)
target_link_libraries(glyphastored
    PRIVATE GlyphaStore::server glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastored PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastored)
glyphastore_enable_hardening(glyphastored)
glyphastore_enable_ipo(glyphastored)
set_target_properties(glyphastored PROPERTIES FOLDER "Applications")
glyphastore_xcode_scheme(glyphastored ARGUMENTS
    --bind 127.0.0.1 --port 7379 --workers 2
)

add_executable(glyphastore_demo src/main.cpp)
target_link_libraries(glyphastore_demo PRIVATE GlyphaStore::core glyphastore_project_options)
glyphastore_set_warnings(glyphastore_demo)
glyphastore_enable_hardening(glyphastore_demo)
glyphastore_enable_ipo(glyphastore_demo)
set_target_properties(glyphastore_demo PROPERTIES FOLDER "Applications")
glyphastore_xcode_scheme(glyphastore_demo)

include(CTest)
if(BUILD_TESTING)
    add_executable(glyphastore_tests
        src/experimental/paired_reactor.cpp
        src/experimental/paired_shard.cpp
        tests/test_main.cpp
        tests/unit/compaction_intent_tests.cpp
        tests/unit/compaction_tests.cpp
        tests/unit/adaptive_batch_sizer_tests.cpp
        tests/unit/index_tests.cpp
        tests/unit/index_hash_seed_tests.cpp
        tests/unit/worker_routing_tests.cpp
        tests/unit/hot_record_table_tests.cpp
        tests/unit/paired_shard_prototype_tests.cpp
        tests/unit/pair_read_generation_tests.cpp
        tests/unit/manifest_tests.cpp
        tests/unit/mutation_slot_pool_tests.cpp
        tests/unit/maintenance_controller_tests.cpp
        tests/unit/namespace_audit_tests.cpp
        tests/unit/persistence_filesystem_tests.cpp
        tests/unit/segment_file_tests.cpp
        tests/unit/store_verify_tests.cpp
        tests/unit/store_backup_tests.cpp
        tests/unit/store_migrate_tests.cpp
        tests/unit/store_repair_tests.cpp
        tests/unit/cli_arguments_tests.cpp
        tests/unit/daemon_config_tests.cpp
        tests/unit/daemon_log_tests.cpp
        tests/unit/security_audit_tests.cpp
        tests/unit/openbsd_sandbox_tests.cpp
        tests/unit/authz_tests.cpp
        tests/unit/peercred_tests.cpp
        tests/unit/abuse_limits_tests.cpp
        tests/unit/bounded_mpsc_queue_tests.cpp
        tests/unit/connection_handoff_tests.cpp
        tests/unit/client_tests.cpp
        tests/unit/client_error_taxonomy_tests.cpp
        tests/unit/client_tls_tests.cpp
        tests/unit/durable_flush_coordinator_tests.cpp
        tests/unit/latency_histogram_tests.cpp
        tests/unit/record_tests.cpp
        tests/unit/resource_limits_tests.cpp
        tests/unit/server_protocol_tests.cpp
        tests/unit/segment_tests.cpp
        tests/unit/segment_header_tests.cpp
        tests/unit/segment_catalog_tests.cpp
        tests/unit/tls_tests.cpp
        tests/unit/trusted_read_tests.cpp
        tests/unit/vacuum_tests.cpp
        tests/unit/worker_tests.cpp
        tests/unit/format_compatibility_tests.cpp
        tests/unit/released_artifact_compat_tests.cpp
        tests/integration/compaction_builder_tests.cpp
        tests/integration/compaction_recovery_tests.cpp
        tests/integration/concurrency_tests.cpp
        tests/integration/optimization_regression_tests.cpp
        tests/integration/paired_store_tests.cpp
        tests/integration/persistence_recovery_tests.cpp
        tests/integration/paired_reactor_prototype_tests.cpp
        tests/integration/rebuild_vacuum_tests.cpp
        tests/integration/server_reactor_tests.cpp
        tests/integration/store_tests.cpp
        tests/property/history_property_tests.cpp
    )
    target_link_libraries(glyphastore_tests
        PRIVATE GlyphaStore::server GlyphaStore::client glyphastore_cli_support glyphastore_project_options
    )
    if(GLYPHASTORE_TLS_ENABLED)
        target_link_libraries(glyphastore_tests PRIVATE GlyphaStore::tls)
    endif()
    target_include_directories(glyphastore_tests PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests"
    )
    target_compile_definitions(glyphastore_tests
        PRIVATE GLYPHASTORE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
    )
    glyphastore_set_warnings(glyphastore_tests)
    glyphastore_enable_hardening(glyphastore_tests)
    glyphastore_enable_ipo(glyphastore_tests)
    add_test(NAME glyphastore_tests COMMAND glyphastore_tests)
    set_target_properties(glyphastore_tests PROPERTIES FOLDER "Quality")
    glyphastore_xcode_scheme(glyphastore_tests)

    add_executable(glyphastore_allocation_fault_tests
        tests/quality/allocation_fault_tests.cpp
    )
    target_link_libraries(glyphastore_allocation_fault_tests
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    glyphastore_set_warnings(glyphastore_allocation_fault_tests)
    glyphastore_enable_hardening(glyphastore_allocation_fault_tests)
    set_target_properties(glyphastore_allocation_fault_tests PROPERTIES FOLDER "Quality")
    add_test(NAME glyphastore_allocation_fault_tests COMMAND glyphastore_allocation_fault_tests)
    set_tests_properties(glyphastore_allocation_fault_tests PROPERTIES
        TIMEOUT 180
        LABELS "fault-injection"
    )
    glyphastore_xcode_scheme(glyphastore_allocation_fault_tests)

    add_executable(glyphastore_crash_persistence tests/crash/crash_persistence.cpp)
    target_link_libraries(glyphastore_crash_persistence PRIVATE GlyphaStore::core glyphastore_project_options)
    target_include_directories(glyphastore_crash_persistence PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    glyphastore_set_warnings(glyphastore_crash_persistence)
    glyphastore_enable_hardening(glyphastore_crash_persistence)
    set_target_properties(glyphastore_crash_persistence PROPERTIES FOLDER "Quality")
    add_test(NAME glyphastore_crash_sync COMMAND glyphastore_crash_persistence --mode matrix)
    add_test(NAME glyphastore_crash_periodic COMMAND glyphastore_crash_persistence --mode periodic-matrix)
    add_test(NAME glyphastore_crash_group COMMAND glyphastore_crash_persistence --mode group-matrix)
    # Sync matrix is large; TSan instrumentation of recovery/encode paths routinely
    # exceeds 10 minutes on hosted/macOS runners even when making forward progress.
    set_tests_properties(glyphastore_crash_sync PROPERTIES TIMEOUT 2400 LABELS "crash")
    set_tests_properties(glyphastore_crash_periodic glyphastore_crash_group PROPERTIES TIMEOUT 600
                         LABELS "crash")

    add_executable(glyphastore_crash_daemon tests/crash/crash_daemon.cpp)
    target_link_libraries(glyphastore_crash_daemon
        PRIVATE GlyphaStore::client GlyphaStore::core GlyphaStore::server glyphastore_project_options
    )
    glyphastore_set_warnings(glyphastore_crash_daemon)
    glyphastore_enable_hardening(glyphastore_crash_daemon)
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
endif()

add_executable(glyphastore_inspect_segment tools/inspect_segment.cpp)
target_link_libraries(glyphastore_inspect_segment
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_inspect_segment PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_inspect_segment)
glyphastore_enable_hardening(glyphastore_inspect_segment)
glyphastore_enable_ipo(glyphastore_inspect_segment)
set_target_properties(glyphastore_inspect_segment PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_inspect_segment ARGUMENTS --help)

add_executable(glyphastore_verify_store tools/verify_store.cpp)
target_link_libraries(glyphastore_verify_store
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_verify_store PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_verify_store)
glyphastore_enable_hardening(glyphastore_verify_store)
glyphastore_enable_ipo(glyphastore_verify_store)
set_target_properties(glyphastore_verify_store PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_verify_store ARGUMENTS --help)

add_executable(glyphastore_backup_store tools/backup_store.cpp)
target_link_libraries(glyphastore_backup_store
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_backup_store PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_backup_store)
glyphastore_enable_hardening(glyphastore_backup_store)
glyphastore_enable_ipo(glyphastore_backup_store)
set_target_properties(glyphastore_backup_store PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_backup_store ARGUMENTS --help)

add_executable(glyphastore_migrate_store tools/migrate_store.cpp)
target_link_libraries(glyphastore_migrate_store
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_migrate_store PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_migrate_store)
glyphastore_enable_hardening(glyphastore_migrate_store)
glyphastore_enable_ipo(glyphastore_migrate_store)
set_target_properties(glyphastore_migrate_store PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_migrate_store ARGUMENTS --help)

add_executable(glyphastore_repair_store tools/repair_store.cpp)
target_link_libraries(glyphastore_repair_store
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_repair_store PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_repair_store)
glyphastore_enable_hardening(glyphastore_repair_store)
glyphastore_enable_ipo(glyphastore_repair_store)
set_target_properties(glyphastore_repair_store PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_repair_store ARGUMENTS --help)

add_executable(glyphastore_interop_client tools/interop_client.cpp)
target_link_libraries(glyphastore_interop_client
    PRIVATE GlyphaStore::client glyphastore_project_options
)
glyphastore_set_warnings(glyphastore_interop_client)
glyphastore_enable_hardening(glyphastore_interop_client)
glyphastore_enable_ipo(glyphastore_interop_client)
set_target_properties(glyphastore_interop_client PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_interop_client ARGUMENTS --help)

add_executable(glyphastore_rebuild_index tools/rebuild_index.cpp)
target_link_libraries(glyphastore_rebuild_index
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_rebuild_index PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_rebuild_index)
glyphastore_enable_hardening(glyphastore_rebuild_index)
glyphastore_enable_ipo(glyphastore_rebuild_index)
set_target_properties(glyphastore_rebuild_index PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_rebuild_index ARGUMENTS --help)

if(GLYPHASTORE_BUILD_BENCHMARKS OR BUILD_TESTING)
    add_executable(glyphastore_pgo_durable tools/pgo_durable.cpp)
    target_link_libraries(glyphastore_pgo_durable PRIVATE GlyphaStore::core glyphastore_project_options)
    glyphastore_set_warnings(glyphastore_pgo_durable)
    glyphastore_enable_hardening(glyphastore_pgo_durable)
    set_target_properties(glyphastore_pgo_durable PROPERTIES FOLDER "Tools")
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
        COMMAND glyphastored --config "${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/daemon-unknown-key.conf"
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

if(GLYPHASTORE_BUILD_BENCHMARKS)
    find_program(GIT_EXECUTABLE NAMES git)
    set(GLYPHASTORE_GIT_SHA "unknown")
    if(GIT_EXECUTABLE)
        execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            OUTPUT_VARIABLE GLYPHASTORE_GIT_SHA
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        execute_process(
            COMMAND ${GIT_EXECUTABLE} diff-index --quiet HEAD --
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE GLYPHASTORE_GIT_DIRTY
            ERROR_QUIET
        )
        if(GLYPHASTORE_GIT_DIRTY)
            string(APPEND GLYPHASTORE_GIT_SHA "-dirty")
        endif()
    endif()
    add_executable(glyphastore_benchmarks
        benchmarks/benchmark_main.cpp
        benchmarks/harness.cpp
        benchmarks/suite.cpp
    )
    target_link_libraries(glyphastore_benchmarks PRIVATE GlyphaStore::core glyphastore_project_options)
    target_include_directories(glyphastore_benchmarks PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks")
    target_compile_definitions(glyphastore_benchmarks PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}")
    glyphastore_set_warnings(glyphastore_benchmarks)
    glyphastore_enable_hardening(glyphastore_benchmarks)
    glyphastore_enable_ipo(glyphastore_benchmarks)
    set_target_properties(glyphastore_benchmarks PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_benchmarks RELEASE ARGUMENTS
        --filter store-put-get --ops 200000 --warmup 1 --repeats 7
    )
    add_executable(glyphastore_paired_benchmark
        benchmarks/paired_shard_benchmark.cpp
        src/experimental/paired_shard.cpp
    )
    target_link_libraries(
        glyphastore_paired_benchmark
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    target_include_directories(glyphastore_paired_benchmark
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_definitions(glyphastore_paired_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_paired_benchmark)
    glyphastore_enable_hardening(glyphastore_paired_benchmark)
    glyphastore_enable_ipo(glyphastore_paired_benchmark)
    set_target_properties(glyphastore_paired_benchmark PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_paired_benchmark RELEASE ARGUMENTS
        --ops 500000 --keys 4096 --value-bytes 64 --warmup 1 --repeats 7
    )
    add_executable(glyphastore_paired_reactor_benchmark
        benchmarks/paired_reactor_benchmark.cpp
        src/experimental/paired_reactor.cpp
        src/experimental/paired_shard.cpp
    )
    target_link_libraries(
        glyphastore_paired_reactor_benchmark
        PRIVATE GlyphaStore::server GlyphaStore::client glyphastore_project_options
    )
    target_include_directories(glyphastore_paired_reactor_benchmark
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )
    target_compile_definitions(glyphastore_paired_reactor_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_paired_reactor_benchmark)
    glyphastore_enable_hardening(glyphastore_paired_reactor_benchmark)
    glyphastore_enable_ipo(glyphastore_paired_reactor_benchmark)
    set_target_properties(glyphastore_paired_reactor_benchmark PROPERTIES FOLDER "Benchmarks")
    add_executable(glyphastore_compaction_benchmark
        benchmarks/compaction_benchmark.cpp
    )
    target_link_libraries(
        glyphastore_compaction_benchmark
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    target_compile_definitions(
        glyphastore_compaction_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_compaction_benchmark)
    glyphastore_enable_hardening(glyphastore_compaction_benchmark)
    glyphastore_enable_ipo(glyphastore_compaction_benchmark)
    set_target_properties(glyphastore_compaction_benchmark PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_compaction_benchmark RELEASE ARGUMENTS
        --repeats 3
    )
    add_executable(glyphastore_maintenance_benchmark
        benchmarks/maintenance_benchmark.cpp
    )
    target_link_libraries(
        glyphastore_maintenance_benchmark
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    target_compile_definitions(
        glyphastore_maintenance_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_maintenance_benchmark)
    glyphastore_enable_hardening(glyphastore_maintenance_benchmark)
    glyphastore_enable_ipo(glyphastore_maintenance_benchmark)
    set_target_properties(glyphastore_maintenance_benchmark PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_maintenance_benchmark RELEASE ARGUMENTS
        --repeats 3
    )
    add_executable(glyphastore_server_benchmarks
        benchmarks/server_benchmark.cpp
        benchmarks/harness.cpp
    )
    target_link_libraries(glyphastore_server_benchmarks
        PRIVATE GlyphaStore::server GlyphaStore::client glyphastore_project_options)
    target_include_directories(glyphastore_server_benchmarks
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks")
    target_compile_definitions(glyphastore_server_benchmarks
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}")
    glyphastore_set_warnings(glyphastore_server_benchmarks)
    glyphastore_enable_hardening(glyphastore_server_benchmarks)
    glyphastore_enable_ipo(glyphastore_server_benchmarks)
    set_target_properties(glyphastore_server_benchmarks PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_server_benchmarks RELEASE ARGUMENTS
        --ops 100000 --workers 2 --clients 2 --pipeline 32 --warmup 1 --repeats 7
    )
    if(BUILD_TESTING)
        foreach(distribution IN ITEMS uniform worker-affine single-worker zipf)
            add_test(
                NAME glyphastore_benchmark_parallel_${distribution}
                COMMAND glyphastore_benchmarks
                    --filter store-parallel-all
                    --ops 512
                    --workers 4
                    --threads 4
                    --distribution ${distribution}
                    --warmup 0
                    --repeats 1
            )
        endforeach()
        add_test(
            NAME glyphastore_benchmark_durable_smoke
            COMMAND glyphastore_benchmarks
                --filter store-durable-all
                --ops 128
                --workers 1
                --warmup 0
                --repeats 1
        )
        add_test(
            NAME glyphastore_benchmark_durable_parallel_smoke
            COMMAND glyphastore_benchmarks
                --filter store-durable-parallel-all
                --ops 128
                --workers 4
                --threads 4
                --distribution worker-affine
                --warmup 0
                --repeats 1
        )
        add_test(
            NAME glyphastore_benchmark_durable_group_smoke
            COMMAND glyphastore_benchmarks
                --filter store-durable-group-parallel-put
                --ops 64
                --workers 1
                --threads 32
                --distribution single-worker
                --latency
                --warmup 0
                --repeats 1
        )
        set_tests_properties(glyphastore_benchmark_durable_group_smoke PROPERTIES
            PASS_REGULAR_EXPRESSION "latency_samples=64"
        )
        add_test(
            NAME glyphastore_benchmark_maintenance_smoke
            COMMAND glyphastore_maintenance_benchmark
                --operations 128
                --threads 2
                --keys 16
                --value-bytes 1024
                --reclaim-value-bytes 1024
                --put-percent 10
                --maintenance-interval-ms 1
                --cooldown-ms 0
                --warmup 0
                --repeats 1
                --mode disabled
        )
        set_tests_properties(glyphastore_benchmark_maintenance_smoke PROPERTIES
            PASS_REGULAR_EXPRESSION "disabled,1,128"
        )
        add_test(
            NAME glyphastore_benchmark_server_tcp
            COMMAND glyphastore_server_benchmarks
                --ops 128
                --workers 2
                --clients 2
                --pipeline 8
                --warmup 0
                --repeats 1
        )
        add_test(
            NAME glyphastore_benchmark_server_tcp_durable_group
            COMMAND glyphastore_server_benchmarks
                --ops 32
                --workers 1
                --clients 4
                --pipeline 8
                --storage-mode durable-group
                --group-max-records 4
                --group-max-wait-ms 20
                --warmup 0
                --repeats 1
        )
        set_tests_properties(glyphastore_benchmark_server_tcp_durable_group PROPERTIES
            PASS_REGULAR_EXPRESSION "maximum_durable_batch_records=4"
        )
    endif()
endif()

if(GLYPHASTORE_BUILD_FUZZERS)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "Fuzz targets require Clang/libFuzzer")
    endif()
    include(CheckCXXSourceCompiles)
    set(CMAKE_REQUIRED_FLAGS "-fsanitize=fuzzer")
    check_cxx_source_compiles(
        "#include <cstddef>\n#include <cstdint>\nextern \"C\" int LLVMFuzzerTestOneInput(const std::uint8_t*, std::size_t) { return 0; }"
        GLYPHASTORE_HAS_LIBFUZZER_RUNTIME
    )
    unset(CMAKE_REQUIRED_FLAGS)
    if(NOT GLYPHASTORE_HAS_LIBFUZZER_RUNTIME)
        message(FATAL_ERROR
            "The selected Clang does not provide a linkable libFuzzer runtime. "
            "On macOS install a full LLVM toolchain and configure with CC/CXX pointing to it."
        )
    endif()
    foreach(target IN ITEMS record_decoder segment_scanner index_rebuild protocol_decoder)
        add_executable(fuzz_${target} fuzz/fuzz_${target}.cpp)
        if(target STREQUAL "protocol_decoder")
            target_link_libraries(fuzz_${target} PRIVATE GlyphaStore::server glyphastore_project_options)
        else()
            target_link_libraries(fuzz_${target} PRIVATE GlyphaStore::core glyphastore_project_options)
        endif()
        target_compile_options(fuzz_${target} PRIVATE -fsanitize=fuzzer,address,undefined)
        target_link_options(fuzz_${target} PRIVATE -fsanitize=fuzzer,address,undefined)
        glyphastore_enable_ipo(fuzz_${target})
    endforeach()
endif()

if(BUILD_TESTING)
    set(glyphastore_check_dependencies
        glyphastore_tests
        glyphastore_crash_persistence
        glyphastore_crash_daemon
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

glyphastore_add_xcode_project_files()

install(TARGETS glyphastore_core glyphastore_wire glyphastore_tls_layer glyphastore_client
    EXPORT GlyphaStoreTargets
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Runtime
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Runtime NAMELINK_COMPONENT Development
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(TARGETS glyphastore_demo glyphastored glyphastore_inspect_segment glyphastore_verify_store
        glyphastore_backup_store glyphastore_migrate_store glyphastore_repair_store glyphastore_rebuild_index
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    COMPONENT Runtime
)
install(FILES
    include/glyphastore/client/client.hpp
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/client"
    COMPONENT Development
)
install(FILES
    include/glyphastore/core/error.hpp
    include/glyphastore/core/types.hpp
    include/glyphastore/core/worker_routing.hpp
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/core"
    COMPONENT Development
)
install(FILES
    include/glyphastore/persistence/compaction_intent.hpp
    include/glyphastore/persistence/filesystem.hpp
    include/glyphastore/persistence/filesystem_hooks.hpp
    include/glyphastore/persistence/manifest.hpp
    include/glyphastore/persistence/namespace_audit.hpp
    include/glyphastore/persistence/resource_limits.hpp
    include/glyphastore/persistence/store_backup.hpp
    include/glyphastore/persistence/store_verify.hpp
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/persistence"
    COMPONENT Development
)
install(FILES
    include/glyphastore/segment/segment_header.hpp
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/segment"
    COMPONENT Development
)
install(FILES
    include/glyphastore/server/protocol.hpp
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/server"
    COMPONENT Development
)
install(FILES
    include/glyphastore/store/config.hpp
    include/glyphastore/store/maintenance_types.hpp
    include/glyphastore/store/store.hpp
    include/glyphastore/store/value.hpp
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/store"
    COMPONENT Development
)

install(
    EXPORT GlyphaStoreTargets
    FILE GlyphaStoreTargets.cmake
    NAMESPACE GlyphaStore::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/GlyphaStore"
    COMPONENT Development
)

configure_package_config_file(
    cmake/GlyphaStoreConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/GlyphaStoreConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/GlyphaStore"
)
if(PROJECT_VERSION_MAJOR EQUAL 0)
    set(GLYPHASTORE_VERSION_COMPATIBILITY SameMinorVersion)
else()
    set(GLYPHASTORE_VERSION_COMPATIBILITY SameMajorVersion)
endif()
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/GlyphaStoreConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY "${GLYPHASTORE_VERSION_COMPATIBILITY}"
)
install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/GlyphaStoreConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/GlyphaStoreConfigVersion.cmake"
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindGlyphaStoreTls.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/GlyphaStore"
    COMPONENT Development
)
install(
    FILES LICENSE NOTICE VERSION
    DESTINATION "${CMAKE_INSTALL_DATADIR}/GlyphaStore"
    COMPONENT Runtime
)
