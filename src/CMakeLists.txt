# GlyphaStore libraries and primary applications (Phase C split).
add_library(glyphastore_cli_support
    ${CMAKE_SOURCE_DIR}/src/cli/arguments.cpp
)
target_compile_features(glyphastore_cli_support PUBLIC cxx_std_23)
target_include_directories(glyphastore_cli_support
    PUBLIC "${CMAKE_SOURCE_DIR}/src"
    PRIVATE "${CMAKE_SOURCE_DIR}/include"
)
target_link_libraries(glyphastore_cli_support PRIVATE glyphastore_project_options)
glyphastore_set_warnings(glyphastore_cli_support)
glyphastore_enable_hardening(glyphastore_cli_support)
set_target_properties(glyphastore_cli_support PROPERTIES FOLDER "Libraries/Internal")

add_library(glyphastore_core
    ${CMAKE_SOURCE_DIR}/src/core/fault_injection.cpp
    ${CMAKE_SOURCE_DIR}/src/core/hot_path_phases.cpp
    ${CMAKE_SOURCE_DIR}/src/core/worker_routing.cpp
    ${CMAKE_SOURCE_DIR}/src/index/index.cpp
    ${CMAKE_SOURCE_DIR}/src/index/index_hash_seed.cpp
    ${CMAKE_SOURCE_DIR}/src/index/key_arena.cpp
    ${CMAKE_SOURCE_DIR}/src/index/swiss_table.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/bootstrap.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/compaction/layout.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/compaction/manifest_transition.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/compaction/planner.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/compaction/resource_estimator.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/compaction_builder.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/compaction_intent.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/filesystem.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/namespace_audit.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/durable_flush_coordinator.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/recovery/recovery.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/recovery/recovery_budget.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/recovery/recovery_catalog.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/recovery/recovery_index_builder.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/recovery/recovery_scanner.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/resource_limits.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/runtime_catalog.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/runtime_catalog_detail.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/runtime_catalog_ops.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/runtime_catalog_maintenance.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/segment_file.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/store_verify.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/store_backup.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/store_migrate.cpp
    ${CMAKE_SOURCE_DIR}/src/persistence/store_repair.cpp
    ${CMAKE_SOURCE_DIR}/src/segment/crc32c.cpp
    ${CMAKE_SOURCE_DIR}/src/segment/global_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/segment/record.cpp
    ${CMAKE_SOURCE_DIR}/src/segment/segment.cpp
    ${CMAKE_SOURCE_DIR}/src/segment/segment_header.cpp
    ${CMAKE_SOURCE_DIR}/src/store/maintenance.cpp
    ${CMAKE_SOURCE_DIR}/src/store/paired/mutation_slot_pool.cpp
    ${CMAKE_SOURCE_DIR}/src/store/paired/read_generation.cpp
    ${CMAKE_SOURCE_DIR}/src/store/paired/shard_pair_runtime.cpp
    ${CMAKE_SOURCE_DIR}/src/store/store.cpp
    ${CMAKE_SOURCE_DIR}/src/store/store_access.cpp
    ${CMAKE_SOURCE_DIR}/src/store/prepared_cold_read.cpp
    ${CMAKE_SOURCE_DIR}/src/vacuum/vacuum.cpp
    ${CMAKE_SOURCE_DIR}/src/worker/pool.cpp
    ${CMAKE_SOURCE_DIR}/src/worker/topology.cpp
    ${CMAKE_SOURCE_DIR}/src/worker/worker.cpp
)
add_library(GlyphaStore::core ALIAS glyphastore_core)
set_target_properties(glyphastore_core PROPERTIES EXPORT_NAME core)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set_source_files_properties(${CMAKE_SOURCE_DIR}/src/persistence/segment_file.cpp PROPERTIES COMPILE_DEFINITIONS _GNU_SOURCE)
endif()

target_compile_features(glyphastore_core PUBLIC cxx_std_23)
target_include_directories(glyphastore_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        "${CMAKE_SOURCE_DIR}/src"
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
if(GLYPHASTORE_FAULT_INJECTION)
    target_compile_definitions(glyphastore_core PUBLIC GLYPHASTORE_FAULT_INJECTION=1)
    message(STATUS "GlyphaStore fault injection: ENABLED (debug-only adverse scheduling hooks)")
else()
    message(STATUS "GlyphaStore fault injection: disabled (production default)")
endif()
if(GLYPHASTORE_HOT_PATH_PHASES)
    target_compile_definitions(glyphastore_core PUBLIC GLYPHASTORE_HOT_PATH_PHASES=1)
    message(STATUS "GlyphaStore hot-path phases: ENABLED (lab attribution counters)")
else()
    message(STATUS "GlyphaStore hot-path phases: disabled (default)")
endif()
glyphastore_set_warnings(glyphastore_core)
glyphastore_enable_hardening(glyphastore_core)
glyphastore_enable_ipo(glyphastore_core)
set_target_properties(glyphastore_core PROPERTIES FOLDER "Libraries")

add_library(glyphastore_wire
    ${CMAKE_SOURCE_DIR}/src/server/protocol.cpp
)
add_library(GlyphaStore::wire ALIAS glyphastore_wire)
set_target_properties(glyphastore_wire PROPERTIES EXPORT_NAME wire)
target_compile_features(glyphastore_wire PUBLIC cxx_std_23)
target_include_directories(glyphastore_wire
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(glyphastore_wire PRIVATE $<BUILD_INTERFACE:glyphastore_project_options>)
glyphastore_set_warnings(glyphastore_wire)
glyphastore_enable_hardening(glyphastore_wire)
glyphastore_enable_ipo(glyphastore_wire)
set_target_properties(glyphastore_wire PROPERTIES FOLDER "Libraries")

add_library(glyphastore_tls_layer
    ${CMAKE_SOURCE_DIR}/src/server/tls.cpp
)
add_library(GlyphaStore::tls_layer ALIAS glyphastore_tls_layer)
set_target_properties(glyphastore_tls_layer PROPERTIES EXPORT_NAME tls_layer)
target_compile_features(glyphastore_tls_layer PUBLIC cxx_std_23)
target_include_directories(glyphastore_tls_layer
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
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
    ${CMAKE_SOURCE_DIR}/src/client/client.cpp
)
add_library(GlyphaStore::client ALIAS glyphastore_client)
set_target_properties(glyphastore_client PROPERTIES EXPORT_NAME client)
target_compile_features(glyphastore_client PUBLIC cxx_std_23)
target_include_directories(glyphastore_client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
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
    set(GLYPHASTORE_POLLER_SOURCE ${CMAKE_SOURCE_DIR}/src/server/poller_epoll.cpp)
elseif(APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
    set(GLYPHASTORE_POLLER_SOURCE ${CMAKE_SOURCE_DIR}/src/server/poller_kqueue.cpp)
else()
    message(FATAL_ERROR "glyphastored requires epoll on Linux or kqueue on macOS/FreeBSD/OpenBSD")
endif()

add_library(glyphastore_server_core
    ${CMAKE_SOURCE_DIR}/src/server/abuse_limits.cpp
    ${CMAKE_SOURCE_DIR}/src/server/authz.cpp
    ${CMAKE_SOURCE_DIR}/src/server/connection_handoff.cpp
    ${CMAKE_SOURCE_DIR}/src/server/crash_test_hooks.cpp
    ${CMAKE_SOURCE_DIR}/src/server/daemon_config.cpp
    ${CMAKE_SOURCE_DIR}/src/server/daemon_log.cpp
    ${CMAKE_SOURCE_DIR}/src/server/disk_read_executor.cpp
    ${CMAKE_SOURCE_DIR}/src/server/pair_writer.cpp
    ${CMAKE_SOURCE_DIR}/src/server/openbsd_sandbox.cpp
    ${CMAKE_SOURCE_DIR}/src/server/peercred.cpp
    ${CMAKE_SOURCE_DIR}/src/server/reactor.cpp
    ${CMAKE_SOURCE_DIR}/src/server/reactor_dispatch.cpp
    ${CMAKE_SOURCE_DIR}/src/server/reactor_factory.cpp
    ${CMAKE_SOURCE_DIR}/src/server/security_audit.cpp
    ${CMAKE_SOURCE_DIR}/src/server/server.cpp
    ${CMAKE_SOURCE_DIR}/src/server/server_builder.cpp
    ${CMAKE_SOURCE_DIR}/src/server/server_health.cpp
    ${CMAKE_SOURCE_DIR}/src/server/server_lifecycle.cpp
    ${CMAKE_SOURCE_DIR}/src/server/server_stats.cpp
    ${CMAKE_SOURCE_DIR}/src/server/socket.cpp
    ${CMAKE_SOURCE_DIR}/src/server/thread_affinity.cpp
    ${CMAKE_SOURCE_DIR}/src/server/wakeup.cpp
    ${GLYPHASTORE_POLLER_SOURCE}
)
add_library(GlyphaStore::server ALIAS glyphastore_server_core)
set_target_properties(glyphastore_server_core PROPERTIES EXPORT_NAME server)
target_compile_features(glyphastore_server_core PUBLIC cxx_std_23)
target_include_directories(glyphastore_server_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        "${CMAKE_SOURCE_DIR}/src"
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

add_executable(glyphastored ${CMAKE_SOURCE_DIR}/src/server/daemon_main.cpp)
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

add_executable(glyphastore_demo ${CMAKE_SOURCE_DIR}/src/main.cpp)
target_link_libraries(glyphastore_demo PRIVATE GlyphaStore::core glyphastore_project_options)
glyphastore_set_warnings(glyphastore_demo)
glyphastore_enable_hardening(glyphastore_demo)
glyphastore_enable_ipo(glyphastore_demo)
set_target_properties(glyphastore_demo PROPERTIES FOLDER "Applications")
glyphastore_xcode_scheme(glyphastore_demo)

