cmake_minimum_required(VERSION 3.25)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" GLYPHASTORE_VERSION)
string(STRIP "${GLYPHASTORE_VERSION}" GLYPHASTORE_VERSION)
if(NOT GLYPHASTORE_VERSION MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
    message(FATAL_ERROR "VERSION must contain exactly one semantic version such as 1.2.3")
endif()

project(GlyphaStore VERSION "${GLYPHASTORE_VERSION}" LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
include(ProjectOptions)
include(CompilerWarnings)
include(Sanitizers)
include(Hardening)
include(XcodeSupport)

glyphastore_project_options()
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set(CMAKE_PREDEFINED_TARGETS_FOLDER "CMake")

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
    src/index/index.cpp
    src/index/key_arena.cpp
    src/index/swiss_table.cpp
    src/persistence/bootstrap.cpp
    src/persistence/compaction.cpp
    src/persistence/compaction_builder.cpp
    src/persistence/compaction_intent.cpp
    src/persistence/filesystem.cpp
    src/persistence/manifest.cpp
    src/persistence/namespace_audit.cpp
    src/persistence/durable_flush_coordinator.cpp
    src/persistence/recovery.cpp
    src/persistence/resource_limits.cpp
    src/persistence/runtime_catalog.cpp
    src/persistence/segment_file.cpp
    src/segment/crc32c.cpp
    src/segment/global_manager.cpp
    src/segment/record.cpp
    src/segment/segment.cpp
    src/segment/segment_header.cpp
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
target_link_libraries(glyphastore_core PRIVATE $<BUILD_INTERFACE:glyphastore_project_options>)
glyphastore_set_warnings(glyphastore_core)
glyphastore_enable_hardening(glyphastore_core)
glyphastore_enable_ipo(glyphastore_core)
set_target_properties(glyphastore_core PROPERTIES FOLDER "Libraries")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(GLYPHASTORE_POLLER_SOURCE src/server/poller_epoll.cpp)
elseif(APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
    set(GLYPHASTORE_POLLER_SOURCE src/server/poller_kqueue.cpp)
else()
    message(FATAL_ERROR "glyphastored requires epoll on Linux or kqueue on macOS/FreeBSD/OpenBSD")
endif()

add_library(glyphastore_server_core
    src/server/connection_handoff.cpp
    src/server/protocol.cpp
    src/server/reactor.cpp
    src/server/server.cpp
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
    PUBLIC GlyphaStore::core
    PRIVATE $<BUILD_INTERFACE:glyphastore_project_options>
)
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
glyphastore_enable_ipo(glyphastore_demo)
set_target_properties(glyphastore_demo PROPERTIES FOLDER "Applications")
glyphastore_xcode_scheme(glyphastore_demo)

include(CTest)
if(BUILD_TESTING)
    add_executable(glyphastore_tests
        tests/test_main.cpp
        tests/unit/compaction_intent_tests.cpp
        tests/unit/compaction_tests.cpp
        tests/unit/index_tests.cpp
        tests/unit/manifest_tests.cpp
        tests/unit/namespace_audit_tests.cpp
        tests/unit/persistence_filesystem_tests.cpp
        tests/unit/segment_file_tests.cpp
        tests/unit/cli_arguments_tests.cpp
        tests/unit/bounded_mpsc_queue_tests.cpp
        tests/unit/connection_handoff_tests.cpp
        tests/unit/durable_flush_coordinator_tests.cpp
        tests/unit/record_tests.cpp
        tests/unit/resource_limits_tests.cpp
        tests/unit/server_protocol_tests.cpp
        tests/unit/segment_tests.cpp
        tests/unit/segment_header_tests.cpp
        tests/unit/segment_catalog_tests.cpp
        tests/unit/trusted_read_tests.cpp
        tests/unit/vacuum_tests.cpp
        tests/unit/worker_tests.cpp
        tests/unit/format_compatibility_tests.cpp
        tests/integration/compaction_builder_tests.cpp
        tests/integration/compaction_recovery_tests.cpp
        tests/integration/concurrency_tests.cpp
        tests/integration/optimization_regression_tests.cpp
        tests/integration/persistence_recovery_tests.cpp
        tests/integration/rebuild_vacuum_tests.cpp
        tests/integration/server_reactor_tests.cpp
        tests/integration/store_tests.cpp
        tests/property/history_property_tests.cpp
    )
    target_link_libraries(glyphastore_tests
        PRIVATE GlyphaStore::server glyphastore_cli_support glyphastore_project_options
    )
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
    set_tests_properties(
        glyphastore_crash_sync
        glyphastore_crash_periodic
        glyphastore_crash_group
        PROPERTIES TIMEOUT 600 LABELS "crash"
    )
endif()

add_executable(glyphastore_inspect_segment tools/inspect_segment.cpp)
target_link_libraries(glyphastore_inspect_segment
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_inspect_segment PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_inspect_segment)
glyphastore_enable_ipo(glyphastore_inspect_segment)
set_target_properties(glyphastore_inspect_segment PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_inspect_segment ARGUMENTS --help)

add_executable(glyphastore_rebuild_index tools/rebuild_index.cpp)
target_link_libraries(glyphastore_rebuild_index
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_rebuild_index PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_rebuild_index)
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
        PASS_REGULAR_EXPRESSION "Inspect a GlyphaStore segment file"
    )
    add_test(NAME glyphastore_cli_rebuild_help COMMAND glyphastore_rebuild_index --help)
    set_tests_properties(glyphastore_cli_rebuild_help PROPERTIES
        PASS_REGULAR_EXPRESSION "Rebuild a GlyphaStore index"
    )
    add_test(NAME glyphastore_cli_rejects_invalid_workers COMMAND glyphastored --workers 257)
    set_tests_properties(glyphastore_cli_rejects_invalid_workers PROPERTIES WILL_FAIL TRUE)
    set_tests_properties(
        glyphastore_cli_daemon_help
        glyphastore_cli_daemon_version
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
    glyphastore_enable_ipo(glyphastore_benchmarks)
    set_target_properties(glyphastore_benchmarks PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_benchmarks RELEASE ARGUMENTS
        --filter store-put-get --ops 200000 --warmup 1 --repeats 7
    )
    add_executable(glyphastore_server_benchmarks
        benchmarks/server_benchmark.cpp
        benchmarks/harness.cpp
    )
    target_link_libraries(glyphastore_server_benchmarks
        PRIVATE GlyphaStore::server glyphastore_project_options)
    target_include_directories(glyphastore_server_benchmarks
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks")
    target_compile_definitions(glyphastore_server_benchmarks
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}")
    glyphastore_set_warnings(glyphastore_server_benchmarks)
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
            NAME glyphastore_benchmark_server_tcp
            COMMAND glyphastore_server_benchmarks
                --ops 128
                --workers 2
                --clients 2
                --pipeline 8
                --warmup 0
                --repeats 1
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
        glyphastored
        glyphastore_inspect_segment
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

install(TARGETS glyphastore_core
    EXPORT GlyphaStoreTargets
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Runtime
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Runtime NAMELINK_COMPONENT Development
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(TARGETS glyphastore_demo glyphastored glyphastore_inspect_segment glyphastore_rebuild_index
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    COMPONENT Runtime
)
install(FILES
    include/glyphastore/core/error.hpp
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/core"
    COMPONENT Development
)
install(FILES
    include/glyphastore/store/config.hpp
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
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/GlyphaStore"
    COMPONENT Development
)
install(
    FILES LICENSE NOTICE VERSION
    DESTINATION "${CMAKE_INSTALL_DATADIR}/GlyphaStore"
    COMPONENT Runtime
)
