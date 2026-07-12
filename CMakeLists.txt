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

glyphastore_project_options()

add_library(glyphastore_core
    src/index/index.cpp
    src/index/key_arena.cpp
    src/index/swiss_table.cpp
    src/segment/crc32c.cpp
    src/segment/global_manager.cpp
    src/segment/record.cpp
    src/segment/segment.cpp
    src/store/store.cpp
    src/vacuum/vacuum.cpp
    src/worker/pool.cpp
    src/worker/topology.cpp
    src/worker/worker.cpp
)
add_library(GlyphaStore::core ALIAS glyphastore_core)
set_target_properties(glyphastore_core PROPERTIES EXPORT_NAME core)

target_compile_features(glyphastore_core PUBLIC cxx_std_23)
target_include_directories(glyphastore_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(glyphastore_core PRIVATE $<BUILD_INTERFACE:glyphastore_project_options>)
glyphastore_set_warnings(glyphastore_core)
glyphastore_enable_hardening(glyphastore_core)
glyphastore_enable_ipo(glyphastore_core)

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
)
target_link_libraries(glyphastore_server_core
    PUBLIC GlyphaStore::core
    PRIVATE $<BUILD_INTERFACE:glyphastore_project_options>
)
glyphastore_set_warnings(glyphastore_server_core)
glyphastore_enable_hardening(glyphastore_server_core)
glyphastore_enable_ipo(glyphastore_server_core)

add_executable(glyphastored src/server/daemon_main.cpp)
target_link_libraries(glyphastored PRIVATE GlyphaStore::server glyphastore_project_options)
glyphastore_set_warnings(glyphastored)
glyphastore_enable_hardening(glyphastored)
glyphastore_enable_ipo(glyphastored)
set_target_properties(glyphastored PROPERTIES XCODE_GENERATE_SCHEME TRUE)

add_executable(glyphastore_demo src/main.cpp)
target_link_libraries(glyphastore_demo PRIVATE GlyphaStore::core glyphastore_project_options)
glyphastore_set_warnings(glyphastore_demo)
glyphastore_enable_ipo(glyphastore_demo)
set_target_properties(glyphastore_demo PROPERTIES
    XCODE_GENERATE_SCHEME TRUE
    XCODE_SCHEME_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)

include(CTest)
if(BUILD_TESTING)
    add_executable(glyphastore_tests
        tests/test_main.cpp
        tests/unit/index_tests.cpp
        tests/unit/bounded_mpsc_queue_tests.cpp
        tests/unit/connection_handoff_tests.cpp
        tests/unit/record_tests.cpp
        tests/unit/server_protocol_tests.cpp
        tests/unit/segment_tests.cpp
        tests/unit/segment_catalog_tests.cpp
        tests/unit/trusted_read_tests.cpp
        tests/unit/vacuum_tests.cpp
        tests/unit/worker_tests.cpp
        tests/integration/concurrency_tests.cpp
        tests/integration/optimization_regression_tests.cpp
        tests/integration/rebuild_vacuum_tests.cpp
        tests/integration/server_reactor_tests.cpp
        tests/integration/store_tests.cpp
        tests/property/history_property_tests.cpp
    )
    target_link_libraries(glyphastore_tests PRIVATE GlyphaStore::server glyphastore_project_options)
    target_include_directories(glyphastore_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    glyphastore_set_warnings(glyphastore_tests)
    glyphastore_enable_hardening(glyphastore_tests)
    glyphastore_enable_ipo(glyphastore_tests)
    add_test(NAME glyphastore_tests COMMAND glyphastore_tests)
    set_target_properties(glyphastore_tests PROPERTIES XCODE_GENERATE_SCHEME TRUE)
endif()

add_executable(glyphastore_inspect_segment tools/inspect_segment.cpp)
target_link_libraries(glyphastore_inspect_segment PRIVATE GlyphaStore::core glyphastore_project_options)
glyphastore_set_warnings(glyphastore_inspect_segment)
glyphastore_enable_ipo(glyphastore_inspect_segment)
set_target_properties(glyphastore_inspect_segment PROPERTIES XCODE_GENERATE_SCHEME TRUE)

add_executable(glyphastore_rebuild_index tools/rebuild_index.cpp)
target_link_libraries(glyphastore_rebuild_index PRIVATE GlyphaStore::core glyphastore_project_options)
glyphastore_set_warnings(glyphastore_rebuild_index)
glyphastore_enable_ipo(glyphastore_rebuild_index)
set_target_properties(glyphastore_rebuild_index PROPERTIES XCODE_GENERATE_SCHEME TRUE)

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
    set_target_properties(glyphastore_benchmarks PROPERTIES XCODE_GENERATE_SCHEME TRUE)
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
    set_target_properties(glyphastore_server_benchmarks PROPERTIES XCODE_GENERATE_SCHEME TRUE)
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

install(TARGETS glyphastore_core glyphastore_server_core
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
install(
    DIRECTORY include/glyphastore
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    COMPONENT Development
    FILES_MATCHING PATTERN "*.hpp"
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
