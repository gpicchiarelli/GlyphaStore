cmake_minimum_required(VERSION 3.25)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" GLYPHASTORE_VERSION)
string(STRIP "${GLYPHASTORE_VERSION}" GLYPHASTORE_VERSION)
if(NOT GLYPHASTORE_VERSION MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
    message(FATAL_ERROR "VERSION must contain exactly one semantic version such as 1.2.3")
endif()

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/ABI_VERSION" GLYPHASTORE_ABI_VERSION)
string(STRIP "${GLYPHASTORE_ABI_VERSION}" GLYPHASTORE_ABI_VERSION)
if(NOT GLYPHASTORE_ABI_VERSION MATCHES "^([0-9]+)\\.([0-9]+)$")
    message(FATAL_ERROR "ABI_VERSION must contain exactly a major.minor version such as 1.0")
endif()
set(GLYPHASTORE_ABI_VERSION_MAJOR "${CMAKE_MATCH_1}")
set(GLYPHASTORE_ABI_VERSION_MINOR "${CMAKE_MATCH_2}")

project(GlyphaStore VERSION "${GLYPHASTORE_VERSION}" LANGUAGES C CXX)

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
include(StandardLibraryHardening)
include(XcodeSupport)
include(FindGlyphaStoreTls)

glyphastore_project_options()
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set(CMAKE_PREDEFINED_TARGETS_FOLDER "CMake")
# Keep executables at the build-tree root so scripts/CI that expect
# build/<preset>/glyphastored (and friends) keep working after the Phase C split.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

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


add_subdirectory(src)
add_subdirectory(tools)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(GLYPHASTORE_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if(GLYPHASTORE_BUILD_FUZZERS)
    add_subdirectory(fuzz)
endif()

glyphastore_add_xcode_project_files()

add_subdirectory(man)

install(TARGETS glyphastore_core glyphastore_wire glyphastore_tls_layer glyphastore_client
    EXPORT GlyphaStoreTargets
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Runtime
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Runtime NAMELINK_COMPONENT Development
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(TARGETS glyphastore_abi
    EXPORT GlyphaStoreTargets
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT AbiRuntime
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT AbiRuntime NAMELINK_COMPONENT Development
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(TARGETS glyphastore_demo glyphastored glyphastore_inspect_segment glyphastore_verify_store
        glyphastore_backup_store glyphastore_migrate_store glyphastore_repair_store glyphastore_rebuild_index
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    COMPONENT Runtime
)
install(FILES
    include/glyphastore/abi/glyphastore.h
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/abi"
    COMPONENT Development
)
install(FILES
    include/glyphastore/client/client.hpp
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/glyphastore/client"
    COMPONENT Development
)

configure_file(cmake/glyphastore-abi.pc.in "${CMAKE_CURRENT_BINARY_DIR}/glyphastore-abi.pc" @ONLY)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/glyphastore-abi.pc"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig"
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

set(GLYPHASTORE_LTO_ENABLED FALSE)
set(GLYPHASTORE_LTO_LINK_OPTIONS "")
if(GLYPHASTORE_ENABLE_LTO AND GLYPHASTORE_IPO_SUPPORTED)
    set(GLYPHASTORE_LTO_ENABLED TRUE)
    set(GLYPHASTORE_LTO_LINK_OPTIONS "${GLYPHASTORE_IPO_LINK_OPTIONS}")
endif()

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
    FILES LICENSE NOTICE VERSION ABI_VERSION THIRD_PARTY_NOTICES.md
    DESTINATION "${CMAKE_INSTALL_DATADIR}/GlyphaStore"
    COMPONENT Runtime
)
set(GLYPHASTORE_DEFAULT_DATA_DIR "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}/lib/glyphastore")
configure_file(
    config/glyphastored.conf.sample.in
    "${CMAKE_CURRENT_BINARY_DIR}/glyphastored.conf.sample"
    @ONLY
)
install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/glyphastored.conf.sample"
    DESTINATION "${CMAKE_INSTALL_DATADIR}/GlyphaStore/examples"
    COMPONENT Runtime
)
