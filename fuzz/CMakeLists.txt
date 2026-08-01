# libFuzzer targets.
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
        add_executable(fuzz_${target} fuzz_${target}.cpp)
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

