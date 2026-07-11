include_guard(GLOBAL)

option(GLYPHASTORE_BUILD_BENCHMARKS "Build microbenchmarks" ON)
option(GLYPHASTORE_BUILD_FUZZERS "Build libFuzzer targets" OFF)
option(GLYPHASTORE_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(GLYPHASTORE_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(GLYPHASTORE_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(GLYPHASTORE_ENABLE_HARDENING "Enable supported hardening flags" ON)
option(GLYPHASTORE_ENABLE_CLANG_TIDY "Run clang-tidy while compiling" OFF)

function(glyphastore_project_options)
    add_library(glyphastore_project_options INTERFACE)
    target_compile_features(glyphastore_project_options INTERFACE cxx_std_23)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON PARENT_SCOPE)
    glyphastore_enable_sanitizers(glyphastore_project_options)
    if(GLYPHASTORE_ENABLE_CLANG_TIDY)
        find_program(GLYPHASTORE_CLANG_TIDY_EXECUTABLE NAMES clang-tidy REQUIRED)
        set(CMAKE_CXX_CLANG_TIDY
            "${GLYPHASTORE_CLANG_TIDY_EXECUTABLE};--config-file=${PROJECT_SOURCE_DIR}/.clang-tidy"
            PARENT_SCOPE
        )
    endif()
endfunction()
