include_guard(GLOBAL)

function(glyphastore_set_warnings target)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE
            -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion
            -Wshadow -Wnon-virtual-dtor -Wold-style-cast -Wcast-align
            -Wnull-dereference -Wdouble-promotion -Wformat=2
            -Wimplicit-fallthrough -Wmissing-field-initializers
        )
        # Apple Clang does not include partial designated aggregates in
        # -Wmissing-field-initializers, unlike GCC and upstream Clang. Keep the
        # developer build aligned with the Linux and BSD CI gates.
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            target_compile_options(${target} PRIVATE -Wmissing-designated-field-initializers)
        endif()
        if(GLYPHASTORE_WARNINGS_AS_ERRORS OR DEFINED ENV{CI})
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    endif()
endfunction()
