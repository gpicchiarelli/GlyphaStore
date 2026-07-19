include_guard(GLOBAL)

function(glyphastore_set_warnings target)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE
            -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion
            -Wshadow -Wnon-virtual-dtor -Wold-style-cast -Wcast-align
            -Wnull-dereference -Wdouble-promotion -Wformat=2
            -Wimplicit-fallthrough
        )
        if(GLYPHASTORE_WARNINGS_AS_ERRORS OR DEFINED ENV{CI})
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    endif()
endfunction()
