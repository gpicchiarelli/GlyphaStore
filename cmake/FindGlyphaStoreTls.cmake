# Discovers LibreSSL (first-class on OpenBSD) or OpenSSL 3.x for the secure profile.
# Sets:
#   GLYPHASTORE_TLS_FOUND
#   GLYPHASTORE_TLS_BACKEND   ("LibreSSL" or "OpenSSL")
#   GlyphaStore::tls          INTERFACE imported target (when found)

include_guard(GLOBAL)

set(GLYPHASTORE_TLS_FOUND FALSE)
set(GLYPHASTORE_TLS_BACKEND "")

if(CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
    find_path(GLYPHASTORE_TLS_INCLUDE_DIR
        NAMES openssl/ssl.h
        PATHS /usr/include
        NO_DEFAULT_PATH
    )
    find_library(GLYPHASTORE_TLS_SSL_LIBRARY
        NAMES ssl libssl
        PATHS /usr/lib
        NO_DEFAULT_PATH
    )
    find_library(GLYPHASTORE_TLS_CRYPTO_LIBRARY
        NAMES crypto libcrypto
        PATHS /usr/lib
        NO_DEFAULT_PATH
    )
    if(GLYPHASTORE_TLS_INCLUDE_DIR AND GLYPHASTORE_TLS_SSL_LIBRARY AND GLYPHASTORE_TLS_CRYPTO_LIBRARY)
        set(GLYPHASTORE_TLS_FOUND TRUE)
        set(GLYPHASTORE_TLS_BACKEND "LibreSSL")
        if(NOT TARGET GlyphaStore::tls)
            add_library(GlyphaStore::tls INTERFACE IMPORTED)
            target_include_directories(GlyphaStore::tls INTERFACE "${GLYPHASTORE_TLS_INCLUDE_DIR}")
            target_link_libraries(GlyphaStore::tls INTERFACE
                "${GLYPHASTORE_TLS_SSL_LIBRARY}"
                "${GLYPHASTORE_TLS_CRYPTO_LIBRARY}"
            )
        endif()
    endif()
else()
    set(_glyphastore_tls_prefixes ${CMAKE_PREFIX_PATH})
    if(DEFINED OPENSSL_ROOT_DIR)
        list(PREPEND _glyphastore_tls_prefixes "${OPENSSL_ROOT_DIR}")
    endif()
    if(DEFINED ENV{OPENSSL_ROOT_DIR})
        list(PREPEND _glyphastore_tls_prefixes "$ENV{OPENSSL_ROOT_DIR}")
    endif()
    foreach(_root IN ITEMS
            /opt/local
            /opt/homebrew/opt/openssl@3
            /usr/local/opt/openssl@3
            /usr/local/opt/openssl)
        if(EXISTS "${_root}")
            list(APPEND _glyphastore_tls_prefixes "${_root}")
        endif()
    endforeach()
    list(REMOVE_DUPLICATES _glyphastore_tls_prefixes)

    set(_glyphastore_tls_saved_prefix "${CMAKE_PREFIX_PATH}")
    set(CMAKE_PREFIX_PATH "${_glyphastore_tls_prefixes}")
    find_package(OpenSSL 3 QUIET)
    set(CMAKE_PREFIX_PATH "${_glyphastore_tls_saved_prefix}")
    unset(_glyphastore_tls_prefixes)
    unset(_glyphastore_tls_saved_prefix)

    if(OpenSSL_FOUND)
        set(GLYPHASTORE_TLS_FOUND TRUE)
        set(GLYPHASTORE_TLS_BACKEND "OpenSSL")
        if(NOT TARGET GlyphaStore::tls)
            add_library(GlyphaStore::tls INTERFACE IMPORTED)
            target_link_libraries(GlyphaStore::tls INTERFACE OpenSSL::SSL OpenSSL::Crypto)
        endif()
    endif()
endif()

mark_as_advanced(
    GLYPHASTORE_TLS_INCLUDE_DIR
    GLYPHASTORE_TLS_SSL_LIBRARY
    GLYPHASTORE_TLS_CRYPTO_LIBRARY
)
