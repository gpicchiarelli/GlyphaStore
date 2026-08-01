include_guard(GLOBAL)

include(CMakeParseArguments)

function(glyphastore_xcode_scheme target)
    set(options RELEASE)
    set(multi_value_arguments ARGUMENTS)
    cmake_parse_arguments(GLYPHASTORE_XCODE "${options}" "" "${multi_value_arguments}" ${ARGN})

    if(NOT TARGET "${target}")
        message(FATAL_ERROR "Cannot configure an Xcode scheme for unknown target: ${target}")
    endif()
    if(NOT CMAKE_GENERATOR STREQUAL "Xcode")
        return()
    endif()

    set_target_properties("${target}" PROPERTIES
        XCODE_GENERATE_SCHEME TRUE
        XCODE_SCHEME_WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    )
    if(GLYPHASTORE_XCODE_RELEASE)
        set_target_properties("${target}" PROPERTIES
            XCODE_SCHEME_LAUNCH_CONFIGURATION Release
        )
    endif()
    if(GLYPHASTORE_XCODE_ARGUMENTS)
        set_target_properties("${target}" PROPERTIES
            XCODE_SCHEME_ARGUMENTS "${GLYPHASTORE_XCODE_ARGUMENTS}"
        )
    endif()
endfunction()

function(glyphastore_add_xcode_project_files)
    if(NOT CMAKE_GENERATOR STREQUAL "Xcode")
        return()
    endif()

    file(GLOB_RECURSE glyphastore_xcode_headers CONFIGURE_DEPENDS
        "${PROJECT_SOURCE_DIR}/include/*.hpp"
        "${PROJECT_SOURCE_DIR}/src/*.hpp"
        "${PROJECT_SOURCE_DIR}/tests/*.hpp"
        "${PROJECT_SOURCE_DIR}/benchmarks/*.hpp"
    )
    file(GLOB_RECURSE glyphastore_xcode_docs CONFIGURE_DEPENDS
        "${PROJECT_SOURCE_DIR}/docs/*.md"
    )
    file(GLOB glyphastore_xcode_scripts CONFIGURE_DEPENDS
        "${PROJECT_SOURCE_DIR}/scripts/*.sh"
        "${PROJECT_SOURCE_DIR}/scripts/*.py"
    )
    set(glyphastore_xcode_root_files
        "${PROJECT_SOURCE_DIR}/README.md"
        "${PROJECT_SOURCE_DIR}/CHANGELOG.md"
        "${PROJECT_SOURCE_DIR}/CONTRIBUTING.md"
        "${PROJECT_SOURCE_DIR}/SECURITY.md"
        "${PROJECT_SOURCE_DIR}/CMakeLists.txt"
        "${PROJECT_SOURCE_DIR}/CMakePresets.json"
        "${PROJECT_SOURCE_DIR}/Makefile"
        "${PROJECT_SOURCE_DIR}/VERSION"
    )
    set(glyphastore_xcode_files
        ${glyphastore_xcode_headers}
        ${glyphastore_xcode_docs}
        ${glyphastore_xcode_scripts}
        ${glyphastore_xcode_root_files}
    )
    source_group(TREE "${PROJECT_SOURCE_DIR}" PREFIX "Project" FILES ${glyphastore_xcode_files})
    add_custom_target(glyphastore_project_files SOURCES ${glyphastore_xcode_files})
    set_target_properties(glyphastore_project_files PROPERTIES
        FOLDER "Project"
        XCODE_GENERATE_SCHEME FALSE
    )
endfunction()
