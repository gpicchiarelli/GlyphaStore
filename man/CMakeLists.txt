# Portable mdoc(7) manuals for every installed runtime binary.
# Sources are .in templates; configure_file injects PROJECT_VERSION and MAN_DATE.
# Installed uncompressed under GNUInstallDirs MANDIR so Linux, macOS, FreeBSD,
# and OpenBSD all resolve pages via man(1)/mandoc without a gzip dependency at
# install time. Distro packaging may compress after install.

set(GLYPHASTORE_MAN_DATE "" CACHE STRING
    "Date stamp for .Dd in man pages (mdoc); empty uses configure-time local date")
if(GLYPHASTORE_MAN_DATE STREQUAL "")
    string(TIMESTAMP GLYPHASTORE_MAN_DATE "%B %d, %Y")
endif()
set(MAN_DATE "${GLYPHASTORE_MAN_DATE}")

option(GLYPHASTORE_COMPRESS_MANPAGES
    "Install gzip-compressed man pages (optional; packagers often compress later)"
    OFF)

set(GLYPHASTORE_MAN_PAGE_INS
    man1/glyphastore_demo.1.in
    man1/glyphastore_inspect_segment.1.in
    man1/glyphastore_verify_store.1.in
    man1/glyphastore_backup_store.1.in
    man1/glyphastore_migrate_store.1.in
    man1/glyphastore_repair_store.1.in
    man1/glyphastore_rebuild_index.1.in
    man7/glyphastore.7.in
    man8/glyphastored.8.in
)

set(GLYPHASTORE_MAN_GENERATED "")
foreach(man_in IN LISTS GLYPHASTORE_MAN_PAGE_INS)
    get_filename_component(man_rel "${man_in}" NAME_WLE) # e.g. glyphastored.8
    get_filename_component(man_section_dir "${man_in}" DIRECTORY) # man1 / man7 / man8
    set(man_out "${CMAKE_CURRENT_BINARY_DIR}/${man_section_dir}/${man_rel}")
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/${man_in}"
        "${man_out}"
        @ONLY
    )
    list(APPEND GLYPHASTORE_MAN_GENERATED "${man_out}")
endforeach()

add_custom_target(glyphastore_manpages DEPENDS ${GLYPHASTORE_MAN_GENERATED})
set_target_properties(glyphastore_manpages PROPERTIES FOLDER "Documentation")

if(GLYPHASTORE_COMPRESS_MANPAGES)
    find_program(GLYPHASTORE_GZIP_EXECUTABLE gzip REQUIRED)
    set(GLYPHASTORE_MAN_INSTALL_FILES "")
    foreach(man_out IN LISTS GLYPHASTORE_MAN_GENERATED)
        set(man_gz "${man_out}.gz")
        add_custom_command(
            OUTPUT "${man_gz}"
            COMMAND /bin/sh -c "\"${GLYPHASTORE_GZIP_EXECUTABLE}\" -n -c \"${man_out}\" > \"${man_gz}\""
            DEPENDS "${man_out}"
            VERBATIM
        )
        list(APPEND GLYPHASTORE_MAN_INSTALL_FILES "${man_gz}")
    endforeach()
    add_custom_target(glyphastore_manpages_gz ALL DEPENDS ${GLYPHASTORE_MAN_INSTALL_FILES})
    add_dependencies(glyphastore_manpages_gz glyphastore_manpages)
    set_target_properties(glyphastore_manpages_gz PROPERTIES FOLDER "Documentation")
    set(_glyphastore_man_install_list "${GLYPHASTORE_MAN_INSTALL_FILES}")
else()
    set(_glyphastore_man_install_list "${GLYPHASTORE_MAN_GENERATED}")
endif()

# Map each generated page to its section destination.
foreach(man_file IN LISTS _glyphastore_man_install_list)
    get_filename_component(man_name "${man_file}" NAME) # glyphastored.8 or .8.gz
    if(man_name MATCHES "\\.1(\\.gz)?$")
        set(_sec man1)
    elseif(man_name MATCHES "\\.7(\\.gz)?$")
        set(_sec man7)
    elseif(man_name MATCHES "\\.8(\\.gz)?$")
        set(_sec man8)
    else()
        message(FATAL_ERROR "Unrecognized man page section for ${man_file}")
    endif()
    # Manuals are part of the deployable Runtime component.
    install(FILES "${man_file}"
        DESTINATION "${CMAKE_INSTALL_MANDIR}/${_sec}"
        COMPONENT Runtime
    )
endforeach()
