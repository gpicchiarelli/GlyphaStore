# Offline/operator tools and PGO helper.
add_executable(glyphastore_inspect_segment inspect_segment.cpp)
target_link_libraries(glyphastore_inspect_segment
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_inspect_segment PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_inspect_segment)
glyphastore_enable_hardening(glyphastore_inspect_segment)
glyphastore_enable_ipo(glyphastore_inspect_segment)
set_target_properties(glyphastore_inspect_segment PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_inspect_segment ARGUMENTS --help)

add_executable(glyphastore_verify_store verify_store.cpp)
target_link_libraries(glyphastore_verify_store
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_verify_store PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_verify_store)
glyphastore_enable_hardening(glyphastore_verify_store)
glyphastore_enable_ipo(glyphastore_verify_store)
set_target_properties(glyphastore_verify_store PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_verify_store ARGUMENTS --help)

add_executable(glyphastore_backup_store backup_store.cpp)
target_link_libraries(glyphastore_backup_store
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_backup_store PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_backup_store)
glyphastore_enable_hardening(glyphastore_backup_store)
glyphastore_enable_ipo(glyphastore_backup_store)
set_target_properties(glyphastore_backup_store PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_backup_store ARGUMENTS --help)

add_executable(glyphastore_migrate_store migrate_store.cpp)
target_link_libraries(glyphastore_migrate_store
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_migrate_store PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_migrate_store)
glyphastore_enable_hardening(glyphastore_migrate_store)
glyphastore_enable_ipo(glyphastore_migrate_store)
set_target_properties(glyphastore_migrate_store PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_migrate_store ARGUMENTS --help)

add_executable(glyphastore_repair_store repair_store.cpp)
target_link_libraries(glyphastore_repair_store
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_repair_store PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_repair_store)
glyphastore_enable_hardening(glyphastore_repair_store)
glyphastore_enable_ipo(glyphastore_repair_store)
set_target_properties(glyphastore_repair_store PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_repair_store ARGUMENTS --help)

add_executable(glyphastore_interop_client interop_client.cpp)
target_link_libraries(glyphastore_interop_client
    PRIVATE GlyphaStore::client glyphastore_project_options
)
glyphastore_set_warnings(glyphastore_interop_client)
glyphastore_enable_hardening(glyphastore_interop_client)
glyphastore_enable_ipo(glyphastore_interop_client)
set_target_properties(glyphastore_interop_client PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_interop_client ARGUMENTS --help)

add_executable(glyphastore_rebuild_index rebuild_index.cpp)
target_link_libraries(glyphastore_rebuild_index
    PRIVATE GlyphaStore::core glyphastore_cli_support glyphastore_project_options
)
target_compile_definitions(glyphastore_rebuild_index PRIVATE GLYPHASTORE_VERSION="${PROJECT_VERSION}")
glyphastore_set_warnings(glyphastore_rebuild_index)
glyphastore_enable_hardening(glyphastore_rebuild_index)
glyphastore_enable_ipo(glyphastore_rebuild_index)
set_target_properties(glyphastore_rebuild_index PROPERTIES FOLDER "Tools")
glyphastore_xcode_scheme(glyphastore_rebuild_index ARGUMENTS --help)

if(GLYPHASTORE_BUILD_BENCHMARKS OR BUILD_TESTING)
    add_executable(glyphastore_pgo_durable pgo_durable.cpp)
    target_link_libraries(glyphastore_pgo_durable PRIVATE GlyphaStore::core glyphastore_project_options)
    glyphastore_set_warnings(glyphastore_pgo_durable)
    glyphastore_enable_hardening(glyphastore_pgo_durable)
    # Must match core's IPO: Clang LTO archives are LLVM bitcode; linking
    # without INTERPROCEDURAL_OPTIMIZATION falls through to ld.bfd and fails
    # with "file format not recognized".
    glyphastore_enable_ipo(glyphastore_pgo_durable)
    set_target_properties(glyphastore_pgo_durable PROPERTIES FOLDER "Tools")
endif()

