# Benchmarks and hosted-CI smoke benches.
if(GLYPHASTORE_BUILD_BENCHMARKS)
    find_program(GIT_EXECUTABLE NAMES git)
    set(GLYPHASTORE_GIT_SHA "unknown")
    if(GIT_EXECUTABLE)
        execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GLYPHASTORE_GIT_SHA
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        execute_process(
            COMMAND ${GIT_EXECUTABLE} diff-index --quiet HEAD --
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            RESULT_VARIABLE GLYPHASTORE_GIT_DIRTY
            ERROR_QUIET
        )
        if(GLYPHASTORE_GIT_DIRTY)
            string(APPEND GLYPHASTORE_GIT_SHA "-dirty")
        endif()
    endif()
    add_executable(glyphastore_benchmarks
        benchmark_main.cpp
        harness.cpp
        suite.cpp
    )
    target_link_libraries(glyphastore_benchmarks PRIVATE GlyphaStore::core glyphastore_project_options)
    target_include_directories(glyphastore_benchmarks PRIVATE "${CMAKE_SOURCE_DIR}/benchmarks")
    target_compile_definitions(glyphastore_benchmarks PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}")
    glyphastore_set_warnings(glyphastore_benchmarks)
    glyphastore_enable_hardening(glyphastore_benchmarks)
    glyphastore_enable_ipo(glyphastore_benchmarks)
    set_target_properties(glyphastore_benchmarks PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_benchmarks RELEASE ARGUMENTS
        --filter store-put-get --ops 200000 --warmup 1 --repeats 7
    )
    add_executable(glyphastore_paired_benchmark
        paired_shard_benchmark.cpp
        ${CMAKE_SOURCE_DIR}/src/experimental/paired_shard.cpp
    )
    target_link_libraries(
        glyphastore_paired_benchmark
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    target_include_directories(glyphastore_paired_benchmark
        PRIVATE "${CMAKE_SOURCE_DIR}/src"
    )
    target_compile_definitions(glyphastore_paired_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_paired_benchmark)
    glyphastore_enable_hardening(glyphastore_paired_benchmark)
    glyphastore_enable_ipo(glyphastore_paired_benchmark)
    set_target_properties(glyphastore_paired_benchmark PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_paired_benchmark RELEASE ARGUMENTS
        --ops 500000 --keys 4096 --value-bytes 64 --warmup 1 --repeats 7
    )
    add_executable(glyphastore_generation_shell_benchmark
        generation_shell_benchmark.cpp
        ${CMAKE_SOURCE_DIR}/src/experimental/pair_read_generation_shell_access.cpp
    )
    target_link_libraries(
        glyphastore_generation_shell_benchmark
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    target_include_directories(glyphastore_generation_shell_benchmark
        PRIVATE "${CMAKE_SOURCE_DIR}/src"
    )
    target_compile_definitions(glyphastore_generation_shell_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_generation_shell_benchmark)
    glyphastore_enable_hardening(glyphastore_generation_shell_benchmark)
    glyphastore_enable_ipo(glyphastore_generation_shell_benchmark)
    set_target_properties(glyphastore_generation_shell_benchmark PROPERTIES FOLDER "Benchmarks")
    add_executable(glyphastore_memory_census_benchmark
        memory_census_benchmark.cpp
        harness.cpp
    )
    target_link_libraries(
        glyphastore_memory_census_benchmark
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    target_include_directories(glyphastore_memory_census_benchmark
        PRIVATE "${CMAKE_SOURCE_DIR}/benchmarks" "${CMAKE_SOURCE_DIR}/src"
    )
    target_compile_definitions(glyphastore_memory_census_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_memory_census_benchmark)
    glyphastore_enable_hardening(glyphastore_memory_census_benchmark)
    glyphastore_enable_ipo(glyphastore_memory_census_benchmark)
    set_target_properties(glyphastore_memory_census_benchmark PROPERTIES FOLDER "Benchmarks")
    add_executable(glyphastore_generation_publication_benchmark
        generation_publication_benchmark.cpp
        ${CMAKE_SOURCE_DIR}/src/experimental/pair_read_generation_shell_access.cpp
    )
    target_link_libraries(
        glyphastore_generation_publication_benchmark
        PRIVATE GlyphaStore::server glyphastore_project_options
    )
    target_include_directories(glyphastore_generation_publication_benchmark
        PRIVATE "${CMAKE_SOURCE_DIR}/src"
    )
    target_compile_definitions(glyphastore_generation_publication_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_generation_publication_benchmark)
    glyphastore_enable_hardening(glyphastore_generation_publication_benchmark)
    glyphastore_enable_ipo(glyphastore_generation_publication_benchmark)
    set_target_properties(glyphastore_generation_publication_benchmark PROPERTIES FOLDER "Benchmarks")
    add_executable(glyphastore_paired_reactor_benchmark
        paired_reactor_benchmark.cpp
        ${CMAKE_SOURCE_DIR}/src/experimental/paired_reactor.cpp
        ${CMAKE_SOURCE_DIR}/src/experimental/paired_shard.cpp
    )
    target_link_libraries(
        glyphastore_paired_reactor_benchmark
        PRIVATE GlyphaStore::server GlyphaStore::client glyphastore_project_options
    )
    target_include_directories(glyphastore_paired_reactor_benchmark
        PRIVATE "${CMAKE_SOURCE_DIR}/src"
    )
    target_compile_definitions(glyphastore_paired_reactor_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_paired_reactor_benchmark)
    glyphastore_enable_hardening(glyphastore_paired_reactor_benchmark)
    glyphastore_enable_ipo(glyphastore_paired_reactor_benchmark)
    set_target_properties(glyphastore_paired_reactor_benchmark PROPERTIES FOLDER "Benchmarks")
    add_executable(glyphastore_compaction_benchmark
        compaction_benchmark.cpp
    )
    target_link_libraries(
        glyphastore_compaction_benchmark
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    target_compile_definitions(
        glyphastore_compaction_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_compaction_benchmark)
    glyphastore_enable_hardening(glyphastore_compaction_benchmark)
    glyphastore_enable_ipo(glyphastore_compaction_benchmark)
    set_target_properties(glyphastore_compaction_benchmark PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_compaction_benchmark RELEASE ARGUMENTS
        --repeats 3
    )
    add_executable(glyphastore_maintenance_benchmark
        maintenance_benchmark.cpp
    )
    target_link_libraries(
        glyphastore_maintenance_benchmark
        PRIVATE GlyphaStore::core glyphastore_project_options
    )
    target_compile_definitions(
        glyphastore_maintenance_benchmark
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}"
    )
    glyphastore_set_warnings(glyphastore_maintenance_benchmark)
    glyphastore_enable_hardening(glyphastore_maintenance_benchmark)
    glyphastore_enable_ipo(glyphastore_maintenance_benchmark)
    set_target_properties(glyphastore_maintenance_benchmark PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_maintenance_benchmark RELEASE ARGUMENTS
        --repeats 3
    )
    add_executable(glyphastore_server_benchmarks
        server_benchmark.cpp
        harness.cpp
    )
    target_link_libraries(glyphastore_server_benchmarks
        PRIVATE GlyphaStore::server GlyphaStore::client glyphastore_project_options)
    target_include_directories(glyphastore_server_benchmarks
        PRIVATE "${CMAKE_SOURCE_DIR}/benchmarks")
    target_compile_definitions(glyphastore_server_benchmarks
        PRIVATE GLYPHASTORE_GIT_SHA="${GLYPHASTORE_GIT_SHA}")
    glyphastore_set_warnings(glyphastore_server_benchmarks)
    glyphastore_enable_hardening(glyphastore_server_benchmarks)
    glyphastore_enable_ipo(glyphastore_server_benchmarks)
    set_target_properties(glyphastore_server_benchmarks PROPERTIES FOLDER "Benchmarks")
    glyphastore_xcode_scheme(glyphastore_server_benchmarks RELEASE ARGUMENTS
        --ops 100000 --workers 2 --clients 2 --pipeline 32 --warmup 1 --repeats 7
    )
    add_executable(glyphastore_client_benchmark client_benchmark.cpp)
    target_link_libraries(glyphastore_client_benchmark
        PRIVATE GlyphaStore::client glyphastore_project_options)
    target_compile_definitions(glyphastore_client_benchmark
        PRIVATE GLYPHASTORE_SDK_VERSION="${PROJECT_VERSION}")
    glyphastore_set_warnings(glyphastore_client_benchmark)
    glyphastore_enable_hardening(glyphastore_client_benchmark)
    glyphastore_enable_ipo(glyphastore_client_benchmark)
    set_target_properties(glyphastore_client_benchmark PROPERTIES FOLDER "Benchmarks")
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
            NAME glyphastore_benchmark_parallel_put_latency_smoke
            COMMAND glyphastore_benchmarks
                --filter store-parallel-put
                --ops 256
                --workers 1
                --threads 1
                --distribution single-worker
                --value-size 64
                --latency
                --warmup 0
                --repeats 1
        )
        set_tests_properties(glyphastore_benchmark_parallel_put_latency_smoke PROPERTIES
            PASS_REGULAR_EXPRESSION "latency_samples=256"
        )
        add_test(
            NAME glyphastore_benchmark_durable_smoke
            COMMAND glyphastore_benchmarks
                --filter store-durable-all
                --ops 128
                --workers 1
                --warmup 0
                --repeats 1
        )
        add_test(
            NAME glyphastore_benchmark_memory_census_smoke
            COMMAND glyphastore_memory_census_benchmark
                --entries 60000
                --key-bytes 16
                --value-bytes 64
                --workers 1
        )
        set_tests_properties(glyphastore_benchmark_memory_census_smoke PROPERTIES
            PASS_REGULAR_EXPRESSION
                "read_generation_base_record_mapped_storage_bytes=(0|3670016).*read_generation_spare_mapping_bytes=(0|4194304).*compact_lookup_saved_bytes=.*allocator_stats_available="
        )
        add_test(
            NAME glyphastore_benchmark_durable_parallel_smoke
            COMMAND glyphastore_benchmarks
                --filter store-durable-parallel-all
                --ops 128
                --workers 4
                --threads 4
                --distribution worker-affine
                --warmup 0
                --repeats 1
        )
        add_test(
            NAME glyphastore_benchmark_durable_group_smoke
            COMMAND glyphastore_benchmarks
                --filter store-durable-group-parallel-put
                --ops 64
                --workers 1
                --threads 4
                --distribution single-worker
                --latency
                --warmup 0
                --repeats 1
        )
        set_tests_properties(glyphastore_benchmark_durable_group_smoke PROPERTIES
            PASS_REGULAR_EXPRESSION "latency_samples=64"
        )
        add_test(
            NAME glyphastore_benchmark_maintenance_smoke
            COMMAND glyphastore_maintenance_benchmark
                --operations 128
                --threads 2
                --keys 16
                --value-bytes 1024
                --reclaim-value-bytes 1024
                --put-percent 10
                --maintenance-interval-ms 1
                --cooldown-ms 0
                --warmup 0
                --repeats 1
                --mode disabled
        )
        set_tests_properties(glyphastore_benchmark_maintenance_smoke PROPERTIES
            PASS_REGULAR_EXPRESSION "disabled,1,128"
        )
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
        add_test(
            NAME glyphastore_benchmark_server_tcp_large_get
            COMMAND glyphastore_server_benchmarks
                --ops 8
                --workers 1
                --clients 1
                --pipeline 8
                --value-size 262144
                --workload get-only
                --warmup 0
                --repeats 1
        )
        add_test(
            NAME glyphastore_benchmark_server_tcp_durable_group
            COMMAND glyphastore_server_benchmarks
                --ops 32
                --workers 1
                --clients 4
                --pipeline 8
                --storage-mode durable-group
                --group-max-records 4
                --group-max-wait-ms 20
                --warmup 0
                --repeats 1
        )
        set_tests_properties(glyphastore_benchmark_server_tcp_durable_group PROPERTIES
            # Under sanitizers / qemu, adaptive batching may close before hitting
            # group-max-records=4; require that group commit ran with a sane batch.
            PASS_REGULAR_EXPRESSION "maximum_durable_batch_records=[1-4]"
        )
    endif()
endif()
