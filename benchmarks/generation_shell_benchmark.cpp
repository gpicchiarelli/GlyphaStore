#include "experimental/pair_read_generation_shell.hpp"
#include "glyphastore/store/paired/read_generation.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using Generation = glyphastore::store::paired::PairReadGeneration;
using Mutation = glyphastore::store::paired::ReadMutation;
using ShellAccess = glyphastore::experimental::PairReadGenerationShellAccess;
using ShellBank = glyphastore::experimental::PairReadGenerationShellBank<2>;
using PublicationPool = glyphastore::experimental::GenerationSlotPool<Generation, 2>;
using InlinePool = glyphastore::experimental::PairReadGenerationInlineSlotPool<2>;
using DirectRing = glyphastore::experimental::PairReadGenerationDirectRing<2>;
using DirectPool = glyphastore::experimental::PairReadGenerationDirectSlotPool<2>;

struct Options final {
    std::size_t operations{20'000};
    std::size_t warmup{1};
    std::size_t repeats{7};
};

[[nodiscard]] auto parse_size(const char* text) -> std::size_t {
    if (text == nullptr) {
        throw std::invalid_argument{"missing numeric argument"};
    }
    const auto value = std::stoull(text);
    if (value == 0 || value > Generation::kMaximumIncrementalDeltaEntries) {
        throw std::invalid_argument{"operation count is outside the incremental delta bound"};
    }
    return static_cast<std::size_t>(value);
}

[[nodiscard]] auto parse_options(const int argc, char** argv) -> Options {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{argv[index]};
        if (argument == "--help" || argument == "-h") {
            std::cout << "usage: glyphastore_generation_shell_benchmark "
                         "[--ops N] [--warmup N] [--repeats N]\n";
            std::exit(0);
        }
        if (index + 1 >= argc) {
            throw std::invalid_argument{"missing argument value"};
        }
        if (argument == "--ops") {
            options.operations = parse_size(argv[++index]);
        } else if (argument == "--warmup") {
            options.warmup = static_cast<std::size_t>(std::stoull(argv[++index]));
        } else if (argument == "--repeats") {
            options.repeats = parse_size(argv[++index]);
        } else {
            throw std::invalid_argument{"unknown argument: " + std::string{argument}};
        }
    }
    return options;
}

[[nodiscard]] auto bytes(const std::string_view text) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

struct Material final {
    glyphastore::WorkerRoutingState routing{};
    std::shared_ptr<glyphastore::Segment> segment;
    std::string key{"generation-shell-key"};
    std::uint64_t hash{};
    std::vector<glyphastore::RecordRef> records;
};

[[nodiscard]] auto make_material(const std::size_t operations) -> Material {
    Material material;
    material.segment = std::make_shared<glyphastore::Segment>(glyphastore::SegmentId{701});
    material.hash = glyphastore::hash_key_routing(material.key, material.routing);
    material.records.reserve(operations);
    const std::array value{std::byte{0x47}, std::byte{0x53}};
    for (std::size_t index = 0; index < operations; ++index) {
        auto record = material.segment->append({.sequence = glyphastore::SequenceNumber{index + 1U},
                                                .opcode = glyphastore::Opcode::put,
                                                .key_hash = material.hash,
                                                .key = bytes(material.key),
                                                .value = value});
        if (!record) {
            throw std::runtime_error{"cannot prepare benchmark Segment"};
        }
        material.records.push_back(*record);
    }
    return material;
}

struct Measurement final {
    double seconds{};
    std::uint64_t checksum{};
    std::uint64_t shell_allocations{};
    std::uint64_t shell_reuses{};
};

[[nodiscard]] auto run_once(const Material& material, const bool fixed_shell) -> Measurement {
    auto initial = Generation::empty(material.routing);
    if (!initial) {
        throw std::runtime_error{"cannot create initial read generation"};
    }
    auto current = *initial;
    auto bank_result = ShellBank::create();
    if (!bank_result) {
        throw std::runtime_error{"cannot create shell bank"};
    }
    auto& bank = **bank_result;
    std::uint64_t checksum{};
    const auto started = Clock::now();
    for (std::size_t index = 0; index < material.records.size(); ++index) {
        const Mutation mutation{.key = {material.key, material.hash},
                                .record = material.records[index],
                                .segment = material.segment,
                                .opcode = glyphastore::Opcode::put};
        auto next = fixed_shell ? ShellAccess::publish_incremental(current, std::span{&mutation, 1},
                                                                   bank.at(index % 2U))
                                : Generation::publish_incremental(current, std::span{&mutation, 1});
        if (!next) {
            throw std::runtime_error{"generation publication failed"};
        }
        current = *next;
        checksum += current->epoch();
    }
    const auto elapsed = Clock::now() - started;
    Measurement measurement{.seconds = std::chrono::duration<double>(elapsed).count(), .checksum = checksum};
    if (fixed_shell) {
        for (std::size_t index = 0; index < 2U; ++index) {
            const auto storage = bank.at(index);
            measurement.shell_allocations += storage->allocation_count();
            measurement.shell_reuses += storage->reuse_count();
        }
    }
    return measurement;
}

[[nodiscard]] auto run_pooled_once(const Material& material, const bool inline_storage) -> Measurement {
    auto initial = Generation::empty(material.routing);
    if (!initial) {
        throw std::runtime_error{"cannot create initial pooled read generation"};
    }
    auto owning_pool_result = PublicationPool::create(*initial);
    auto inline_pool_result = InlinePool::create(*initial);
    auto bank_result = ShellBank::create();
    if (!owning_pool_result || !inline_pool_result || !bank_result) {
        throw std::runtime_error{"cannot create generation publication pool"};
    }
    auto& owning_pool = **owning_pool_result;
    auto& inline_pool = **inline_pool_result;
    auto& bank = **bank_result;
    if ((inline_storage ? inline_pool.adopt() : owning_pool.adopt()) == nullptr) {
        throw std::runtime_error{"cannot adopt initial pooled generation"};
    }

    std::uint64_t checksum{};
    const auto started = Clock::now();
    for (std::size_t index = 0; index < material.records.size(); ++index) {
        const Mutation mutation{.key = {material.key, material.hash},
                                .record = material.records[index],
                                .segment = material.segment,
                                .opcode = glyphastore::Opcode::put};
        if (inline_storage) {
            auto reservation = inline_pool.try_reserve();
            if (!reservation) {
                throw std::runtime_error{"inline pool reservation failed"};
            }
            reservation->mark_store_linearized();
            if (inline_pool.publish_incremental(*reservation, std::span{&mutation, 1}) !=
                glyphastore::experimental::GenerationSlotPublishStatus::published) {
                throw std::runtime_error{"inline pool publication failed"};
            }
            const auto* adopted = inline_pool.adopt();
            if (adopted == nullptr) {
                throw std::runtime_error{"inline pool adoption failed"};
            }
            checksum += adopted->epoch();
            inline_pool.reclaim();
            continue;
        }

        auto reservation = owning_pool.try_reserve();
        if (!reservation) {
            throw std::runtime_error{"owning pool reservation failed"};
        }
        auto next =
            ShellAccess::publish_incremental(owning_pool.writer_generation_owner(), std::span{&mutation, 1},
                                             bank.at(reservation->slot_index()));
        if (!next) {
            throw std::runtime_error{"owning pool generation build failed"};
        }
        reservation->mark_store_linearized();
        if (owning_pool.commit(*reservation, std::move(*next)) !=
            glyphastore::experimental::GenerationSlotPublishStatus::published) {
            throw std::runtime_error{"owning pool publication failed"};
        }
        const auto* adopted = owning_pool.adopt();
        if (adopted == nullptr) {
            throw std::runtime_error{"owning pool adoption failed"};
        }
        checksum += adopted->epoch();
        owning_pool.reclaim();
    }
    const auto elapsed = Clock::now() - started;
    Measurement measurement{.seconds = std::chrono::duration<double>(elapsed).count(), .checksum = checksum};
    for (std::size_t index = 0; index < 2U; ++index) {
        if (inline_storage) {
            measurement.shell_allocations += inline_pool.shell_allocation_count(index);
            measurement.shell_reuses += inline_pool.shell_reuse_count(index);
        } else {
            const auto storage = bank.at(index);
            measurement.shell_allocations += storage->allocation_count();
            measurement.shell_reuses += storage->reuse_count();
        }
    }
    return measurement;
}

[[nodiscard]] auto run_direct_once(const Material& material) -> Measurement {
    auto initial = Generation::empty(material.routing);
    if (!initial) {
        throw std::runtime_error{"cannot create initial direct generation"};
    }
    DirectRing ring{*initial};
    std::uint64_t checksum{};
    const auto started = Clock::now();
    for (std::size_t index = 0; index < material.records.size(); ++index) {
        const Mutation mutation{.key = {material.key, material.hash},
                                .record = material.records[index],
                                .segment = material.segment,
                                .opcode = glyphastore::Opcode::put};
        auto next = ring.publish(std::span{&mutation, 1});
        if (!next) {
            throw std::runtime_error{"direct generation publication failed"};
        }
        checksum += (*next)->epoch();
    }
    const auto elapsed = Clock::now() - started;
    Measurement measurement{.seconds = std::chrono::duration<double>(elapsed).count(), .checksum = checksum};
    for (std::size_t index = 0; index < 2U; ++index) {
        measurement.shell_allocations += ring.allocation_count(index);
        measurement.shell_reuses += ring.reuse_count(index);
    }
    return measurement;
}

[[nodiscard]] auto run_direct_pool_once(const Material& material) -> Measurement {
    auto pool_result = DirectPool::create(material.routing);
    if (!pool_result) {
        throw std::runtime_error{"cannot create direct generation slot pool"};
    }
    auto& pool = **pool_result;
    if (pool.adopt() == nullptr) {
        throw std::runtime_error{"cannot adopt initial direct pooled generation"};
    }
    std::uint64_t checksum{};
    const auto started = Clock::now();
    for (std::size_t index = 0; index < material.records.size(); ++index) {
        const Mutation mutation{.key = {material.key, material.hash},
                                .record = material.records[index],
                                .segment = material.segment,
                                .opcode = glyphastore::Opcode::put};
        auto reservation = pool.try_reserve();
        if (!reservation) {
            throw std::runtime_error{"direct pool reservation failed"};
        }
        reservation->mark_store_linearized();
        if (pool.publish_incremental(*reservation, std::span{&mutation, 1}) !=
            glyphastore::experimental::GenerationSlotPublishStatus::published) {
            throw std::runtime_error{"direct pool publication failed"};
        }
        const auto* adopted = pool.adopt();
        if (adopted == nullptr) {
            throw std::runtime_error{"direct pool adoption failed"};
        }
        checksum += adopted->epoch();
        pool.reclaim();
    }
    const auto elapsed = Clock::now() - started;
    Measurement measurement{.seconds = std::chrono::duration<double>(elapsed).count(), .checksum = checksum};
    for (std::size_t index = 0; index < 2U; ++index) {
        measurement.shell_allocations += pool.shell_allocation_count(index);
        measurement.shell_reuses += pool.shell_reuse_count(index);
    }
    pool.stop_admission();
    if (!pool.mark_reader_quiescent() || !pool.try_finish_shutdown()) {
        throw std::runtime_error{"direct pool shutdown failed"};
    }
    return measurement;
}

void print(const std::string_view implementation, const std::size_t repeat, const std::size_t operations,
           const Measurement& measurement) {
    const auto operations_per_second = static_cast<double>(operations) / measurement.seconds;
    std::cout << implementation << '\t' << repeat << '\t' << std::fixed << std::setprecision(3)
              << measurement.seconds << '\t' << std::setprecision(0) << operations_per_second << '\t'
              << std::setprecision(2)
              << (measurement.seconds * 1'000'000'000.0 / static_cast<double>(operations)) << '\t'
              << measurement.shell_allocations << '\t' << measurement.shell_reuses << '\t'
              << measurement.checksum << '\n';
}

} // namespace

int main(const int argc, char** argv) try {
    const auto options = parse_options(argc, argv);
    const auto material = make_material(options.operations);
    for (std::size_t index = 0; index < options.warmup; ++index) {
        static_cast<void>(run_once(material, false));
        static_cast<void>(run_once(material, true));
        static_cast<void>(run_pooled_once(material, false));
        static_cast<void>(run_pooled_once(material, true));
        static_cast<void>(run_direct_once(material));
        static_cast<void>(run_direct_pool_once(material));
    }
    std::cout << "implementation\trepeat\tseconds\tops_per_second\tns_per_op\t"
                 "shell_allocations\tshell_reuses\tchecksum\n";
    for (std::size_t repeat = 0; repeat < options.repeats; ++repeat) {
        if ((repeat & 1U) == 0U) {
            print("make_shared", repeat, options.operations, run_once(material, false));
            print("fixed_shell", repeat, options.operations, run_once(material, true));
            print("owning_pool", repeat, options.operations, run_pooled_once(material, false));
            print("inline_pool", repeat, options.operations, run_pooled_once(material, true));
            print("direct_ring", repeat, options.operations, run_direct_once(material));
            print("direct_slot_pool", repeat, options.operations, run_direct_pool_once(material));
        } else {
            print("direct_slot_pool", repeat, options.operations, run_direct_pool_once(material));
            print("direct_ring", repeat, options.operations, run_direct_once(material));
            print("inline_pool", repeat, options.operations, run_pooled_once(material, true));
            print("owning_pool", repeat, options.operations, run_pooled_once(material, false));
            print("fixed_shell", repeat, options.operations, run_once(material, true));
            print("make_shared", repeat, options.operations, run_once(material, false));
        }
    }
    return 0;
} catch (const std::exception& error) {
    std::cerr << "generation shell benchmark error: " << error.what() << '\n';
    return 1;
}
