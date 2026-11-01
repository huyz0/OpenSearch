// SPDX-License-Identifier: Apache-2.0
// Copyright OpenSearch Contributors.
// Exploratory packed-array primitives, not OpenSearch engine or JVM measurements.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

struct Descriptor {
    std::uint64_t logical_key;
    std::uint64_t route_epoch;
    std::uint64_t checkpoint;
    std::uint32_t mapping_reference;
    std::uint32_t active;
};
static_assert(sizeof(Descriptor) == 32);
static volatile std::uint64_t consumed;

static std::uint64_t random_word(std::uint64_t& state) {
    state += 0x9e3779b97f4a7c15ULL;
    auto value = state;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

template<typename Work> static std::uint64_t elapsed(Work work) {
    const auto start = std::chrono::steady_clock::now();
    consumed = work();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();
}

static std::uint64_t median(std::vector<std::uint64_t> values) {
    std::sort(values.begin(), values.end());
    return values.at(values.size() / 2);
}

int main(int argc, char** argv) {
    try {
        if (argc != 3) throw std::invalid_argument("usage: descriptor_capacity_probe <1..10000000 descriptors> <10|100|1000 active basis points>");
        const auto count = std::stoull(argv[1]);
        const auto active_bps = std::stoull(argv[2]);
        if (count < 1 || count > 10000000 || (active_bps != 10 && active_bps != 100 && active_bps != 1000)) {
            throw std::invalid_argument("descriptor or activity bound exceeded");
        }
        constexpr int trials = 3, passes = 20;
        constexpr std::size_t lookup_count = 2000000;
        std::vector<Descriptor> descriptors(count);
        std::vector<std::uint32_t> active;
        active.reserve(count * active_bps / 10000);
        for (std::size_t i = 0; i < count; ++i) {
            const bool selected = ((i + 1) * active_bps / 10000 != i * active_bps / 10000);
            descriptors[i] = {i * 0x9e3779b97f4a7c15ULL, 1, 100 + i, static_cast<std::uint32_t>(i % 128), selected};
            if (selected) active.push_back(static_cast<std::uint32_t>(i));
        }
        std::vector<Descriptor> compact_active;
        compact_active.reserve(active.size());
        for (auto index : active) compact_active.push_back(descriptors[index]);
        std::vector<std::uint32_t> lookups(lookup_count);
        std::uint64_t state = 0x51ce11a017ULL;
        for (auto& index : lookups) index = static_cast<std::uint32_t>(random_word(state) % count);
        const auto full_scan = [&]() {
            std::uint64_t result = 0;
            for (int pass = 0; pass < passes; ++pass) {
                std::atomic_signal_fence(std::memory_order_seq_cst);
                for (const auto& descriptor : descriptors) {
                    if (descriptor.active) result += descriptor.logical_key ^ descriptor.checkpoint;
                }
            }
            return result;
        };
        const auto active_scan = [&]() {
            std::uint64_t result = 0;
            for (int pass = 0; pass < passes; ++pass) {
                std::atomic_signal_fence(std::memory_order_seq_cst);
                for (const auto index : active) {
                    const auto& descriptor = descriptors[index];
                    result += descriptor.logical_key ^ descriptor.checkpoint;
                }
            }
            return result;
        };
        const auto compact_scan = [&]() {
            std::uint64_t result = 0;
            for (int pass = 0; pass < passes; ++pass) {
                std::atomic_signal_fence(std::memory_order_seq_cst);
                for (const auto& descriptor : compact_active) result += descriptor.logical_key ^ descriptor.checkpoint;
            }
            return result;
        };
        const auto lookup = [&]() {
            std::uint64_t result = 0;
            for (const auto index : lookups) result += descriptors[index].logical_key ^ descriptors[index].checkpoint;
            return result;
        };
        if (full_scan() != active_scan() || full_scan() != compact_scan()) throw std::runtime_error("scan checksums disagree");
        elapsed(full_scan); elapsed(active_scan); elapsed(compact_scan); elapsed(lookup);
        std::vector<std::uint64_t> full_times, active_times, compact_times, lookup_times;
        for (int trial = 0; trial < trials; ++trial) {
            full_times.push_back(elapsed(full_scan));
            active_times.push_back(elapsed(active_scan));
            compact_times.push_back(elapsed(compact_scan));
            lookup_times.push_back(elapsed(lookup));
        }
        const auto json_array = [](const auto& values) {
            std::string text = "[";
            for (std::size_t i = 0; i < values.size(); ++i) text += (i ? "," : "") + std::to_string(values[i]);
            return text + "]";
        };
        std::cout << "{\"descriptors\":" << count << ",\"active_basis_points\":" << active_bps
            << ",\"active_descriptors\":" << active.size() << ",\"descriptor_bytes\":" << count * sizeof(Descriptor)
            << ",\"active_index_bytes\":" << active.size() * sizeof(std::uint32_t)
            << ",\"compact_active_bytes\":" << compact_active.size() * sizeof(Descriptor)
            << ",\"lookup_index_bytes\":" << lookups.size() * sizeof(std::uint32_t)
            << ",\"passes_per_trial\":" << passes << ",\"trials\":" << trials
            << ",\"median_full_scan_ns\":" << median(full_times) / passes
            << ",\"median_active_scan_ns\":" << median(active_times) / passes
            << ",\"median_compact_active_scan_ns\":" << median(compact_times) / passes
            << ",\"full_trial_total_ns\":" << json_array(full_times)
            << ",\"active_trial_total_ns\":" << json_array(active_times)
            << ",\"compact_trial_total_ns\":" << json_array(compact_times)
            << ",\"lookup_trial_total_ns\":" << json_array(lookup_times)
            << ",\"median_lookup_ns_per_operation\":" << static_cast<double>(median(lookup_times)) / lookup_count
            << ",\"checksum\":" << consumed << "}\n";
    } catch (const std::exception& failure) {
        std::cerr << failure.what() << '\n';
        return 1;
    }
}
