// Linux / AArch64 and x86-64. No libnuma dependency.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <sys/mman.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/mempolicy.h>
#include <sched.h>
#include <sys/syscall.h>
#endif

constexpr std::size_t MiB = 1024 * 1024;
struct alignas(64) Cell { std::uint64_t next; unsigned char padding[56]; };
static_assert(sizeof(Cell) == 64, "One pointer per cache line");

std::uint64_t clock_ns() {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0)
        throw std::runtime_error("clock_gettime failed");
    return std::uint64_t(ts.tv_sec) * 1000000000ULL + ts.tv_nsec;
}

void pause_spin() {
#if defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#elif defined(__x86_64__)
    asm volatile("pause" ::: "memory");
#else
    std::this_thread::yield();
#endif
}

std::vector<int> cpu_list(const std::string& text) {
    std::vector<int> result;
    std::stringstream in(text);
    std::string part;
    while (std::getline(in, part, ',')) {
        if (part.empty()) continue;
        auto dash = part.find('-');
        int first = std::stoi(part), last = first;
        if (dash != std::string::npos) last = std::stoi(part.substr(dash + 1));
        if (first < 0 || last < first || last > 1000000)
            throw std::runtime_error("Invalid CPU list");
        for (int i = first; i <= last; ++i) result.push_back(i);
    }
    return result;
}

std::string read_text(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Cannot read " + path);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

std::set<int> allowed_cpus() {
#ifdef __linux__
    // Allocated affinity masks also support servers with CPU IDs above 1023.
    const int capacity = std::max(1024L, sysconf(_SC_NPROCESSORS_CONF));
    const std::size_t bytes = CPU_ALLOC_SIZE(capacity);
    cpu_set_t* mask = CPU_ALLOC(capacity);
    if (!mask) throw std::bad_alloc();
    CPU_ZERO_S(bytes, mask);
    if (sched_getaffinity(0, bytes, mask) != 0) {
        CPU_FREE(mask);
        throw std::runtime_error("sched_getaffinity failed");
    }
    std::set<int> result;
    for (int i = 0; i < capacity; ++i)
        if (CPU_ISSET_S(i, bytes, mask)) result.insert(i);
    CPU_FREE(mask);
    return result;
#else
    throw std::runtime_error("This benchmark requires Linux; --self-test is portable");
#endif
}

void pin(int cpu) {
#ifdef __linux__
    const int capacity = std::max(cpu + 1, 1024);
    const std::size_t bytes = CPU_ALLOC_SIZE(capacity);
    cpu_set_t* mask = CPU_ALLOC(capacity);
    if (!mask) throw std::bad_alloc();
    CPU_ZERO_S(bytes, mask);
    CPU_SET_S(cpu, bytes, mask);
    const int rc = sched_setaffinity(0, bytes, mask);
    const int error = errno;
    CPU_FREE(mask);
    if (rc != 0) throw std::runtime_error("Cannot pin CPU " + std::to_string(cpu) + ": " + std::strerror(error));
#else
    (void)cpu;
    throw std::runtime_error("CPU binding requires Linux");
#endif
}

struct Domain { int node; int socket; std::vector<int> cpus; };
std::vector<Domain> domains(const std::set<int>& allowed) {
    std::vector<Domain> result;
    for (const auto& entry : std::filesystem::directory_iterator("/sys/devices/system/node")) {
        const auto name = entry.path().filename().string();
        if (name.size() <= 4 || name.substr(0, 4) != "node" ||
            !std::all_of(name.begin() + 4, name.end(), [](char c) { return c >= '0' && c <= '9'; })) continue;
        std::vector<int> cpus;
        for (int cpu : cpu_list(read_text((entry.path() / "cpulist").string())))
            if (allowed.count(cpu)) cpus.push_back(cpu);
        if (cpus.empty()) continue;
        const int socket = std::stoi(read_text("/sys/devices/system/cpu/cpu" +
            std::to_string(cpus.front()) + "/topology/physical_package_id"));
        for (int cpu : cpus) {
            const int other = std::stoi(read_text("/sys/devices/system/cpu/cpu" +
                std::to_string(cpu) + "/topology/physical_package_id"));
            if (socket != other) throw std::runtime_error("NUMA node spans sockets; select a different topology");
        }
        result.push_back({std::stoi(name.substr(4)), socket, std::move(cpus)});
    }
    std::sort(result.begin(), result.end(), [](const Domain& a, const Domain& b) { return a.node < b.node; });
    return result;
}

struct BindingMask {
    std::vector<unsigned long> words;
    unsigned long maxnode;
};

BindingMask binding_mask(int node) {
    if (node < 0) throw std::runtime_error("Negative memory node");
    constexpr std::size_t bits = sizeof(unsigned long) * 8;
    BindingMask mask{std::vector<unsigned long>(std::size_t(node) / bits + 1, 0),
                     static_cast<unsigned long>(node) + 2UL};
    mask.words[std::size_t(node) / bits] = 1UL << (std::size_t(node) % bits);
    // Linux get_nodes() decrements maxnode before reading the bitmap.
    // node + 1 would exclude the requested bit, including an empty mask at node 0.
    return mask;
}

class Region {
public:
    Cell* cells;
    std::size_t bytes;
    Region(std::size_t size, int node) : cells(nullptr), bytes(size) {
        void* raw = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (raw == MAP_FAILED) throw std::runtime_error("mmap: " + std::string(std::strerror(errno)));
        cells = static_cast<Cell*>(raw);
#ifdef __linux__
        // Bind BEFORE first touch. No migration of existing/shared application memory.
        const auto mask = binding_mask(node);
        if (syscall(SYS_mbind, cells, bytes, MPOL_BIND, mask.words.data(),
                    mask.maxnode, 0UL) != 0) {
            const int error = errno;
            std::string message = "mbind(node " + std::to_string(node) +
                ", maxnode " + std::to_string(mask.maxnode) + "): " + std::strerror(error);
            std::ifstream status("/proc/self/status");
            std::string line;
            while (std::getline(status, line))
                if (line.compare(0, 18, "Mems_allowed_list:") == 0)
                    message += "; " + line;
            munmap(cells, bytes);
            throw std::runtime_error(message);
        }
        // Keep page size predictable. Results include translation/TLB costs.
        if (madvise(cells, bytes, MADV_NOHUGEPAGE) != 0) {
            munmap(cells, bytes);
            throw std::runtime_error("MADV_NOHUGEPAGE failed");
        }
#else
        (void)node;
#endif
    }
    ~Region() { munmap(cells, bytes); }
    Region(const Region&) = delete;
    Region& operator=(const Region&) = delete;
    std::size_t count() const { return bytes / sizeof(Cell); }
    // "verified_sample" verifies evenly spaced pages, not every page.
    std::string verify(int node) const {
#ifdef __linux__
        const auto page_size = std::size_t(sysconf(_SC_PAGESIZE));
        const std::size_t pages = (bytes - 1) / page_size + 1;
        const std::size_t samples = std::min<std::size_t>(256, pages);
        std::vector<void*> addresses(samples);
        std::vector<int> status(samples, -999);
        for (std::size_t i = 0; i < samples; ++i)
            addresses[i] = reinterpret_cast<unsigned char*>(cells) + (i * pages / samples) * page_size;
        if (syscall(SYS_move_pages, 0, samples, addresses.data(), nullptr, status.data(), 0) < 0) {
            std::cerr << "Page-location query unavailable: " << std::strerror(errno) << '\n';
            return "unverified";
        }
        for (int actual : status) {
            if (actual < 0) {
                std::cerr << "Page-location query returned " << actual << '\n';
                return "unverified";
            }
            if (actual != node) throw std::runtime_error("Physical pages are not on requested node " + std::to_string(node));
        }
        return "verified_sample";
#else
        (void)node;
        return "unverified";
#endif
    }
};

void make_ring(Cell* cells, std::size_t count) {
    std::vector<std::uint64_t> order(count);
    for (std::size_t i = 0; i < count; ++i) order[i] = i;
    std::mt19937_64 rng(0x9205250);
    std::shuffle(order.begin(), order.end(), rng);
    for (std::size_t i = 0; i < count; ++i)
        cells[order[i]].next = order[(i + 1) % count];
}

// Every address depends on the preceding load. Volatile prevents load removal.
std::uint64_t chase(const Cell* cells, std::uint64_t index, std::uint64_t loads) {
    const volatile Cell* p = cells;
    for (std::uint64_t i = 0; i < loads; ++i) index = p[index].next;
    return index;
}


void timer_fence() {
#if defined(__aarch64__)
    asm volatile("dsb ish\n\tisb" ::: "memory");
#elif defined(__x86_64__)
    asm volatile("lfence" ::: "memory");
#else
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
}

double timed_chase(const Region& region, std::uint64_t& index, std::uint64_t loads) {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    timer_fence();
    const auto begin = clock_ns();
    timer_fence();
    index = chase(region.cells, index, loads);
    timer_fence();
    const auto end = clock_ns();
    std::atomic_signal_fence(std::memory_order_seq_cst);
    return double(end - begin) / double(loads);
}

double median(std::vector<double> samples) {
    std::sort(samples.begin(), samples.end());
    const std::size_t mid = samples.size() / 2;
    return samples.size() % 2 ? samples[mid] : (samples[mid - 1] + samples[mid]) / 2;
}


struct MatrixOptions {
    std::vector<int> nodes;
    std::size_t size_bytes = 512 * MiB;
    int rounds = 7;
    std::uint64_t loads = 2000000;
    std::string output = "memory_latency_results";
    bool list = false, test = false, help = false;
};

std::uint64_t positive_integer(const std::string& s) {
    if (s.empty() || !std::all_of(s.begin(), s.end(), [](char c) { return c >= '0' && c <= '9'; }))
        throw std::runtime_error("Expected a positive integer, got " + s);
    auto n = std::stoull(s);
    if (!n) throw std::runtime_error("Value must be positive");
    return n;
}

MatrixOptions matrix_options(int argc, char** argv) {
    MatrixOptions o;
    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        if (k == "--list") { o.list = true; continue; }
        if (k == "--self-test") { o.test = true; continue; }
        if (k == "--help") { o.help = true; continue; }
        if (i + 1 == argc) throw std::runtime_error("Missing value for " + k);
        std::string v = argv[++i];
        if (k == "--nodes") o.nodes = cpu_list(v);
        else if (k == "--output") o.output = v;
        else if (k == "--size-mib" || k == "--size-kib") {
            auto n = positive_integer(v);
            const std::size_t unit = k == "--size-mib" ? MiB : 1024;
            if (n > std::numeric_limits<std::size_t>::max() / unit)
                throw std::runtime_error("Working set is too large");
            o.size_bytes = std::size_t(n) * unit;
        } else if (k == "--rounds") {
            auto n = positive_integer(v);
            if (n > 100000) throw std::runtime_error("Too many rounds");
            o.rounds = int(n);
        } else if (k == "--loads") o.loads = positive_integer(v);
        else throw std::runtime_error("Unknown option " + k);
    }
    if (std::set<int>(o.nodes.begin(), o.nodes.end()).size() != o.nodes.size())
        throw std::runtime_error("Duplicate node IDs");
    return o;
}

void matrix_self_test() {
    for (std::size_t n : {1UL, 17UL, 4096UL}) {
        std::vector<Cell> cells(n);
        make_ring(cells.data(), n);
        std::vector<bool> seen(n, false);
        std::uint64_t pos = 0;
        for (std::size_t i = 0; i < n; ++i) {
            if (pos >= n || seen[pos]) throw std::runtime_error("Invalid permutation cycle");
            seen[pos] = true;
            pos = cells[pos].next;
        }
        if (pos != 0 || chase(cells.data(), 0, n * 2) != 0)
            throw std::runtime_error("Pointer chase failed");
    }
    for (int node : {0, 1, 7, 63, 64, 128}) {
        auto mask = binding_mask(node);
        constexpr std::size_t bits = sizeof(unsigned long) * 8;
        if (mask.maxnode - 1 <= static_cast<unsigned long>(node) ||
            mask.words[node / bits] != (1UL << (node % bits)))
            throw std::runtime_error("Invalid node mask");
    }
    if (median({3, 1, 2}) != 2 || median({4, 1, 3, 2}) != 2.5)
        throw std::runtime_error("Median failed");
    std::cout << "Self-test passed: random single-cycle ring, dependent loads, node masks, median.\n";
}

const char* access_class(const Domain& reader, const Domain& data) {
    if (reader.node == data.node) return "intra_chip";
    return reader.socket == data.socket ? "inter_chip_same_socket" : "inter_socket";
}

int main(int argc, char** argv) {
    try {
        const auto o = matrix_options(argc, argv);
        if (o.help) {
            std::cout << "./memory_latency --list\n"
                         "./memory_latency --nodes 0,1,4,5 --output results\n"
                         "Options: --size-mib 512 (or --size-kib N), --rounds 7, --loads 2000000\n"
                         "Without --nodes, test every accessible CPU-bearing NUMA node.\n"
                         "One representative CPU per node; requests are measured sequentially.\n";
            return 0;
        }
        if (o.test) { matrix_self_test(); return 0; }
        const auto allowed = allowed_cpus();
        const auto topology = domains(allowed);
        if (o.list) {
            for (const auto& d : topology)
                std::cout << "node=" << d.node << " socket=" << d.socket
                          << " representative_cpu=" << d.cpus.front()
                          << " allowed_core_count=" << d.cpus.size() << '\n';
            return 0;
        }
        std::vector<Domain> selected;
        if (o.nodes.empty()) selected = topology;
        else {
            for (int node : o.nodes) {
                auto it = std::find_if(topology.begin(), topology.end(),
                    [&](const Domain& d) { return d.node == node; });
                if (it == topology.end()) throw std::runtime_error("Node unavailable: " + std::to_string(node));
                selected.push_back(*it);
            }
        }
        if (selected.empty()) throw std::runtime_error("No accessible NUMA nodes");
        std::filesystem::create_directories(o.output);
        auto open_csv = [&](const std::string& name) {
            std::ofstream f(std::filesystem::path(o.output) / name);
            if (!f) throw std::runtime_error("Cannot create output " + name);
            f << std::fixed << std::setprecision(3);
            return f;
        };
        auto raw = open_csv("rounds.csv");
        auto summary = open_csv("summary.csv");
        auto matrix = open_csv("matrix_ns.csv");
        auto layout = open_csv("topology.csv");
        raw << "reader_node,data_node,reader_cpu,reader_socket,data_socket,access_class,round,loads,ns_per_load\n";
        summary << "reader_node,data_node,reader_cpu,reader_socket,data_socket,access_class,working_set_mib,loads_per_round,rounds,median_ns_per_load,min_ns_per_load,max_ns_per_load,page_location\n";
        layout << "node,socket,reader_cpu\n";
        matrix << "reader_node";
        for (const auto& d : selected) {
            matrix << ",node" << d.node;
            layout << d.node << ',' << d.socket << ',' << d.cpus.front() << '\n';
        }
        matrix << '\n';
        std::vector<std::vector<double>> medians(selected.size(), std::vector<double>(selected.size()));
        const double size_mib = double(o.size_bytes) / MiB;
        std::cerr << "Working set " << size_mib << " MiB, " << o.rounds << " rounds per pair.\n";
        // Keep only one large mapping alive. The target CPU initializes it,
        // then becomes idle while each requesting CPU measures the same ring.
        for (std::size_t col = 0; col < selected.size(); ++col) {
            const auto& data = selected[col];
            pin(data.cpus.front());
            Region region(o.size_bytes, data.node);
            make_ring(region.cells, region.count());
            const auto initial = region.verify(data.node);
            for (std::size_t row = 0; row < selected.size(); ++row) {
                const auto& reader = selected[row];
                pin(reader.cpus.front());
                std::uint64_t index = chase(region.cells, 0, region.count());
                std::vector<double> samples;
                for (int round = 0; round < o.rounds; ++round) {
                    auto ns = timed_chase(region, index, o.loads);
                    samples.push_back(ns);
                    raw << reader.node << ',' << data.node << ',' << reader.cpus.front() << ','
                        << reader.socket << ',' << data.socket << ',' << access_class(reader, data) << ','
                        << round + 1 << ',' << o.loads << ',' << ns << '\n';
                }
                const auto final = region.verify(data.node);
                const char* placement = initial == "verified_sample" && final == "verified_sample"
                    ? "verified_sample" : "unverified";
                const auto bounds = std::minmax_element(samples.begin(), samples.end());
                const double result = median(samples);
                medians[row][col] = result;
                summary << reader.node << ',' << data.node << ',' << reader.cpus.front() << ','
                    << reader.socket << ',' << data.socket << ',' << access_class(reader, data) << ','
                        << size_mib << ',' << o.loads << ',' << o.rounds << ',' << result << ','
                    << *bounds.first << ',' << *bounds.second << ',' << placement << '\n';
                raw.flush();
                summary.flush();
                std::cerr << "CPU " << reader.cpus.front() << " (node " << reader.node
                          << ") -> DRAM node " << data.node << ": " << std::fixed
                          << std::setprecision(3) << result << " ns/load, " << placement << '\n';
            }
        }
        for (std::size_t row = 0; row < selected.size(); ++row) {
            matrix << selected[row].node;
            for (double value : medians[row]) matrix << ',' << value;
            matrix << '\n';
        }
        std::cerr << "Completed: " << o.output << "/matrix_ns.csv\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }
}
