// Query-derived Buffer -> Consumer copy replay, not an SQL executor or link test.
// TSV: consumer_worker cpu execution_node source_home chunk_bytes copies
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <linux/mempolicy.h>
#include <map>
#include <memory>
#include <mutex>
#include <sched.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <vector>

using Clock = std::chrono::steady_clock;
static uint64_t now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}
static void check(bool ok, const std::string& message)
{
    if (!ok) throw std::runtime_error(message + ": " + strerror(errno));
}
struct Buffer {
    void* ptr;
    size_t size;
    uint64_t reprobes = 0;
    Buffer(size_t bytes, int node, unsigned char pattern = 0) : size((bytes + 4095) / 4096 * 4096)
    {
        ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        check(ptr != MAP_FAILED, "mmap");
        madvise(ptr, size, MADV_NOHUGEPAGE);
        unsigned long mask = 1UL << node;
        check(syscall(SYS_mbind, ptr, size, MPOL_BIND, &mask, 64, 0) == 0, "mbind");
        memset(ptr, pattern, size);
        verify(node);
    }
    void verify(int node)
    {
        size_t n = std::min(size / 4096, size_t(32));
        std::vector<void*> pages(n);
        std::vector<int> homes(n, -999);
        for (size_t i = 0; i < n; ++i)
            pages[i] = static_cast<char*>(ptr) + (i * (size / 4096) / n) * 4096;
        for (int attempt = 0; attempt < 3; ++attempt) {
            check(syscall(SYS_move_pages, 0, n, pages.data(), nullptr, homes.data(), 0) == 0, "move_pages");
            bool retry = false;
            for (size_t i = 0; i < homes.size(); ++i) {
                if (homes[i] == -ENOENT && attempt < 2) {
                    // Querying a nonpresent PTE does not establish a wrong home.
                    // Fault in this sample, outside all timing, and QUERY again.
                    volatile unsigned char value = *static_cast<volatile unsigned char*>(pages[i]);
                    (void)value;
                    ++reprobes;
                    retry = true;
                }
            }
            if (!retry) break;
        }
        for (size_t i = 0; i < homes.size(); ++i) {
            if (homes[i] != node)
                throw std::runtime_error("page placement mismatch: expected=" + std::to_string(node) +
                    " observed=" + std::to_string(homes[i]) + " bytes=" + std::to_string(size) +
                    " page_index=" + std::to_string(i));
        }
    }
    ~Buffer() { munmap(ptr, size); }
};
struct Chunk {
    int home;
    size_t bytes;
    uint64_t copies;
    unsigned char pattern;
    unsigned char* source = nullptr;
    std::unique_ptr<Buffer> memory;
};
struct Worker {
    int id, cpu, node;
    std::vector<Chunk> chunks;
    uint64_t start = 0, end = 0, bytes = 0, checksum = 0;
    long minorFaults = 0, majorFaults = 0;
    uint64_t placementReprobes = 0;
    std::string error;
};

int main(int argc, char** argv)
{
    try {
        check(argc == 4, "usage: stream_consumer_replay plan.tsv isolated|concurrent multiplier");
        bool isolated = std::string(argv[2]) == "isolated";
        check(isolated || std::string(argv[2]) == "concurrent", "unknown schedule");
        uint64_t multiplier = std::stoull(argv[3]);
        check(multiplier > 0 && multiplier <= 1024, "invalid work multiplier");
        std::ifstream input(argv[1]); check(input.good(), "open replay plan");
        std::map<int, Worker> workers;
        int id, cpu, node, home; size_t bytes; uint64_t copies;
        while (input >> id >> cpu >> node >> home >> bytes >> copies) {
            check(node >= 0 && node < 8 && home >= 0 && home < 8 && bytes > 0 && bytes <= 16*1024*1024 && copies > 0, "invalid chunk");
            auto found = workers.find(id);
            if (found == workers.end()) {
                Worker w; w.id = id; w.cpu = cpu; w.node = node;
                found = workers.emplace(id, std::move(w)).first;
            }
            check(found->second.cpu == cpu && found->second.node == node, "inconsistent worker location");
            unsigned char pattern = 1 + (found->second.chunks.size() % 254);
            found->second.chunks.push_back({home, bytes, copies, pattern, nullptr, nullptr});
        }
        check(input.eof() && !workers.empty(), "malformed/empty replay plan");
        size_t sourceMappings = 0;
        for (const auto& pair : workers) sourceMappings += pair.second.chunks.size();
        // Leave headroom below the usual vm.max_map_count=65530. Only very large
        // plans use pooled VMAs; ordinary plans retain the original layout.
        const bool poolSourceMappings = sourceMappings + workers.size() > 60000;
        cpu_set_t allowed; CPU_ZERO(&allowed);
        check(sched_getaffinity(0, sizeof(allowed), &allowed) == 0, "getaffinity");
        std::map<int,int> cpuOwners;
        std::vector<int> nodes;
        for (auto& pair : workers) {
            auto& w = pair.second;
            check(w.cpu >= 0 && w.cpu < CPU_SETSIZE && CPU_ISSET(w.cpu, &allowed), "CPU unavailable");
            check(cpuOwners.emplace(w.cpu, w.id).second, "duplicate consumer CPU");
            nodes.push_back(w.node);
        }
        std::sort(nodes.begin(), nodes.end()); nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
        std::mutex mutex; std::condition_variable cv;
        size_t ready = 0, finished = 0;
        std::atomic<int> active(-2); // -2 waits, -1 starts all groups.
        std::atomic<bool> abort(false);
        std::vector<std::thread> threads;
        for (auto& pair : workers) {
            Worker* worker = &pair.second;
            threads.emplace_back([&, worker] {
                bool announced = false;
                try {
                    auto& w = *worker;
                    cpu_set_t affinity; CPU_ZERO(&affinity); CPU_SET(w.cpu, &affinity);
                    check(sched_setaffinity(0, sizeof(affinity), &affinity) == 0, "pin consumer");
                    size_t destinationSize = 0;
                    std::map<int, size_t> sourceSizes;
                    for (auto& c : w.chunks) {
                        if (poolSourceMappings)
                            sourceSizes[c.home] += (c.bytes + 4095) / 4096 * 4096;
                        else {
                            c.memory = std::make_unique<Buffer>(c.bytes, c.home, c.pattern);
                            c.source = static_cast<unsigned char*>(c.memory->ptr);
                        }
                        destinationSize = std::max(destinationSize, c.bytes);
                    }
                    std::map<int, std::unique_ptr<Buffer>> sourceBuffers;
                    if (poolSourceMappings) {
                        for (const auto& source : sourceSizes)
                            sourceBuffers.emplace(source.first, std::make_unique<Buffer>(source.second, source.first));
                        std::map<int, size_t> sourceOffsets;
                        for (auto& c : w.chunks) {
                            auto& source = sourceBuffers.at(c.home);
                            size_t& offset = sourceOffsets[c.home];
                            c.source = static_cast<unsigned char*>(source->ptr) + offset;
                            memset(c.source, c.pattern, c.bytes);
                            offset += (c.bytes + 4095) / 4096 * 4096;
                        }
                    }
                    Buffer destination(destinationSize, w.node);
                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        ++ready; announced = true; cv.notify_all();
                        cv.wait(lock, [&] { return abort || active == -1 || active == w.node; });
                    }
                    if (abort) return;
                    std::vector<uint64_t> remaining;
                    for (auto& c : w.chunks) remaining.push_back(c.copies * multiplier);
                    struct rusage before{}, after{};
                    check(getrusage(RUSAGE_THREAD, &before) == 0, "getrusage before copy");
                    w.start = now_ns();
                    bool work = true;
                    unsigned char expected = 0;
                    while (work) {
                        work = false;
                        for (size_t i = 0; i < w.chunks.size(); ++i) {
                            if (remaining[i] == 0) continue;
                            auto& c = w.chunks[i]; work = true; --remaining[i];
                            memcpy(destination.ptr, c.source, c.bytes);
                            asm volatile("" ::: "memory");
                            w.bytes += c.bytes;
                            expected = c.pattern;
                        }
                    }
                    w.end = now_ns();
                    check(getrusage(RUSAGE_THREAD, &after) == 0, "getrusage after copy");
                    w.minorFaults = after.ru_minflt - before.ru_minflt;
                    w.majorFaults = after.ru_majflt - before.ru_majflt;
                    check(w.majorFaults == 0, "major page fault during timed copy; reject contaminated measurement");
                    check(sched_getcpu() == w.cpu, "consumer CPU changed");
                    w.checksum = static_cast<unsigned char*>(destination.ptr)[0];
                    check(w.checksum == expected, "copy checksum");
                    destination.verify(w.node);
                    w.placementReprobes = destination.reprobes;
                    if (poolSourceMappings) {
                        for (auto& source : sourceBuffers) {
                            source.second->verify(source.first);
                            w.placementReprobes += source.second->reprobes;
                        }
                    } else {
                        for (auto& c : w.chunks) {
                            c.memory->verify(c.home);
                            w.placementReprobes += c.memory->reprobes;
                        }
                    }
                } catch (const std::exception& e) {
                    worker->error = e.what(); abort = true;
                }
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    if (!announced) ++ready;
                    ++finished;
                }
                cv.notify_all();
            });
        }
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [&] { return ready == workers.size(); });
        }
        // Caller starts PMU collection after this allocation/placement barrier.
        std::cout << "READY\n" << std::flush;
        std::string go; std::getline(std::cin, go);
        if (go != "GO") abort = true;
        uint64_t trialStart = now_ns();
        if (isolated) {
            size_t expected = 0;
            for (int n : nodes) {
                for (auto& p : workers) expected += p.second.node == n;
                std::unique_lock<std::mutex> lock(mutex);
                active = n; cv.notify_all();
                cv.wait(lock, [&] { return abort || finished >= expected; });
                if (abort) break;
            }
        } else {
            std::lock_guard<std::mutex> lock(mutex);
            active = -1; cv.notify_all();
        }
        if (abort) cv.notify_all();
        for (auto& t : threads) t.join();
        uint64_t trialEnd = now_ns();
        for (auto& p : workers) check(p.second.error.empty(), p.second.error);
        check(!abort, "replay aborted");
        std::cout << "{\"schema_version\":2,\"schedule\":\"" << argv[2] << "\",\"multiplier\":" << multiplier
                  << ",\"start_ns\":" << trialStart << ",\"end_ns\":" << trialEnd << ",\"workers\":[";
        bool first = true;
        for (auto& p : workers) {
            auto& w = p.second;
            if (!first) std::cout << ','; first = false;
            std::cout << "{\"worker\":" << w.id << ",\"cpu\":" << w.cpu << ",\"node\":" << w.node
                      << ",\"bytes\":" << w.bytes << ",\"start_ns\":" << w.start << ",\"end_ns\":" << w.end
                      << ",\"minor_faults\":" << w.minorFaults << ",\"major_faults\":" << w.majorFaults
                      << ",\"placement_reprobes\":" << w.placementReprobes
                      << ",\"checksum\":" << w.checksum << '}';
        }
        std::cout << "]}\n" << std::flush;
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n'; return 1;
    }
}
