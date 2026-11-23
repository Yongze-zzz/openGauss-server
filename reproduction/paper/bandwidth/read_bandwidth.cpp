// Sequential NUMA-home reads. Reports foreground payload bytes, not PMU/link bytes.
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <linux/mempolicy.h>
#include <mutex>
#include <sched.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <vector>

using Clock = std::chrono::steady_clock;
static uint64_t now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}

static void require(bool ok, const std::string& what)
{
    if (!ok) throw std::runtime_error(what);
}

// The memory clobber prevents reuse of a previous sweep's checksum.
// Multiple independent accumulators permit streaming/vectorized reads.
__attribute__((noinline)) static uint64_t sweep(const uint64_t* p, size_t words)
{
    asm volatile("" ::: "memory");
    uint64_t a = 0, b = 0, c = 0, d = 0;
    for (size_t i = 0; i < words; i += 4) {
        a += p[i]; b += p[i + 1]; c += p[i + 2]; d += p[i + 3];
    }
    return a + b + c + d;
}

static int cpu_node(int cpu)
{
    for (int node = 0; node < 64; ++node) {
        std::ifstream f("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                        "/node" + std::to_string(node) + "/cpulist");
        if (f.good()) return node;
    }
    return -1;
}

struct Buffer {
    void* data = MAP_FAILED;
    size_t bytes;
    size_t page = size_t(sysconf(_SC_PAGESIZE));
    Buffer(size_t size, int node) : bytes(size)
    {
        data = mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        require(data != MAP_FAILED, "mmap: " + std::string(strerror(errno)));
        // Base pages make sampled move_pages results unambiguous. No global change.
        madvise(data, bytes, MADV_NOHUGEPAGE);
        unsigned long mask = 1UL << node;
        if (syscall(SYS_mbind, data, bytes, MPOL_BIND, &mask, 64, 0) != 0) {
            const std::string err = strerror(errno);
            munmap(data, bytes); data = MAP_FAILED;
            throw std::runtime_error("mbind: " + err);
        }
    }
    ~Buffer() { if (data != MAP_FAILED) munmap(data, bytes); }
    size_t check(int node)
    {
        const size_t pages = bytes / page;
        const size_t n = std::min(size_t(1024), pages);
        std::vector<void*> addresses(n);
        std::vector<int> status(n, -999);
        for (size_t i = 0; i < n; ++i)
            addresses[i] = static_cast<char*>(data) + (i * pages / n) * page;
        require(syscall(SYS_move_pages, 0, n, addresses.data(), nullptr,
                        status.data(), 0) == 0,
                "move_pages query failed: " + std::string(strerror(errno)));
        for (int s : status)
            require(s == node, "page sample mismatch/unknown: expected N" +
                    std::to_string(node) + ", got " + std::to_string(s));
        return n;
    }
};

struct Result {
    uint64_t bytes = 0, start = 0, end = 0, checksum = 0;
    size_t pre_pages = 0, post_pages = 0;
    int cpu = -1, node = -1;
    std::string error;
};

int main(int argc, char** argv)
{
    try {
        // Positional arguments are supplied by run.py.
        require(argc == 6, "usage: read_bandwidth CPU_LIST HOME_NODE FG_THREADS MIB SECONDS");
        std::vector<int> cpus;
        std::istringstream input(argv[1]);
        std::string item;
        while (std::getline(input, item, ',')) cpus.push_back(std::stoi(item));
        int home = std::stoi(argv[2]), foreground = std::stoi(argv[3]);
        long long mib = std::stoll(argv[4]);
        double seconds = std::stod(argv[5]);
        require(home >= 0 && home < 64 && foreground > 0 &&
                size_t(foreground) <= cpus.size() && mib >= 1 && seconds >= 0.1,
                "invalid arguments");
        const size_t bytes = size_t(mib) * 1024 * 1024;
        cpu_set_t allowed;
        require(sched_getaffinity(0, sizeof(allowed), &allowed) == 0, "getaffinity failed");
        auto sorted = cpus;
        std::sort(sorted.begin(), sorted.end());
        require(std::adjacent_find(sorted.begin(), sorted.end()) == sorted.end(),
                "duplicate CPU");
        for (int cpu : cpus)
            require(cpu >= 0 && cpu < CPU_SETSIZE && CPU_ISSET(cpu, &allowed),
                    "CPU unavailable: " + std::to_string(cpu));

        std::mutex mutex;
        std::condition_variable cv;
        size_t ready = 0;
        std::atomic<uint64_t> start(0);
        std::atomic<bool> abort(false);
        std::vector<Result> results(cpus.size());
        std::vector<std::thread> threads;
        for (size_t t = 0; t < cpus.size(); ++t) {
            threads.emplace_back([&, t] {
                bool announced = false;
                try {
                    cpu_set_t set;
                    CPU_ZERO(&set); CPU_SET(cpus[t], &set);
                    require(sched_setaffinity(0, sizeof(set), &set) == 0, "pin failed");
                    Buffer buffer(bytes, home);
                    auto p = static_cast<uint64_t*>(buffer.data);
                    const size_t words = bytes / sizeof(uint64_t);
                    for (size_t i = 0; i < words; ++i)
                        p[i] = uint64_t(i) ^ (uint64_t(t + 1) << 32);
                    auto& r = results[t];
                    r.pre_pages = buffer.check(home);
                    r.cpu = sched_getcpu(); r.node = cpu_node(r.cpu);
                    require(r.cpu == cpus[t] && r.node >= 0, "CPU/node verification failed");
                    {
                        std::lock_guard<std::mutex> lock(mutex);
                        ++ready; announced = true;
                    }
                    cv.notify_one();
                    uint64_t epoch;
                    while (!(epoch = start.load(std::memory_order_acquire)))
                        std::this_thread::yield();
                    if (abort.load()) return;
                    while (now_ns() < epoch) std::this_thread::yield();
                    r.start = now_ns();
                    const uint64_t deadline = epoch + uint64_t(seconds * 1e9);
                    do {
                        r.checksum += sweep(p, words);
                        r.bytes += bytes;
                    } while (now_ns() < deadline);
                    r.end = now_ns();
                    require(sched_getcpu() == cpus[t], "CPU changed despite binding");
                    r.post_pages = buffer.check(home);
                } catch (const std::exception& e) {
                    results[t].error = e.what(); abort.store(true);
                    if (!announced) {
                        { std::lock_guard<std::mutex> lock(mutex); ++ready; }
                        cv.notify_one();
                    }
                }
            });
        }
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [&] { return ready == cpus.size(); });
        }
        // Give all initialized readers the same measurement deadline.
        start.store(now_ns() + 10000000, std::memory_order_release);
        for (auto& thread : threads) thread.join();
        for (const auto& r : results) require(r.error.empty(), r.error);
        require(!abort.load(), "worker aborted");
        uint64_t total = 0, first = UINT64_MAX, last = 0;
        for (int i = 0; i < foreground; ++i) {
            total += results[i].bytes;
            first = std::min(first, results[i].start);
            last = std::max(last, results[i].end);
        }
        // Decimal GB/s. Aggregate uses one common foreground window.
        double elapsed = double(last - first) * 1e-9;
        std::cout << std::setprecision(12)
                  << "{\"foreground_GB_s\":" << double(total) / elapsed / 1e9
                  << ",\"foreground_bytes\":" << total
                  << ",\"foreground_window_s\":" << elapsed
                  << ",\"workers\":[";
        for (size_t i = 0; i < results.size(); ++i) {
            auto& r = results[i];
            if (i) std::cout << ',';
            std::cout << "{\"cpu\":" << r.cpu << ",\"exec_node\":" << r.node
                      << ",\"home_node\":" << home
                      << ",\"foreground\":" << (int(i) < foreground ? "true" : "false")
                      << ",\"bytes\":" << r.bytes
                      << ",\"start_ns\":" << r.start << ",\"end_ns\":" << r.end
                      << ",\"GB_s\":" << double(r.bytes) / double(r.end - r.start)
                      << ",\"pre_page_samples\":" << r.pre_pages
                      << ",\"post_page_samples\":" << r.post_pages
                      << ",\"checksum\":" << r.checksum << '}';
        }
        std::cout << "]}\n";
    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << '\n';
        return 1;
    }
}
