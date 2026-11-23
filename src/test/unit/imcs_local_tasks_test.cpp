#include "access/htap/imcs_local_tasks.h"
#include <algorithm>
#include <cassert>
#include <thread>

int main()
{
    ImcsLocalTaskQueues queues;
    std::vector<unsigned> ids;
    std::vector<int> homes;
    for (unsigned i = 0; i < 192; ++i) { ids.push_back(i + 100); homes.push_back(i % 8); }
    std::vector<int> seen(192, 0);
    std::mutex result;
    std::vector<std::thread> threads;
    // More workers than partitions per node; empty/late workers never wait.
    for (int w = 0; w < 168; ++w) threads.emplace_back([&, w] {
        for (;;) {
            int task = queues.claim(1, 0, 1, w % 8, ids, homes);
            if (task < 0) break;
            assert(homes[task] == w % 8);
            std::lock_guard<std::mutex> lock(result);
            ++seen[task];
        }
    });
    for (auto& t : threads) t.join();
    assert(std::all_of(seen.begin(), seen.end(), [](int n) { return n == 1; }));
    assert(queues.claim(1, 0, 1, 0, ids, homes) == -1);
    // A rescan and a separate plan each own independent task sets.
    assert(queues.claim(1, 1, 1, 0, ids, homes) == 0);
    assert(queues.claim(2, 0, 1, 0, ids, homes) == 0);
    // Backward claims retain physical partition indices and direction.
    assert(queues.claim(3, 0, -1, 7, ids, homes) == 191);
    assert(queues.claim(3, 0, -1, 7, ids, homes) == 183);
    auto changed = ids;
    changed[0]++;
    bool rejected = false;
    try { queues.claim(1, 0, 1, 0, changed, homes); }
    catch (const std::runtime_error&) { rejected = true; }
    assert(rejected);
    // Error unwinding releases the mutex and preserves other queues.
    assert(queues.claim(1, 1, 1, 0, ids, homes) == 8);
    assert(queues.claim(4, 0, 1, 99, ids, homes) == -1);
}
