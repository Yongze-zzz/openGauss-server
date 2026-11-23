#ifndef IMCS_LOCAL_TASKS_H
#define IMCS_LOCAL_TASKS_H

#include <cstdint>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

/* One instance per StreamNodeGroup (query), destroyed after all producers quit.
 * Scanning and rescan cleanup stay with the executor that owns each range.
 */
class ImcsLocalTaskQueues {
    struct Queue {
        std::vector<unsigned int> oids;
        std::vector<int> nodes;
        std::map<int, std::vector<int> > tasks;
        std::map<int, std::size_t> next;
        int direction;
    };
    std::mutex mutex_;
    std::map<std::pair<int, std::uint64_t>, Queue> queues_;
    struct CuLayout {
        std::vector<unsigned int> oids;
        std::vector<std::uint32_t> limits;
    };
    std::map<std::pair<int, std::uint64_t>, CuLayout> cuLayouts_;
public:
    void freezeCuLimits(int plan, std::uint64_t generation, const std::vector<unsigned int>& oids,
                       std::uint32_t* limits)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = std::make_pair(plan, generation);
        auto found = cuLayouts_.find(key);
        if (found == cuLayouts_.end()) {
            CuLayout layout{oids, std::vector<std::uint32_t>(limits, limits + oids.size())};
            found = cuLayouts_.emplace(key, std::move(layout)).first;
        }
        if (found->second.oids != oids)
            throw std::runtime_error("IMCS CU partition list differs between workers");
        for (std::size_t i = 0; i < oids.size(); ++i)
            limits[i] = found->second.limits[i];
    }

    int claim(int plan, std::uint64_t generation, int direction, int node,
              const std::vector<unsigned int>& oids, const std::vector<int>& nodes)
    {
        if (oids.size() != nodes.size() || (direction != 1 && direction != -1))
            throw std::runtime_error("invalid IMCS local task list");
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = std::make_pair(plan, generation);
        auto it = queues_.find(key);
        if (it == queues_.end()) {
            // Build privately before publishing: allocation failure cannot leave
            // a partially initialized shared queue.
            Queue q;
            q.oids = oids; q.nodes = nodes; q.direction = direction;
            for (std::size_t i = 0; i < oids.size(); ++i) {
                int index = direction == 1 ? int(i) : int(oids.size() - i - 1);
                q.tasks[nodes[index]].push_back(index);
                q.next[nodes[index]] = 0;
            }
            it = queues_.emplace(key, std::move(q)).first;
        }
        Queue& q = it->second;
        if (q.oids != oids || q.nodes != nodes || q.direction != direction)
            throw std::runtime_error("IMCS local task pruning/layout differs between workers");
        auto tasks = q.tasks.find(node);
        if (tasks == q.tasks.end()) return -1;
        std::size_t& next = q.next.at(node);
        return next == tasks->second.size() ? -1 : tasks->second[next++];
    }
};
#endif
