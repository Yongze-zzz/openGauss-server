#ifndef IMCS_NUMA_POLICY_H
#define IMCS_NUMA_POLICY_H
#include <cstdint>
namespace imcsnuma {
// Initial research heuristics, not validated optimum settings.
constexpr double CPU_GAP = 0.20;
constexpr double SOURCE_MIN = 0.55;
constexpr double TARGET_MAX = 0.65;
constexpr unsigned STABLE_PERIODS = 2;
constexpr unsigned COOLDOWN_PERIODS = 3;
constexpr unsigned MIN_REMAINING_CUS = 8;
constexpr double INITIAL_MOVE_US_PER_PAGE = 20.0;
struct PlacementAction {
    std::uint64_t unit = 0, generation = 0;
    int source = -1, target = -1;
    const char* reason = "cpu-driven fixed-partition move";
};
// CPU demand is a measured worker-core estimate; no memory-bandwidth guarantee.
bool worthMoving(double source, double target, int sourceCpus, int targetCpus,
                 double demandCores, double remainingCpuUs, std::uint64_t pages,
                 double moveUsPerPage);
}
#endif
