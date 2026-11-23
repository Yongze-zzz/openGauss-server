#include "access/htap/imcs_numa_policy.h"
#include <algorithm>
#include <cmath>
namespace imcsnuma {
bool worthMoving(double source, double target, int sourceCpus, int targetCpus,
                 double demand, double remainingCpuUs, std::uint64_t pages, double pageCost)
{
    if (sourceCpus <= 0 || targetCpus <= 0 || !std::isfinite(demand) || demand <= 0 ||
        !std::isfinite(remainingCpuUs) || remainingCpuUs <= 0 || pages == 0 ||
        !std::isfinite(pageCost) || pageCost <= 0 || source <= SOURCE_MIN ||
        target >= TARGET_MAX || source - target <= CPU_GAP)
        return false;
    double afterSource = std::max(0.0, source - demand / sourceCpus);
    double afterTarget = target + demand / targetCpus;
    double gain = (source - target) - std::abs(afterSource - afterTarget);
    double costUs = static_cast<double>(pages) * pageCost;
    return afterTarget < TARGET_MAX && gain >= 0.02 &&
        remainingCpuUs > 3 * costUs && gain * remainingCpuUs > 2 * costUs;
}
}
