#ifndef IMCS_NUMA_CPU_H
#define IMCS_NUMA_CPU_H
#include <sched.h>
#include <cstdint>
#include <cstdio>
namespace imcsnuma {
constexpr int MAX_NODES = sizeof(unsigned long) * 8;
// Capture before server initialization; preserve an explicit numactl MPOL_BIND.
int CaptureLauncherMemoryPolicy();
unsigned long LauncherBindNodes();
int RestoreLauncherMemoryPolicy();
class CpuSampler {
public:
    CpuSampler();
    ~CpuSampler();
    void configure(const cpu_set_t& scope);
    void reset();
    bool sample(); // One pass through /proc/stat; cached CPU->node map.
    int count[MAX_NODES];
    double utilization[MAX_NODES];
private:
    struct Cpu { std::uint64_t busy, total; };
    int node_[CPU_SETSIZE];
    Cpu previous_[CPU_SETSIZE];
    FILE* input_;
    bool initialized_;
};
}
#endif
