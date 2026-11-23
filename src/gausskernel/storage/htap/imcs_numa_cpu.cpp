#include "access/htap/imcs_numa_cpu.h"
#include <numa.h>
#include <numaif.h>
#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <stdexcept>
namespace imcsnuma {
namespace {
int launcherMode = MPOL_DEFAULT;
unsigned long launcherNodes = 0;
}
int CaptureLauncherMemoryPolicy()
{
    int mode = MPOL_DEFAULT;
    unsigned long nodes = 0;
    if (get_mempolicy(&mode, &nodes, MAX_NODES, NULL, 0) != 0) return -1;
    if ((mode & 0xff) == MPOL_BIND) {
        launcherMode = mode;
        launcherNodes = nodes;
    }
    return 0;
}
unsigned long LauncherBindNodes() { return launcherNodes; }
int RestoreLauncherMemoryPolicy()
{
    return launcherNodes ? set_mempolicy(launcherMode, &launcherNodes, MAX_NODES) : 0;
}
CpuSampler::CpuSampler() : input_(NULL), initialized_(false)
{
    memset(count, 0, sizeof(count));
    memset(utilization, 0, sizeof(utilization));
    memset(previous_, 0, sizeof(previous_));
    std::fill(node_, node_ + CPU_SETSIZE, -1);
}
CpuSampler::~CpuSampler() { if (input_) fclose(input_); }
void CpuSampler::configure(const cpu_set_t& scope)
{
    memset(count, 0, sizeof(count));
    memset(previous_, 0, sizeof(previous_));
    std::fill(node_, node_ + CPU_SETSIZE, -1);
    initialized_ = false;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &scope)) continue;
        int node = numa_node_of_cpu(cpu);
        if (node < 0 || node >= MAX_NODES) throw std::runtime_error("invalid CPU NUMA topology");
        node_[cpu] = node;
        ++count[node];
    }
    if (!input_) input_ = fopen("/proc/stat", "r");
    if (!input_) throw std::runtime_error("cannot open /proc/stat");
}
void CpuSampler::reset()
{
    initialized_ = false;
    memset(previous_, 0, sizeof(previous_));
}
bool CpuSampler::sample()
{
    if (!input_) return false;
    rewind(input_);
    clearerr(input_);
    memset(utilization, 0, sizeof(utilization));
    bool seen[CPU_SETSIZE] = {};
    bool valid = initialized_;
    char line[4096];
    while (fgets(line, sizeof(line), input_)) {
        if (strncmp(line, "cpu", 3) != 0 || line[3] < '0' || line[3] > '9') continue;
        char* p;
        long cpu = strtol(line + 3, &p, 10);
        if (cpu < 0 || cpu >= CPU_SETSIZE || node_[cpu] < 0) continue;
        std::uint64_t v[8];
        for (int i = 0; i < 8; ++i) {
            char* end;
            v[i] = strtoull(p, &end, 10);
            if (end == p) { valid = false; v[i] = 0; }
            p = end;
        }
        Cpu current;
        current.busy = v[0] + v[1] + v[2] + v[5] + v[6] + v[7];
        current.total = current.busy + v[3] + v[4]; // guest is included in user/nice.
        Cpu old = previous_[cpu];
        if (!old.total || current.total <= old.total || current.busy < old.busy) valid = false;
        else utilization[node_[cpu]] += std::min(1.0, double(current.busy - old.busy) / (current.total - old.total));
        previous_[cpu] = current;
        seen[cpu] = true;
    }
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
        if (node_[cpu] >= 0 && !seen[cpu]) valid = false;
    for (int node = 0; node < MAX_NODES; ++node)
        if (count[node]) utilization[node] /= count[node];
    initialized_ = true;
    return valid && !ferror(input_);
}
}
