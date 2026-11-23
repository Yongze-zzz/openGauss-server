#ifndef IMCS_ADAPTIVE_H
#define IMCS_ADAPTIVE_H
#include "postgres.h"
struct IMCSDesc;
struct Instrumentation;
struct ImcsAdaptSample {
    IMCSDesc* desc;
    double cycles;
    bool entered;
};
// Registration / partition transitions are outside the vector hot loop.
extern void ImcsAdaptScanInit(const void* owner, IMCSDesc* desc);
extern void ImcsAdaptProgress(const void* owner, uint32 cuid, uint32 lastCuid);
// Batch hooks: TLS only for statistics, atomics only for mandatory admission.
extern void ImcsAdaptBegin(IMCSDesc* desc, Instrumentation* instr, volatile ImcsAdaptSample* sample);
extern void ImcsAdaptEnd(volatile ImcsAdaptSample* sample, Instrumentation* instr, bool done, bool failed);
extern void ImcsAdaptWorkerLeave();
extern void ImcsAdaptTick();
extern long ImcsAdaptWaitMs();
extern char* ImcsAdaptStatusJson(Oid root);
#endif
