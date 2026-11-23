#ifndef VSONICSHAREDAGG_H
#define VSONICSHAREDAGG_H

#ifdef ENABLE_HTAP
class StreamNodeGroup;
struct Stream;
struct StreamSharedContext;
struct VecAggState;
class VectorBatch;

void InitSonicAggExchange(StreamSharedContext* context, Stream* stream);
void DestroySonicAggExchanges(StreamNodeGroup* group);
bool StartSonicSharedAgg(VecAggState* node);
VectorBatch* RunSonicSharedAgg(VecAggState* node);
void EndSonicSharedAgg(VecAggState* node);
#endif

#endif
