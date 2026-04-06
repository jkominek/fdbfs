#include "fuse/fuse_inflight_action.h"
#include "generic/inflight.h"

template class InflightT<FuseInflightAction>;
template class Inflight_markusedT<FuseInflightAction>;
