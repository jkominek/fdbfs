#include "oplog.h"

#include <atomic>
#include <iterator>
#include <limits>
#include <mutex>
#include <set>
#include <stdio.h>
#include <stdlib.h>

namespace {

std::mutex oplog_tracking_mutex;
std::set<uint64_t> oplog_live_ids;
std::set<uint64_t> oplog_dead_ids;
std::atomic<uint64_t> next_oplog_id = 1;

} // namespace

uint64_t allocate_oplog_id() {
  const uint64_t id = next_oplog_id.fetch_add(1, std::memory_order_relaxed);
  if (id == 0) {
    // wrapped around; would allow reuse of old op ids.
    // TODO allow wrap-around.
    std::terminate();
  }
  return id;
}

void mark_oplog_live(uint64_t op_id) {
  std::lock_guard<std::mutex> guard(oplog_tracking_mutex);
  oplog_live_ids.insert(op_id);
}

void mark_oplog_dead(uint64_t op_id) {
  std::lock_guard<std::mutex> guard(oplog_tracking_mutex);
  oplog_live_ids.erase(op_id);
  oplog_dead_ids.insert(op_id);
}

const char *op_id_to_cstr(const std::optional<uint64_t> &op_id, char *buf,
                          size_t buflen) {
  if (!op_id.has_value()) {
    return "null";
  }
  snprintf(buf, buflen, "%llu", static_cast<unsigned long long>(*op_id));
  return buf;
}

std::optional<std::pair<uint64_t, uint64_t>> claim_local_oplog_cleanup_span() {
  std::lock_guard<std::mutex> guard(oplog_tracking_mutex);
  if (oplog_dead_ids.empty()) {
    return std::nullopt;
  }

  const uint64_t min_dead = *oplog_dead_ids.begin();
  uint64_t stop = 0;
  std::set<uint64_t>::iterator erase_end;

  if (oplog_live_ids.empty()) {
    const uint64_t max_dead = *oplog_dead_ids.rbegin();
    if (max_dead == std::numeric_limits<uint64_t>::max()) {
      // we reserve overflow as impossible by design.
      std::terminate();
    }
    stop = max_dead + 1;
    erase_end = oplog_dead_ids.end();
  } else {
    const uint64_t min_live = *oplog_live_ids.begin();
    if (min_dead >= min_live) {
      // NOTE if this situation persists, we'll fail to
      // make progress clearing out the op log
      return std::nullopt;
    }
    erase_end = oplog_dead_ids.lower_bound(min_live);
    if (erase_end == oplog_dead_ids.begin()) {
      return std::nullopt;
    }
    const uint64_t max_clear = *std::prev(erase_end);
    if (max_clear == std::numeric_limits<uint64_t>::max()) {
      std::terminate();
    }
    stop = max_clear + 1;
  }

  oplog_dead_ids.erase(oplog_dead_ids.begin(), erase_end);
  return std::make_pair(min_dead, stop);
}
