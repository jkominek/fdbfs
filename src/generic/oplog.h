#ifndef __OPLOG_H__
#define __OPLOG_H__

#include <optional>
#include <utility>

#include <cstddef>
#include <cstdint>

uint64_t allocate_oplog_id();
void mark_oplog_live(uint64_t op_id);
void mark_oplog_dead(uint64_t op_id);
const char *op_id_to_cstr(const std::optional<uint64_t> &op_id, char *buf,
                          size_t buflen);

[[nodiscard]] std::optional<std::pair<uint64_t, uint64_t>>
claim_local_oplog_cleanup_span();

#endif
