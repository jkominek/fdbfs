#ifndef __UTIL_SQLITE_H__
#define __UTIL_SQLITE_H__

#include <expected>
#include <filesystem>
#include <vector>

#include "generic/util.h"

[[nodiscard]] std::expected<INodeRecord, int>
resolve_path(const std::filesystem::path &path,
             bool terminal_may_be_directory = false);

[[nodiscard]] std::expected<INodeRecord, int>
lookup_inode(fdbfs_ino_t parent, std::string_view name);

void forget_inodes_best_effort(const std::vector<fdbfs_ino_t> &inos);

#endif
