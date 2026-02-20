#ifndef __UTIL_UNLINK_H__
#define __UTIL_UNLINK_H__

#include <errno.h>

#include "util.h"

struct UnlinkParsedInode {
  INodeRecord inode;
  bool inode_in_use = false;
};

struct UnlinkParseError {
  int err = EIO;
  const char *why = "unlink parse error";
};

enum class UnlinkNlinkMutation {
  Decrement,
  SetZero,
};

struct UnlinkApplyOptions {
  UnlinkNlinkMutation nlink_mutation = UnlinkNlinkMutation::Decrement;
  bool unlink_directory_semantics = false;
};

[[nodiscard]] std::expected<bool, int>
keyvalue_range_is_empty(FDBFuture *future);

[[nodiscard]] std::expected<UnlinkParsedInode, UnlinkParseError>
parse_unlink_target_inode(FDBFuture *inode_metadata_future,
                          fuse_ino_t expected_ino);

[[nodiscard]] std::expected<void, int>
apply_unlink_target_mutation(FDBTransaction *transaction, INodeRecord &inode,
                             bool inode_in_use,
                             const UnlinkApplyOptions &options);

#endif // __UTIL_UNLINK_H__
