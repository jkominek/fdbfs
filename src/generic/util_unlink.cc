#include "util_unlink.h"

#include <string.h>
#include <time.h>

// Read the future, and indicate whether or not there were some KVs
std::expected<bool, int> keyvalue_range_is_empty(FDBFuture *future) {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  const fdb_error_t err =
      fdb_future_get_keyvalue_array(future, &kvs, &kvcount, &more);
  if (err) {
    return std::unexpected(err);
  }
  return kvcount == 0;
}

std::expected<UnlinkParsedInode, UnlinkParseError>
parse_unlink_target_inode(FDBFuture *inode_metadata_future,
                          fuse_ino_t expected_ino) {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  const fdb_error_t err = fdb_future_get_keyvalue_array(inode_metadata_future,
                                                        &kvs, &kvcount, &more);
  // step 1 check that there's a KV there
  if (err) {
    return std::unexpected(
        UnlinkParseError{.err = err, .why = "inode metadata fetch failed"});
  }
  if (kvcount <= 0) {
    return std::unexpected(
        UnlinkParseError{.err = EIO, .why = "target inode missing"});
  }

  // step 2 check that the key length matches the expectation
  const auto expected_inode_key = pack_inode_key(expected_ino);
  const FDBKeyValue inode_kv = kvs[0];
  if ((inode_kv.key_length != static_cast<int>(expected_inode_key.size())) ||
      (memcmp(inode_kv.key, expected_inode_key.data(),
              expected_inode_key.size()) != 0)) {
    return std::unexpected(
        UnlinkParseError{.err = EIO, .why = "target inode key mismatch"});
  }

  // step 3 verify that the inode protobuf is valid
  UnlinkParsedInode parsed;
  if (!(parsed.inode.ParseFromArray(inode_kv.value, inode_kv.value_length) &&
        parsed.inode.IsInitialized() && parsed.inode.has_nlinks())) {
    return std::unexpected(
        UnlinkParseError{.err = EIO, .why = "target inode malformed"});
  }
  if (parsed.inode.inode() != expected_ino) {
    return std::unexpected(
        UnlinkParseError{.err = EIO, .why = "target inode id mismatch"});
  }

  // step 4 check for use records
  parsed.inode_in_use = false;
  for (int i = 1; i < kvcount; i++) {
    const FDBKeyValue &kv = kvs[i];
    // not a use key, so it doesn't count
    if ((kv.key_length <= inode_key_length) ||
        (kv.key[inode_key_length] != INODE_USE_PREFIX)) {
      continue;
    }
    if (kv.key_length <
        inode_key_length + 1 + static_cast<int>(sizeof(fuse_ino_t))) {
      return std::unexpected(
          UnlinkParseError{.err = EIO, .why = "malformed use record key"});
    }

    // this one shouldn't be possible, and reflects either a bug in our code
    // or breakage in foundationdb
    fuse_ino_t encoded_ino = 0;
    memcpy(&encoded_ino, kv.key + key_prefix.size() + 1, sizeof(encoded_ino));
    encoded_ino = be64toh(encoded_ino);
    if (encoded_ino != expected_ino) {
      return std::unexpected(
          UnlinkParseError{.err = EIO, .why = "use record key inode mismatch"});
    }

    parsed.inode_in_use = true;
    break;
  }

  // at this point we know that the inode is valid and whether or not there
  // are use records
  return parsed;
}

std::expected<void, int>
apply_unlink_target_mutation(FDBTransaction *transaction, INodeRecord &inode,
                             bool inode_in_use,
                             const UnlinkApplyOptions &options) {
  if (!inode.has_nlinks()) {
    return std::unexpected(EIO);
  }

  switch (options.nlink_mutation) {
  case UnlinkNlinkMutation::Decrement:
    if (inode.nlinks() == 0) {
      return std::unexpected(EIO);
    }
    inode.set_nlinks(inode.nlinks() - 1);
    break;
  case UnlinkNlinkMutation::SetZero:
    inode.set_nlinks(0);
    break;
  }

  struct timespec tv;
  clock_gettime(CLOCK_REALTIME, &tv);
  update_ctime(&inode, &tv);

  if (!fdb_set_protobuf(transaction, pack_inode_key(inode.inode()), inode)) {
    return std::unexpected(EIO);
  }

  if ((inode.nlinks() == 0) ||
      (options.unlink_directory_semantics && (inode.nlinks() <= 1))) {
    // all done with the inode
    if (inode_in_use) {
      // but somebody is using it, so mark it for garbage collection
      const auto key = pack_garbage_key(inode.inode());
      const uint8_t b = 0x00;
      fdb_transaction_set(transaction, key.data(), key.size(), &b, 1);
    } else {
      erase_inode(transaction, inode.inode());
    }
  }

  return {};
}
