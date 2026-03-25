#include "util_sqlite.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "generic/forget.hpp"
#include "generic/getinode.hpp"
#include "generic/lookup.hpp"
#include "generic/void_inflight_action.h"
#include "sqlite_inflight_action.h"

namespace {

constexpr auto kSqliteInflightTimeout = std::chrono::seconds(5);
constexpr fdbfs_ino_t kRootIno = 1;

std::future<SqliteResult> take_future(SqliteRequest *req) {
  return req->promise.get_future();
}

std::expected<SqliteReply, int>
wait_for_sqlite_result(std::future<SqliteResult> &future) {
  if (future.wait_for(kSqliteInflightTimeout) != std::future_status::ready) {
    return std::unexpected(ETIMEDOUT);
  }

  SqliteResult result = future.get();
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  return *std::move(result);
}

std::expected<INodeRecord, int> get_inode(fdbfs_ino_t ino) {
  auto req = std::make_unique<SqliteRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_getinode<SqliteInflightAction, std::monostate>(
      req.get(), ino, make_transaction(), std::monostate{});
  inflight->start();

  auto reply = wait_for_sqlite_result(future);
  if (!reply.has_value()) {
    return std::unexpected(reply.error());
  }
  if (!std::holds_alternative<SqliteReplyINode>(*reply)) {
    return std::unexpected(EIO);
  }
  return std::get<SqliteReplyINode>(*reply).inode;
}

} // namespace

std::expected<INodeRecord, int> lookup_inode(fdbfs_ino_t parent,
                                             std::string_view name) {
  auto req = std::make_unique<SqliteRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_lookup<SqliteInflightAction,
                                       SqliteInflightAction::INodeHandlerEntry>(
      req.get(), parent, std::string(name), make_transaction(),
      SqliteInflightAction::INodeHandlerEntry{});
  inflight->start();

  auto reply = wait_for_sqlite_result(future);
  if (!reply.has_value()) {
    return std::unexpected(reply.error());
  }
  if (!std::holds_alternative<SqliteReplyINode>(*reply)) {
    return std::unexpected(EIO);
  }
  return std::get<SqliteReplyINode>(*reply).inode;
}

void forget_inodes_best_effort(const std::vector<fdbfs_ino_t> &inos) {
  std::vector<ForgetEntry> entries;
  entries.reserve(inos.size());
  for (fdbfs_ino_t ino : inos) {
    auto generation = decrement_lookup_count(ino, 1);
    if (!generation.has_value()) {
      continue;
    }
    entries.push_back(ForgetEntry{.ino = ino, .generation = *generation});
  }
  if (entries.empty()) {
    return;
  }

  auto *inflight = new Inflight_forget<VoidInflightAction>(
      std::monostate{}, std::move(entries), make_transaction());
  inflight->start();
}

std::expected<INodeRecord, int>
resolve_path(const std::filesystem::path &path, bool terminal_may_be_directory) {
  const std::filesystem::path normalized = path.lexically_normal();

  if (normalized.empty() || normalized == std::filesystem::path("/")) {
    return get_inode(kRootIno);
  }

  auto root = get_inode(kRootIno);
  if (!root.has_value()) {
    return std::unexpected(root.error());
  }

  std::vector<std::string> components;
  for (const auto &component_path : normalized) {
    const std::string component = component_path.string();
    if (component.empty() || component == "/" || component == ".") {
      continue;
    }
    if (component == "..") {
      return std::unexpected(EINVAL);
    }
    components.push_back(component);
  }

  if (components.empty()) {
    return *std::move(root);
  }

  std::vector<INodeRecord> inode_chain;
  inode_chain.push_back(*std::move(root));
  std::vector<fdbfs_ino_t> looked_up_inodes;

  for (size_t index = 0; index < components.size(); ++index) {
    const bool is_last = (index + 1 == components.size());
    const INodeRecord &current = inode_chain.back();
    const mode_t current_type = current.mode() & S_IFMT;
    if ((current_type != S_IFDIR) && !is_last) {
      forget_inodes_best_effort(looked_up_inodes);
      return std::unexpected(ENOTDIR);
    }

    auto next = lookup_inode(current.inode(), components[index]);
    if (!next.has_value()) {
      forget_inodes_best_effort(looked_up_inodes);
      return std::unexpected(next.error());
    }

    INodeRecord resolved = *std::move(next);
    const mode_t resolved_type = resolved.mode() & S_IFMT;
    if (resolved_type == S_IFLNK) {
      std::filesystem::path preceding("/");
      for (size_t i = 0; i < index; ++i) {
        preceding /= components[i];
      }

      std::filesystem::path trailing;
      for (size_t i = index + 1; i < components.size(); ++i) {
        trailing /= components[i];
      }

      std::filesystem::path rewritten =
          (preceding / std::filesystem::path(resolved.symlink()))
              .lexically_normal();
      rewritten /= trailing;

      looked_up_inodes.push_back(resolved.inode());
      // resolve the updated path before forgetting unneeded inodes.
      // this ensures that they can't disappear on us until we're done
      // and saves on the db traffic of releasing and reacquiring the
      // use records.
      auto result = resolve_path(rewritten, terminal_may_be_directory);
      forget_inodes_best_effort(looked_up_inodes);
      return result;
    }

    if (is_last) {
      if ((resolved_type == S_IFDIR) && !terminal_may_be_directory) {
        looked_up_inodes.push_back(resolved.inode());
        forget_inodes_best_effort(looked_up_inodes);
        return std::unexpected(EISDIR);
      }
    } else if (resolved_type == S_IFREG) {
      looked_up_inodes.push_back(resolved.inode());
      forget_inodes_best_effort(looked_up_inodes);
      return std::unexpected(ENOTDIR);
    }

    looked_up_inodes.push_back(resolved.inode());
    inode_chain.push_back(std::move(resolved));
  }

  // remove the inode we're about to return from the set
  // that will be forgotten.
  if (!looked_up_inodes.empty()) {
    looked_up_inodes.pop_back();
  }
  forget_inodes_best_effort(looked_up_inodes);
  return inode_chain.back();
}
