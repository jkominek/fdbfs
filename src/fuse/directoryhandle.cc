#include "directoryhandle.h"

#include "atomicfield.hpp"
#include "generic/util.h"
#include "void_inflight_action.h"

#include <time.h>

#include <utility>

namespace {

bool more_than_one_second_since(const struct timespec &since,
                                const struct timespec &now) {
  if (now.tv_sec > since.tv_sec + 1) {
    return true;
  }
  if (now.tv_sec < since.tv_sec + 1) {
    return false;
  }
  return now.tv_nsec > since.tv_nsec;
}

} // namespace

DirectoryHandle::DirectoryHandle(fuse_ino_t ino) : ino(ino) {}

// releases lk before returning; callers must not assume it is still held
void DirectoryHandle::launch_atime_update_locked(
    const struct timespec &target, const struct timespec &attempt_time,
    std::unique_lock<std::mutex> &lk) {
  auto encoded_target = encode_timespec(target);
  std::vector<AtomicFieldOp> ops;
  ops.push_back(AtomicFieldOp{
      .suffix = {'t', 'a'},
      .mutation_type = FDB_MUTATION_TYPE_MAX,
      .value =
          std::vector<uint8_t>(encoded_target.begin(), encoded_target.end()),
  });

  last_update_attempt = attempt_time;
  inflight_updates.fetch_add(1, std::memory_order_relaxed);

  auto *inflight = new Inflight_atomicfield<VoidInflightAction>(
      std::monostate{}, static_cast<fdbfs_ino_t>(ino), std::move(ops),
      make_transaction());
  inflight->set_on_done([this](int) {
    inflight_updates.fetch_sub(1, std::memory_order_release);
    inflight_updates.notify_one();
  });

  lk.unlock();
  inflight->start();
}

void DirectoryHandle::close() {
  {
    std::unique_lock<std::mutex> lk(state_mutex);
    closed = true;
    // launch a final attempt to update the atime
    if (latest_read.has_value()) {
      launch_atime_update_locked(*latest_read, *latest_read, lk);
    }
  }

  // wait for all pending inflights to complete before we return to the
  // caller. this ensures that they can't observe the atime update _after_
  // the close returns. if there are no updates running, then no problem
  // we'll exit fast.
  while (true) {
    const size_t pending = inflight_updates.load(std::memory_order_acquire);
    if (pending == 0) {
      break;
    }
    inflight_updates.wait(pending, std::memory_order_acquire);
  }
}

bool DirectoryHandle::is_closed() const {
  std::scoped_lock<std::mutex> guard(state_mutex);
  return closed;
}

fuse_ino_t DirectoryHandle::inode() const { return ino; }

void DirectoryHandle::reserve_cookies_through(uint64_t res) {
  std::scoped_lock<std::mutex> guard(cookie_mutex);
  assert(cookie_filenames.empty() && "reservation should be made before use");
  assert(filename_cookies.empty());

  reserved_cookies = res;
  if (res >= static_cast<uint64_t>(ReaddirStartKind::AfterDot)) {
    cookie_filenames.push_back(".");
    filename_cookies.emplace(cookie_filenames.back(), cookie_filenames.size());
  }
  if (res >= static_cast<uint64_t>(ReaddirStartKind::AfterDotDot)) {
    cookie_filenames.push_back("..");
    filename_cookies.emplace(cookie_filenames.back(), cookie_filenames.size());
  }
}

void DirectoryHandle::read_complete() {
  struct timespec now{};
  clock_gettime(CLOCK_REALTIME, &now);
  std::unique_lock<std::mutex> lk(state_mutex);
  if (closed) {
    return;
  }

  if (!first_read.has_value()) {
    first_read = now;
  }
  // in the unlikely event of thread shenanigans, check that we don't
  // move the latest read time backwards
  if (!latest_read.has_value() ||
      compare_timespec_value(now, *latest_read) > 0) {
    latest_read = now;
  }

  // avoid launching updates more than once per second
  // ideally the calling process finishes reading the entire directory
  // in under a second and this clause never triggers at all.
  const auto &threshold =
      last_update_attempt.has_value() ? *last_update_attempt : *first_read;
  if (more_than_one_second_since(threshold, now)) {
    launch_atime_update_locked(*latest_read, now, lk);
  }
}

uint64_t DirectoryHandle::filename_to_cookie(std::string_view name) {
  std::scoped_lock<std::mutex> guard(cookie_mutex);

  if (auto it = filename_cookies.find(std::string(name));
      it != filename_cookies.end()) {
    return it->second;
  }

  const uint64_t cookie = static_cast<uint64_t>(cookie_filenames.size()) + 1;
  cookie_filenames.emplace_back(name);
  filename_cookies.emplace(cookie_filenames.back(), cookie);
  return cookie;
}

std::optional<std::string_view>
DirectoryHandle::cookie_to_filename(uint64_t cookie) const {
  std::scoped_lock<std::mutex> guard(cookie_mutex);
  if (cookie <= reserved_cookies) {
    return std::nullopt;
  }

  const size_t index = static_cast<size_t>(cookie - 1);
  if (index >= cookie_filenames.size()) {
    return std::nullopt;
  }
  return std::string_view(cookie_filenames[index]);
}
