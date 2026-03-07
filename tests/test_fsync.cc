#include <catch2/catch_test_macros.hpp>

#include <aio.h>
#include <fcntl.h>
#include <unistd.h>

#include <random>
#include <string>
#include <vector>

#include "test_posix_helpers.h"
#include "test_support.h"

namespace {
void reset_aiocb(struct aiocb &cb) { memset(&cb, 0, sizeof(cb)); }

} // namespace

TEST_CASE("aio_fsync does not complete before prior aio_writes",
          "[integration][fsync][write][aio]") {
  scenario([&](FdbfsEnv &env) {
    // this test makes an attempt to observe the fsync contract
    // being violated, which is really tricky. the idea is that we
    // issue a whole bunch of asynchronous writes, and then a async
    // fsync. if we ever observe the fsync being done before one
    // of the writes, then we know there's a bug in fsync.
    //
    // if we _don't_ observe that, then we know nothing.
    const fs::path p = env.p("fsync-ordering.bin");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);

    constexpr int kWrites = 48;
    constexpr size_t kWriteSize = 128 * 1024;
    std::mt19937_64 rng(0xabc1234567ULL);
    std::vector<std::vector<uint8_t>> payloads;
    payloads.reserve(kWrites);
    for (int i = 0; i < kWrites; ++i) {
      payloads.emplace_back(generate_bytes(rng, kWriteSize));
    }

    std::vector<struct aiocb> write_cbs(kWrites);
    std::vector<bool> write_done(kWrites, false);
    int pending_writes = kWrites;

    // first prep all of the aio calls
    for (int i = 0; i < kWrites; ++i) {
      reset_aiocb(write_cbs[i]);
      write_cbs[i].aio_fildes = fd;
      write_cbs[i].aio_buf = payloads[i].data();
      write_cbs[i].aio_nbytes = payloads[i].size();
      // Ensure every write requires a read/write cycle of the first and
      // last blocks, so that they'll all conflict with one another, and
      // take longer to resolve.
      write_cbs[i].aio_offset = static_cast<off_t>(1 + i);
    }
    struct aiocb fsync_cb;
    reset_aiocb(fsync_cb);
    fsync_cb.aio_fildes = fd;

    // and then execute all of the aio calls as fast as we can
    for (int i = 0; i < kWrites; ++i) {
      errno = 0;
      const int rc = ::aio_write(&write_cbs[i]);
      INFO("aio_write[" << i << "] rc=" << rc
                        << " errno=" << errno_with_message(errno));
      REQUIRE(rc == 0);
    }
    errno = 0;
    const int fsync_aio_rc = ::aio_fsync(O_SYNC, &fsync_cb);
    INFO("aio_fsync rc=" << fsync_aio_rc
                         << " errno=" << errno_with_message(errno));
    REQUIRE(fsync_aio_rc == 0);

    // now monitor the aio calls

    bool fsync_done = false;
    bool fsync_return_consumed = false;

    while (!fsync_done || pending_writes > 0) {
      std::vector<const struct aiocb *> wait_list;
      wait_list.reserve(static_cast<size_t>(pending_writes) + 1);
      for (int i = 0; i < kWrites; ++i) {
        if (!write_done[i]) {
          wait_list.push_back(&write_cbs[i]);
        }
      }
      if (!fsync_done) {
        wait_list.push_back(&fsync_cb);
      }

      REQUIRE_FALSE(wait_list.empty());
      const int suspend_rc =
          ::aio_suspend(wait_list.data(), wait_list.size(), nullptr);
      INFO("aio_suspend rc=" << suspend_rc
                             << " errno=" << errno_with_message(errno));
      REQUIRE(suspend_rc == 0);

      for (int i = 0; i < kWrites; ++i) {
        if (write_done[i]) {
          continue;
        }
        errno = 0;
        const int werr = ::aio_error(&write_cbs[i]);
        if (werr == EINPROGRESS) {
          continue;
        }
        INFO("aio_error(write)[" << i << "]=" << errno_with_message(werr));
        REQUIRE(werr == 0);
        const ssize_t wret = ::aio_return(&write_cbs[i]);
        INFO("aio_return(write)[" << i << "]=" << wret);
        REQUIRE(wret == static_cast<ssize_t>(kWriteSize));
        write_done[i] = true;
        pending_writes--;
      }

      if (!fsync_done) {
        errno = 0;
        const int ferr = ::aio_error(&fsync_cb);
        if (ferr != EINPROGRESS) {
          INFO("aio_error(fsync)=" << errno_with_message(ferr));
          REQUIRE(ferr == 0);
          fsync_done = true;
          if (!fsync_return_consumed) {
            const int fret = ::aio_return(&fsync_cb);
            INFO("aio_return(fsync)=" << fret);
            REQUIRE(fret == 0);
            fsync_return_consumed = true;
          }
          if (pending_writes > 0) {
            FAIL("aio_fsync completed while earlier aio_write operations were "
                 "still pending");
          }
        }
      }
    }

    FDBFS_REQUIRE_OK(::close(fd));
  });
}
