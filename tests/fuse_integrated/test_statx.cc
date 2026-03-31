#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#define FUSE_USE_VERSION 35
#include <fuse3/fuse_lowlevel.h>

#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <ctime>

#include "test_support.h"

TEST_CASE("statx reports birth time for regular files",
          "[integration][statx][getattr]") {
  scenario([&](FdbfsEnv &env) {
#if FUSE_VERSION < FUSE_MAKE_VERSION(3, 18)
    SKIP("statx is only available with libfuse 3.18+");
    return;
#endif
    if (is_host_backend()) {
      SKIP("host backend does not exercise fdbfs_statx");
    }

    const fs::path p = env.p("statx_btime_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    struct statx stx{};
    errno = 0;
    FDBFS_REQUIRE_OK(
        ::statx(AT_FDCWD, p.c_str(), AT_STATX_SYNC_AS_STAT, STATX_BTIME, &stx));

    CHECK((stx.stx_mask & STATX_BTIME) != 0);
    CHECK(stx.stx_btime.tv_sec >= 1262304000);
  });
}
