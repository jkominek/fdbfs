#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>

#include "test_support.h"

namespace {

void require_fcntl_lock(int fd, int cmd, short type, off_t start, off_t len,
                        short whence = SEEK_SET) {
  struct flock fl{};
  fl.l_type = type;
  fl.l_whence = whence;
  fl.l_start = start;
  fl.l_len = len;

  errno = 0;
  const int rc = ::fcntl(fd, cmd, &fl);
  const int saved_errno = errno;
  INFO("fcntl(cmd=" << cmd << ", type=" << type << ", whence=" << whence
                    << ", start=" << static_cast<long long>(start)
                    << ", len=" << static_cast<long long>(len) << ") rc=" << rc
                    << " errno=" << errno_with_message(saved_errno));
  REQUIRE(rc == 0);
}

struct flock require_fcntl_getlk(int fd, short type, off_t start, off_t len,
                                 short whence = SEEK_SET) {
  struct flock fl{};
  fl.l_type = type;
  fl.l_whence = whence;
  fl.l_start = start;
  fl.l_len = len;

  errno = 0;
  const int rc = ::fcntl(fd, F_GETLK, &fl);
  const int saved_errno = errno;
  INFO("fcntl(cmd=F_GETLK, type="
       << type << ", whence=" << whence
       << ", start=" << static_cast<long long>(start)
       << ", len=" << static_cast<long long>(len) << ") rc=" << rc << " errno="
       << errno_with_message(saved_errno) << " => out(type=" << fl.l_type
       << ", start=" << static_cast<long long>(fl.l_start) << ", len="
       << static_cast<long long>(fl.l_len) << ", pid=" << fl.l_pid << ")");
  REQUIRE(rc == 0);
  return fl;
}

int fcntl_lock_errno(int fd, int cmd, short type, off_t start, off_t len,
                     short whence = SEEK_SET) {
  struct flock fl{};
  fl.l_type = type;
  fl.l_whence = whence;
  fl.l_start = start;
  fl.l_len = len;
  errno = 0;
  if (::fcntl(fd, cmd, &fl) == 0) {
    return 0;
  }
  return errno;
}

std::array<int, 2> make_pipe() {
  std::array<int, 2> p{-1, -1};
  FDBFS_REQUIRE_OK(::pipe(p.data()));
  return p;
}

void write_byte(int fd, char c) { FDBFS_REQUIRE_NONNEG(::write(fd, &c, 1)); }

char read_byte(int fd) {
  char c = '\0';
  FDBFS_REQUIRE_NONNEG(::read(fd, &c, 1));
  return c;
}

bool poll_readable(int fd, int timeout_ms) {
  struct pollfd pfd{};
  pfd.fd = fd;
  pfd.events = POLLIN;
  const int rc = ::poll(&pfd, 1, timeout_ms);
  if (rc < 0) {
    INFO("poll errno=" << errno_with_message(errno));
    REQUIRE(rc >= 0);
  }
  return (rc > 0) && ((pfd.revents & POLLIN) != 0);
}

bool lock_failed_with_contention_errno(int err) {
  return err == EACCES || err == EAGAIN;
}

} // namespace

TEST_CASE("posix locks basic read lock and unlock succeed",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lockfile_read");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);

    require_fcntl_lock(fd, F_SETLK, F_RDLCK, 0, 16);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 0, 16);

    FDBFS_REQUIRE_OK(::close(fd));
  });
}

TEST_CASE("posix locks basic write lock and unlock succeed",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lockfile_write");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);

    // len=0 means lock to EOF in POSIX byte-range lock semantics.
    require_fcntl_lock(fd, F_SETLK, F_WRLCK, 0, 0);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 0, 0);

    FDBFS_REQUIRE_OK(::close(fd));
  });
}

TEST_CASE("posix locks blocking setlkw succeeds when uncontended",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lockfile_blocking");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);

    require_fcntl_lock(fd, F_SETLKW, F_WRLCK, 32, 8);
    require_fcntl_lock(fd, F_SETLKW, F_UNLCK, 32, 8);

    FDBFS_REQUIRE_OK(::close(fd));
  });
}

TEST_CASE("posix locks setlk/getlk basic roundtrip and no-lock behavior",
          "[integration][locks][getlk]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lockfile_getlk_roundtrip");

    int parent_fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(parent_fd);

    // No lock held yet: kernel should report unlock.
    {
      struct flock out = require_fcntl_getlk(parent_fd, F_WRLCK, 10, 20);
      CHECK(out.l_type == F_UNLCK);
    }

    int ready_pipe[2] = {-1, -1};
    int done_pipe[2] = {-1, -1};
    FDBFS_REQUIRE_OK(::pipe(ready_pipe));
    FDBFS_REQUIRE_OK(::pipe(done_pipe));

    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
      ::close(ready_pipe[0]);
      ::close(done_pipe[1]);

      int child_fd = ::open(p.c_str(), O_RDWR);
      if (child_fd < 0) {
        _exit(2);
      }

      struct flock lock{};
      lock.l_type = F_WRLCK;
      lock.l_whence = SEEK_SET;
      lock.l_start = 10;
      lock.l_len = 20;
      if (::fcntl(child_fd, F_SETLK, &lock) != 0) {
        _exit(3);
      }

      char ok = '1';
      if (::write(ready_pipe[1], &ok, 1) != 1) {
        _exit(4);
      }

      char done = '\0';
      if (::read(done_pipe[0], &done, 1) != 1) {
        _exit(5);
      }

      lock.l_type = F_UNLCK;
      (void)::fcntl(child_fd, F_SETLK, &lock);
      (void)::close(child_fd);
      _exit(0);
    }

    ::close(ready_pipe[1]);
    ::close(done_pipe[0]);

    char ready = '\0';
    REQUIRE(::read(ready_pipe[0], &ready, 1) == 1);
    CHECK(ready == '1');

    // Overlapping query should report child's write lock.
    {
      struct flock out = require_fcntl_getlk(parent_fd, F_WRLCK, 15, 4);
      CHECK(out.l_type == F_WRLCK);
      CHECK(out.l_start == 10);
      CHECK(out.l_len == 20);
      CHECK(out.l_pid == child);
    }

    REQUIRE(::write(done_pipe[1], "x", 1) == 1);

    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);

    ::close(ready_pipe[0]);
    ::close(done_pipe[1]);

    // Child released lock: no conflict again.
    {
      struct flock out = require_fcntl_getlk(parent_fd, F_WRLCK, 10, 20);
      CHECK(out.l_type == F_UNLCK);
    }

    FDBFS_REQUIRE_OK(::close(parent_fd));
  });
}

TEST_CASE("posix locks close releases lock without explicit unlock",
          "[integration][locks][close]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_close_releases");
    int parent_fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(parent_fd);
    require_fcntl_lock(parent_fd, F_SETLK, F_WRLCK, 0, 100);

    auto cmd = make_pipe();
    auto evt = make_pipe();
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
      ::close(cmd[1]);
      ::close(evt[0]);
      int fd = ::open(p.c_str(), O_RDWR);
      if (fd < 0) {
        _exit(2);
      }
      const int first = fcntl_lock_errno(fd, F_SETLK, F_WRLCK, 0, 100);
      if (first == 0) {
        _exit(3);
      }
      (void)::write(evt[1], "F", 1);
      char go = '\0';
      if (::read(cmd[0], &go, 1) != 1) {
        _exit(4);
      }
      const int second = fcntl_lock_errno(fd, F_SETLK, F_WRLCK, 0, 100);
      if (second != 0) {
        _exit(5);
      }
      (void)::write(evt[1], "S", 1);
      _exit(0);
    }

    ::close(cmd[0]);
    ::close(evt[1]);
    CHECK(read_byte(evt[0]) == 'F');
    FDBFS_REQUIRE_OK(::close(parent_fd));
    write_byte(cmd[1], 'x');
    CHECK(read_byte(evt[0]) == 'S');
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    ::close(cmd[1]);
    ::close(evt[0]);
  });
}

TEST_CASE("posix locks closing another fd on same inode drops process locks",
          "[integration][locks][close]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_close_otherfd");
    int fd1 = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    int fd2 = ::open(p.c_str(), O_RDWR);
    FDBFS_REQUIRE_NONNEG(fd1);
    FDBFS_REQUIRE_NONNEG(fd2);
    require_fcntl_lock(fd1, F_SETLK, F_WRLCK, 0, 100);

    auto cmd = make_pipe();
    auto evt = make_pipe();
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
      ::close(cmd[1]);
      ::close(evt[0]);
      int fd = ::open(p.c_str(), O_RDWR);
      if (fd < 0) {
        _exit(2);
      }
      const int first = fcntl_lock_errno(fd, F_SETLK, F_WRLCK, 0, 100);
      if (first == 0) {
        _exit(3);
      }
      (void)::write(evt[1], "F", 1);
      char go = '\0';
      if (::read(cmd[0], &go, 1) != 1) {
        _exit(4);
      }
      const int second = fcntl_lock_errno(fd, F_SETLK, F_WRLCK, 0, 100);
      if (second != 0) {
        _exit(5);
      }
      (void)::write(evt[1], "S", 1);
      _exit(0);
    }

    ::close(cmd[0]);
    ::close(evt[1]);
    CHECK(read_byte(evt[0]) == 'F');
    FDBFS_REQUIRE_OK(::close(fd2)); // Should drop process locks for this inode.
    write_byte(cmd[1], 'x');
    CHECK(read_byte(evt[0]) == 'S');
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    FDBFS_REQUIRE_OK(::close(fd1));
    ::close(cmd[1]);
    ::close(evt[0]);
  });
}

TEST_CASE("posix locks closing dup fd drops process locks on that inode",
          "[integration][locks][close]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_close_dupfd");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    int dupfd = ::dup(fd);
    FDBFS_REQUIRE_NONNEG(dupfd);
    require_fcntl_lock(fd, F_SETLK, F_WRLCK, 0, 100);

    auto cmd = make_pipe();
    auto evt = make_pipe();
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
      ::close(cmd[1]);
      ::close(evt[0]);
      int cfd = ::open(p.c_str(), O_RDWR);
      if (cfd < 0) {
        _exit(2);
      }
      const int first = fcntl_lock_errno(cfd, F_SETLK, F_WRLCK, 0, 100);
      if (first == 0) {
        _exit(3);
      }
      (void)::write(evt[1], "F", 1);
      char go = '\0';
      if (::read(cmd[0], &go, 1) != 1) {
        _exit(4);
      }
      const int second = fcntl_lock_errno(cfd, F_SETLK, F_WRLCK, 0, 100);
      if (second != 0) {
        _exit(5);
      }
      (void)::write(evt[1], "S", 1);
      _exit(0);
    }

    ::close(cmd[0]);
    ::close(evt[1]);
    CHECK(read_byte(evt[0]) == 'F');
    FDBFS_REQUIRE_OK(::close(dupfd));
    write_byte(cmd[1], 'x');
    CHECK(read_byte(evt[0]) == 'S');
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    FDBFS_REQUIRE_OK(::close(fd));
    ::close(cmd[1]);
    ::close(evt[0]);
  });
}

TEST_CASE("posix locks process exit releases held locks",
          "[integration][locks][exit]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_exit_releases");
    int parent_fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(parent_fd);

    auto cmd = make_pipe();
    auto evt = make_pipe();
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
      ::close(cmd[1]);
      ::close(evt[0]);
      int fd = ::open(p.c_str(), O_RDWR);
      if (fd < 0) {
        _exit(2);
      }
      const int err = fcntl_lock_errno(fd, F_SETLK, F_WRLCK, 0, 100);
      if (err != 0) {
        _exit(3);
      }
      (void)::write(evt[1], "R", 1);
      char go = '\0';
      if (::read(cmd[0], &go, 1) != 1) {
        _exit(4);
      }
      _exit(0); // intentionally no explicit unlock
    }

    ::close(cmd[0]);
    ::close(evt[1]);
    CHECK(read_byte(evt[0]) == 'R');

    const int while_live =
        fcntl_lock_errno(parent_fd, F_SETLK, F_WRLCK, 0, 100);
    INFO("while child alive lock errno=" << errno_with_message(while_live));
    CHECK(lock_failed_with_contention_errno(while_live));

    write_byte(cmd[1], 'x');
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);

    const int after_exit =
        fcntl_lock_errno(parent_fd, F_SETLK, F_WRLCK, 0, 100);
    INFO("after child exit lock errno=" << errno_with_message(after_exit));
    CHECK(after_exit == 0);
    require_fcntl_lock(parent_fd, F_SETLK, F_UNLCK, 0, 100);
    FDBFS_REQUIRE_OK(::close(parent_fd));
    ::close(cmd[1]);
    ::close(evt[0]);
  });
}

TEST_CASE(
    "posix locks overlapping readers block writer until all readers release",
    "[integration][locks][blocking]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_rw_blocking");
    int r1_fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(r1_fd);
    require_fcntl_lock(r1_fd, F_SETLK, F_RDLCK, 0, 100);

    auto r2_cmd = make_pipe();
    auto r2_evt = make_pipe();
    const pid_t r2 = ::fork();
    REQUIRE(r2 >= 0);
    if (r2 == 0) {
      ::close(r2_cmd[1]);
      ::close(r2_evt[0]);
      int fd = ::open(p.c_str(), O_RDWR);
      if (fd < 0) {
        _exit(2);
      }
      if (fcntl_lock_errno(fd, F_SETLK, F_RDLCK, 50, 100) != 0) {
        _exit(3);
      }
      (void)::write(r2_evt[1], "R", 1);
      char go = '\0';
      if (::read(r2_cmd[0], &go, 1) != 1) {
        _exit(4);
      }
      (void)fcntl_lock_errno(fd, F_SETLK, F_UNLCK, 50, 100);
      _exit(0);
    }
    ::close(r2_cmd[0]);
    ::close(r2_evt[1]);
    CHECK(read_byte(r2_evt[0]) == 'R');

    auto w_cmd = make_pipe();
    auto w_evt = make_pipe();
    const pid_t w = ::fork();
    REQUIRE(w >= 0);
    if (w == 0) {
      ::close(w_cmd[1]);
      ::close(w_evt[0]);
      int fd = ::open(p.c_str(), O_RDWR);
      if (fd < 0) {
        _exit(2);
      }
      (void)::write(w_evt[1], "B", 1); // about to block in F_SETLKW
      struct flock fl{};
      fl.l_type = F_WRLCK;
      fl.l_whence = SEEK_SET;
      fl.l_start = 0;
      fl.l_len = 75;
      if (::fcntl(fd, F_SETLKW, &fl) != 0) {
        _exit(3);
      }
      (void)::write(w_evt[1], "A", 1); // acquired
      char go = '\0';
      if (::read(w_cmd[0], &go, 1) != 1) {
        _exit(4);
      }
      (void)fcntl_lock_errno(fd, F_SETLK, F_UNLCK, 0, 75);
      _exit(0);
    }
    ::close(w_cmd[0]);
    ::close(w_evt[1]);
    CHECK(read_byte(w_evt[0]) == 'B');
    CHECK(!poll_readable(w_evt[0], 200));

    write_byte(r2_cmd[1], 'x'); // release reader 2 only
    int r2_status = 0;
    REQUIRE(::waitpid(r2, &r2_status, 0) == r2);
    CHECK(WIFEXITED(r2_status));
    CHECK(WEXITSTATUS(r2_status) == 0);
    CHECK(!poll_readable(w_evt[0], 200));

    require_fcntl_lock(r1_fd, F_SETLK, F_UNLCK, 0,
                       100); // now writer should get lock
    CHECK(poll_readable(w_evt[0], 2000));
    CHECK(read_byte(w_evt[0]) == 'A');
    write_byte(w_cmd[1], 'x');
    int w_status = 0;
    REQUIRE(::waitpid(w, &w_status, 0) == w);
    CHECK(WIFEXITED(w_status));
    CHECK(WEXITSTATUS(w_status) == 0);
    FDBFS_REQUIRE_OK(::close(r1_fd));
    ::close(r2_cmd[1]);
    ::close(r2_evt[0]);
    ::close(w_cmd[1]);
    ::close(w_evt[0]);
  });
}

TEST_CASE("posix locks non-overlapping ranges do not conflict",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_nonoverlap");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    require_fcntl_lock(fd, F_SETLK, F_WRLCK, 0, 100);
    const int err = fcntl_lock_errno(fd, F_SETLK, F_WRLCK, 100, 100);
    INFO("non-overlap second lock errno=" << errno_with_message(err));
    CHECK(err == 0);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 0, 0);
    FDBFS_REQUIRE_OK(::close(fd));
  });
}

TEST_CASE("posix locks partial unlock splits locked region",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_partial_unlock");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    require_fcntl_lock(fd, F_SETLK, F_WRLCK, 0, 100);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 40, 20);

    auto cmd = make_pipe();
    auto evt = make_pipe();
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
      ::close(cmd[1]);
      ::close(evt[0]);
      int cfd = ::open(p.c_str(), O_RDWR);
      if (cfd < 0) {
        _exit(2);
      }
      // middle hole should be lockable
      if (fcntl_lock_errno(cfd, F_SETLK, F_WRLCK, 45, 5) != 0) {
        _exit(3);
      }
      (void)fcntl_lock_errno(cfd, F_SETLK, F_UNLCK, 45, 5);
      // left fragment still blocked
      if (!lock_failed_with_contention_errno(
              fcntl_lock_errno(cfd, F_SETLK, F_WRLCK, 10, 5))) {
        _exit(4);
      }
      // right fragment still blocked
      if (!lock_failed_with_contention_errno(
              fcntl_lock_errno(cfd, F_SETLK, F_WRLCK, 70, 5))) {
        _exit(5);
      }
      (void)::write(evt[1], "D", 1);
      char go = '\0';
      (void)::read(cmd[0], &go, 1);
      _exit(0);
    }
    ::close(cmd[0]);
    ::close(evt[1]);
    CHECK(read_byte(evt[0]) == 'D');
    write_byte(cmd[1], 'x');
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 0, 0);
    FDBFS_REQUIRE_OK(::close(fd));
    ::close(cmd[1]);
    ::close(evt[0]);
  });
}

TEST_CASE("posix locks same-process upgrade and downgrade succeed",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_upgrade_downgrade");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    require_fcntl_lock(fd, F_SETLK, F_RDLCK, 0, 50);
    require_fcntl_lock(fd, F_SETLK, F_WRLCK, 0, 50);
    require_fcntl_lock(fd, F_SETLK, F_RDLCK, 0, 50);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 0, 50);
    FDBFS_REQUIRE_OK(::close(fd));
  });
}

TEST_CASE("posix locks len zero locks to EOF including grown file",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_to_eof");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    require_fcntl_lock(fd, F_SETLK, F_WRLCK, 10, 0);
    FDBFS_REQUIRE_OK(::ftruncate(fd, 8192));

    auto cmd = make_pipe();
    auto evt = make_pipe();
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
      ::close(cmd[1]);
      ::close(evt[0]);
      int cfd = ::open(p.c_str(), O_RDWR);
      if (cfd < 0) {
        _exit(2);
      }
      if (!lock_failed_with_contention_errno(
              fcntl_lock_errno(cfd, F_SETLK, F_WRLCK, 5000, 10))) {
        _exit(3);
      }
      (void)::write(evt[1], "D", 1);
      char go = '\0';
      (void)::read(cmd[0], &go, 1);
      _exit(0);
    }
    ::close(cmd[0]);
    ::close(evt[1]);
    CHECK(read_byte(evt[0]) == 'D');
    write_byte(cmd[1], 'x');
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 0, 0);
    FDBFS_REQUIRE_OK(::close(fd));
    ::close(cmd[1]);
    ::close(evt[0]);
  });
}

TEST_CASE("posix locks negative length ranges behave correctly",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_negative_len");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    require_fcntl_lock(fd, F_SETLK, F_WRLCK, 100, -20); // [80,99]

    auto cmd = make_pipe();
    auto evt = make_pipe();
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
      ::close(cmd[1]);
      ::close(evt[0]);
      int cfd = ::open(p.c_str(), O_RDWR);
      if (cfd < 0) {
        _exit(2);
      }
      if (!lock_failed_with_contention_errno(
              fcntl_lock_errno(cfd, F_SETLK, F_WRLCK, 90, 5))) {
        _exit(3);
      }
      if (fcntl_lock_errno(cfd, F_SETLK, F_WRLCK, 60, 10) != 0) {
        _exit(4);
      }
      (void)::write(evt[1], "D", 1);
      char go = '\0';
      (void)::read(cmd[0], &go, 1);
      _exit(0);
    }
    ::close(cmd[0]);
    ::close(evt[1]);
    CHECK(read_byte(evt[0]) == 'D');
    write_byte(cmd[1], 'x');
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 0, 0);
    FDBFS_REQUIRE_OK(::close(fd));
    ::close(cmd[1]);
    ::close(evt[0]);
  });
}

TEST_CASE("posix locks unlocking non-held ranges is harmless",
          "[integration][locks]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("lock_unlock_nonheld");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    require_fcntl_lock(fd, F_SETLK, F_UNLCK, 123, 17);
    FDBFS_REQUIRE_OK(::close(fd));
  });
}
