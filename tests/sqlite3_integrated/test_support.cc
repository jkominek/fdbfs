#include "test_support.h"

#include <catch2/catch_test_macros.hpp>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

namespace {

fs::path required_env_path(const char *name) {
  const char *v = std::getenv(name);
  if ((v == nullptr) || (v[0] == '\0')) {
    throw std::runtime_error(std::string("missing required env var: ") + name);
  }
  return fs::path(v);
}

std::string required_env_string(const char *name) {
  return required_env_path(name).string();
}

std::string sqlite3_integrated_prefix() {
  static const std::string prefix = []() {
    return "sqli" + std::to_string(getpid());
  }();
  return prefix;
}

void ensure_database_ready_once() {
  static bool initialized = false;
  if (initialized) {
    return;
  }

  const std::string prefix = sqlite3_integrated_prefix();
  REQUIRE(::setenv("FDBFS_FS_PREFIX", prefix.c_str(), 1) == 0);

  const fs::path mkfs_exe = required_env_path("FDBFS_MKFS_EXE");
  const std::string cmd = mkfs_exe.string() + " --force " + prefix;
  REQUIRE(std::system(cmd.c_str()) == 0);

  initialized = true;
}

std::FILE *fdopen_checked(int fd, const char *mode) {
  std::FILE *f = ::fdopen(fd, mode);
  if (f == nullptr) {
    throw std::runtime_error(std::string("fdopen failed: ") +
                             std::strerror(errno));
  }
  return f;
}

void write_all(int fd, std::string_view s) {
  const char *p = s.data();
  size_t remaining = s.size();
  while (remaining > 0) {
    const ssize_t rc = ::write(fd, p, remaining);
    if (rc < 0 && errno == EINTR) {
      continue;
    }
    if (rc <= 0) {
      throw std::runtime_error(std::string("write failed: ") +
                               std::strerror(errno));
    }
    p += rc;
    remaining -= static_cast<size_t>(rc);
  }
}

std::string slurp_nonblocking_fd(int fd) {
  std::string out;
  char buf[512];
  for (;;) {
    const ssize_t rc = ::read(fd, buf, sizeof(buf));
    if (rc > 0) {
      out.append(buf, static_cast<size_t>(rc));
      continue;
    }
    if (rc == 0) {
      break;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }
    throw std::runtime_error(std::string("read failed: ") +
                             std::strerror(errno));
  }
  return out;
}

void make_nonblocking(int fd) {
  const int old_flags = ::fcntl(fd, F_GETFL);
  if (old_flags < 0) {
    throw std::runtime_error(std::string("fcntl(F_GETFL) failed: ") +
                             std::strerror(errno));
  }
  if (::fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) < 0) {
    throw std::runtime_error(std::string("fcntl(F_SETFL) failed: ") +
                             std::strerror(errno));
  }
}

} // namespace

void sqlite3_integrated_reset_database() { ensure_database_ready_once(); }

Sqlite3CliProcess::Sqlite3CliProcess(std::string db_name) {
  ensure_database_ready_once();

  int stdin_pipe[2];
  int stdout_pipe[2];
  int stderr_pipe[2];
  REQUIRE(::pipe(stdin_pipe) == 0);
  REQUIRE(::pipe(stdout_pipe) == 0);
  REQUIRE(::pipe(stderr_pipe) == 0);

  pid_ = ::fork();
  REQUIRE(pid_ >= 0);

  if (pid_ == 0) {
    ::close(stdin_pipe[1]);
    ::close(stdout_pipe[0]);
    ::close(stderr_pipe[0]);

    if (::dup2(stdin_pipe[0], STDIN_FILENO) < 0) {
      _exit(126);
    }
    if (::dup2(stdout_pipe[1], STDOUT_FILENO) < 0) {
      _exit(126);
    }
    if (::dup2(stderr_pipe[1], STDERR_FILENO) < 0) {
      _exit(126);
    }

    ::close(stdin_pipe[0]);
    ::close(stdout_pipe[1]);
    ::close(stderr_pipe[1]);

    const std::string sqlite3_bin = required_env_string("FDBFS_SQLITE3_BIN");
    ::execlp(sqlite3_bin.c_str(), sqlite3_bin.c_str(), nullptr);
    _exit(127);
  }

  ::close(stdin_pipe[0]);
  ::close(stdout_pipe[1]);
  ::close(stderr_pipe[1]);

  stdin_fd_ = stdin_pipe[1];
  stdout_fd_ = stdout_pipe[0];
  stderr_fd_ = stderr_pipe[0];
  make_nonblocking(stderr_fd_);
  stdout_file_ = fdopen_checked(stdout_fd_, "r");

  const std::string setup =
      ".bail off\n"
      ".echo off\n"
      ".headers off\n"
      ".mode list\n"
      ".timeout 5000\n"
      ".load " + required_env_string("FDBFS_SQLITE3_LIB") + "\n" +
      ".open file:" + db_name + "?vfs=fdbfs\n";
  CommandResult result = run(setup);
  REQUIRE(result.stderr_text.empty());
}

Sqlite3CliProcess::~Sqlite3CliProcess() {
  if (stdin_fd_ >= 0) {
    (void)::close(stdin_fd_);
    stdin_fd_ = -1;
  }
  if (stdout_file_ != nullptr) {
    (void)::fclose(reinterpret_cast<std::FILE *>(stdout_file_));
    stdout_file_ = nullptr;
    stdout_fd_ = -1;
  } else if (stdout_fd_ >= 0) {
    (void)::close(stdout_fd_);
    stdout_fd_ = -1;
  }
  if (stderr_fd_ >= 0) {
    stderr_buffer_ += slurp_nonblocking_fd(stderr_fd_);
    (void)::close(stderr_fd_);
    stderr_fd_ = -1;
  }
  if (pid_ > 0) {
    int status = 0;
    (void)::waitpid(pid_, &status, 0);
    pid_ = -1;
  }
}

Sqlite3CliProcess::CommandResult Sqlite3CliProcess::run(std::string_view script) {
  const std::string token =
      "__FDBFS_SQLITE3_END_" + std::to_string(++token_counter_) + "__";

  std::string payload(script);
  if (payload.empty() || payload.back() != '\n') {
    payload.push_back('\n');
  }
  payload += ".print ";
  payload += token;
  payload += "\n";

  write_all(stdin_fd_, payload);

  std::string stdout_text;
  char *line = nullptr;
  size_t cap = 0;
  std::FILE *out = reinterpret_cast<std::FILE *>(stdout_file_);
  for (;;) {
    errno = 0;
    const ssize_t got = ::getline(&line, &cap, out);
    if (got < 0) {
      const std::string stderr_text = stderr_buffer_ + slurp_nonblocking_fd(stderr_fd_);
      free(line);
      throw std::runtime_error("getline failed before sentinel; stderr: " +
                               stderr_text);
    }
    std::string_view sv(line, static_cast<size_t>(got));
    if (!sv.empty() && sv.back() == '\n') {
      sv.remove_suffix(1);
    }
    if (sv == token) {
      break;
    }
    stdout_text.append(line, static_cast<size_t>(got));
  }
  free(line);

  stderr_buffer_ += slurp_nonblocking_fd(stderr_fd_);
  CommandResult result{
      .stdout_text = std::move(stdout_text),
      .stderr_text = std::move(stderr_buffer_),
  };
  stderr_buffer_.clear();
  return result;
}
