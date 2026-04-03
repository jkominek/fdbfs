#include "test_support.h"

#include <catch2/catch_test_macros.hpp>

#include <dlfcn.h>
#include <sqlite3ext.h>
#undef sqlite3_vfs_register
#undef sqlite3_vfs_unregister

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>

namespace {

sqlite3_vfs *g_registered_vfs = nullptr;
sqlite3_api_routines g_fake_sqlite3_api = {};
void *g_sqlite3_lib_handle = nullptr;

fs::path required_env_path(const char *name) {
  const char *v = std::getenv(name);
  if ((v == nullptr) || (v[0] == '\0')) {
    throw std::runtime_error(std::string("missing required env var: ") + name);
  }
  return fs::path(v);
}

std::string shell_quote(std::string_view s) {
  std::string out;
  out.reserve(s.size() + 2);
  out.push_back('\'');
  for (char c : s) {
    if (c == '\'') {
      out += "'\\''";
    } else {
      out.push_back(c);
    }
  }
  out.push_back('\'');
  return out;
}

using Sqlite3ExtensionInitT = int (*)(sqlite3 *, char **,
                                      const sqlite3_api_routines *);

std::string sqlite3_direct_prefix() {
  static const std::string prefix = []() {
    const auto now =
        std::chrono::steady_clock::now().time_since_epoch().count();
    return "sql" + std::to_string(getpid()) + "-" + std::to_string(now);
  }();
  return prefix;
}

void ensure_sqlite3_direct_database_ready() {
  static bool initialized = false;
  if (initialized) {
    return;
  }

  const std::string prefix = sqlite3_direct_prefix();
  REQUIRE(::setenv("FDBFS_FS_PREFIX", prefix.c_str(), 1) == 0);

  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");
  const std::string cmd = "cd " + shell_quote(source_dir.string()) +
                          " && ./gen.py " + shell_quote(prefix) + " | fdbcli";
  REQUIRE(std::system(cmd.c_str()) == 0);

  initialized = true;
}

} // namespace

extern "C" int sqlite3_vfs_register(sqlite3_vfs *vfs, int) {
  g_registered_vfs = vfs;
  return SQLITE_OK;
}

extern "C" int sqlite3_vfs_unregister(sqlite3_vfs *vfs) {
  if (g_registered_vfs == vfs) {
    g_registered_vfs = nullptr;
  }
  return SQLITE_OK;
}

sqlite3_vfs *sqlite3_direct_vfs() {
  static bool initialized = false;
  ensure_sqlite3_direct_database_ready();
  if (!initialized) {
    g_fake_sqlite3_api.vfs_register = sqlite3_vfs_register;
    g_fake_sqlite3_api.vfs_unregister = sqlite3_vfs_unregister;

    const fs::path lib_path = required_env_path("FDBFS_SQLITE3_LIB");
    g_sqlite3_lib_handle = dlopen(lib_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    REQUIRE(g_sqlite3_lib_handle != nullptr);

    void *sym = dlsym(g_sqlite3_lib_handle, "sqlite3_extension_init");
    REQUIRE(sym != nullptr);

    auto init = reinterpret_cast<Sqlite3ExtensionInitT>(sym);
    REQUIRE(init != nullptr);
    REQUIRE(init(nullptr, nullptr, &g_fake_sqlite3_api) ==
            SQLITE_OK_LOAD_PERMANENTLY);
    REQUIRE(g_registered_vfs != nullptr);
    initialized = true;
  }
  return g_registered_vfs;
}

void sqlite3_direct_reset_database() { ensure_sqlite3_direct_database_ready(); }

void sqlite3_direct_shutdown() {
  if (g_sqlite3_lib_handle != nullptr) {
    (void)dlclose(g_sqlite3_lib_handle);
    g_sqlite3_lib_handle = nullptr;
  }
  g_registered_vfs = nullptr;
}

Sqlite3DirectFile::Sqlite3DirectFile(sqlite3_vfs *vfs)
    : vfs_(vfs), storage(static_cast<size_t>(vfs->szOsFile)) {}

Sqlite3DirectFile::~Sqlite3DirectFile() { close_if_open(); }

Sqlite3DirectFile::Sqlite3DirectFile(Sqlite3DirectFile &&other) noexcept
    : vfs_(other.vfs_), storage(std::move(other.storage)) {
  other.vfs_ = nullptr;
}

Sqlite3DirectFile &
Sqlite3DirectFile::operator=(Sqlite3DirectFile &&other) noexcept {
  if (this != &other) {
    close_if_open();
    vfs_ = other.vfs_;
    storage = std::move(other.storage);
    other.vfs_ = nullptr;
  }
  return *this;
}

void Sqlite3DirectFile::close_if_open() {
  sqlite3_file *f = file();
  if ((f != nullptr) && (f->pMethods != nullptr)) {
    (void)f->pMethods->xClose(f);
    f->pMethods = nullptr;
  }
}

std::string sqlite3_full_path(std::string_view path) {
  sqlite3_vfs *vfs = sqlite3_direct_vfs();
  std::vector<char> out(static_cast<size_t>(vfs->mxPathname), '\0');
  REQUIRE(vfs->xFullPathname(vfs, std::string(path).c_str(),
                             static_cast<int>(out.size()),
                             out.data()) == SQLITE_OK);
  return std::string(out.data());
}

int sqlite3_access(sqlite3_vfs *vfs, std::string_view path, int flags) {
  int result = -1;
  REQUIRE(vfs->xAccess(vfs, std::string(path).c_str(), flags, &result) ==
          SQLITE_OK);
  return result;
}

int sqlite3_delete(sqlite3_vfs *vfs, std::string_view path) {
  return vfs->xDelete(vfs, std::string(path).c_str(), 0);
}

std::string sqlite3_last_error(sqlite3_vfs *vfs) {
  char buf[128] = {};
  CHECK(vfs->xGetLastError(vfs, sizeof(buf), buf) == 0);
  return std::string(buf);
}

Sqlite3DirectFile sqlite3_open_file(sqlite3_vfs *vfs, std::string_view path,
                                    int flags, int *out_flags) {
  Sqlite3DirectFile holder(vfs);
  REQUIRE(vfs->xOpen(vfs, std::string(path).c_str(), holder.file(), flags,
                     out_flags) == SQLITE_OK);
  REQUIRE(holder.file()->pMethods != nullptr);
  return holder;
}
