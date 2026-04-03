#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include <sqlite3.h>

namespace fs = std::filesystem;

sqlite3_vfs *sqlite3_direct_vfs();
void sqlite3_direct_reset_database();
void sqlite3_direct_shutdown();

struct Sqlite3DirectFile {
  explicit Sqlite3DirectFile(sqlite3_vfs *vfs);
  ~Sqlite3DirectFile();

  Sqlite3DirectFile(const Sqlite3DirectFile &) = delete;
  Sqlite3DirectFile &operator=(const Sqlite3DirectFile &) = delete;
  Sqlite3DirectFile(Sqlite3DirectFile &&other) noexcept;
  Sqlite3DirectFile &operator=(Sqlite3DirectFile &&other) noexcept;

  [[nodiscard]] sqlite3_file *file() {
    return reinterpret_cast<sqlite3_file *>(storage.data());
  }

  void close_if_open();

private:
  sqlite3_vfs *vfs_;
  std::vector<std::byte> storage;
};

std::string sqlite3_full_path(std::string_view path);
int sqlite3_access(sqlite3_vfs *vfs, std::string_view path, int flags);
int sqlite3_delete(sqlite3_vfs *vfs, std::string_view path);

Sqlite3DirectFile sqlite3_open_file(sqlite3_vfs *vfs, std::string_view path,
                                    int flags, int *out_flags = nullptr);
