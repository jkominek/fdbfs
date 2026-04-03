#pragma once

#include <string>
#include <string_view>

class Sqlite3CliProcess {
public:
  explicit Sqlite3CliProcess(std::string db_name);
  ~Sqlite3CliProcess();

  Sqlite3CliProcess(const Sqlite3CliProcess &) = delete;
  Sqlite3CliProcess &operator=(const Sqlite3CliProcess &) = delete;

  struct CommandResult {
    std::string stdout_text;
    std::string stderr_text;
  };

  [[nodiscard]] CommandResult run(std::string_view script);

private:
  int stdin_fd_ = -1;
  int stdout_fd_ = -1;
  int stderr_fd_ = -1;
  void *stdout_file_ = nullptr;
  int token_counter_ = 0;
  int pid_ = -1;
  std::string stderr_buffer_;
};

void sqlite3_integrated_reset_database();
