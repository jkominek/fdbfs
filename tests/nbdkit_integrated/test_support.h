#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "generic/util.h"

class NbdkitIntegratedServices {
public:
  NbdkitIntegratedServices();
  ~NbdkitIntegratedServices();

  NbdkitIntegratedServices(const NbdkitIntegratedServices &) = delete;
  NbdkitIntegratedServices &
  operator=(const NbdkitIntegratedServices &) = delete;

  [[nodiscard]] const std::string &prefix() const;
  [[nodiscard]] unique_transaction make_transaction() const;
  [[nodiscard]] INodeRecord create_regular_file(std::string_view name);
  [[nodiscard]] INodeRecord create_directory(std::string_view name);
  void write_file(fdbfs_ino_t ino, std::vector<uint8_t> bytes, off_t off = 0);

private:
  class Impl;
  Impl *impl_;
};

NbdkitIntegratedServices &nbdkit_integrated_services();

bool nbdkit_integrated_tools_available();

struct CommandResult {
  int status = -1;
  std::string stdout_text;
  std::string stderr_text;
};

class NbdkitProcess {
public:
  explicit NbdkitProcess(std::string prefix);
  ~NbdkitProcess();

  NbdkitProcess(const NbdkitProcess &) = delete;
  NbdkitProcess &operator=(const NbdkitProcess &) = delete;

  [[nodiscard]] std::string root_uri() const;
  [[nodiscard]] std::string export_uri(std::string_view export_name) const;

  [[nodiscard]] CommandResult nbdinfo(std::vector<std::string> args) const;
  [[nodiscard]] CommandResult nbdcopy(std::vector<std::string> args) const;

private:
  std::filesystem::path temp_dir_;
  std::filesystem::path stdout_path_;
  std::filesystem::path stderr_path_;
  uint16_t port_ = 0;
  pid_t pid_ = -1;
};
