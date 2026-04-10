#include "test_support.h"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

void collect_export_names(const nlohmann::json &node,
                          std::vector<std::string> &names) {
  if (node.is_object()) {
    for (const auto &[key, value] : node.items()) {
      if ((key == "export-name" || key == "name") && value.is_string()) {
        names.push_back(value.get<std::string>());
      } else {
        collect_export_names(value, names);
      }
    }
    return;
  }
  if (node.is_array()) {
    for (const auto &value : node) {
      collect_export_names(value, names);
    }
  }
}

std::vector<uint8_t> make_pattern(size_t size, uint8_t seed) {
  std::vector<uint8_t> bytes(size);
  for (size_t i = 0; i < size; ++i) {
    bytes[i] = static_cast<uint8_t>((seed + i * 17) % 251);
  }
  return bytes;
}

void write_local_file(const fs::path &path, const std::vector<uint8_t> &bytes) {
  std::ofstream out(path, std::ios::binary);
  REQUIRE(out.good());
  out.write(reinterpret_cast<const char *>(bytes.data()),
            static_cast<std::streamsize>(bytes.size()));
  REQUIRE(out.good());
}

std::vector<uint8_t> read_local_file(const fs::path &path) {
  std::ifstream in(path, std::ios::binary);
  REQUIRE(in.good());
  return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), {});
}

} // namespace

TEST_CASE("nbdkit plugin lists regular files as exports",
          "[nbdkit][integrated][list]") {
  if (!nbdkit_integrated_tools_available()) {
    SKIP("nbdkit/nbdinfo/nbdcopy not available");
  }

  auto &services = nbdkit_integrated_services();
  const std::string file_a = "nbd-export-a";
  const std::string file_b = "nbd-export-b";
  const std::string dir_name = "nbd-export-dir";

  (void)services.create_regular_file(file_a);
  (void)services.create_regular_file(file_b);
  (void)services.create_directory(dir_name);

  NbdkitProcess server(services.prefix());
  CommandResult list = server.nbdinfo({"--list", "--json", server.root_uri()});
  REQUIRE(WIFEXITED(list.status));
  REQUIRE(WEXITSTATUS(list.status) == 0);
  INFO("nbdinfo stdout:\n" << list.stdout_text);
  INFO("nbdinfo stderr:\n" << list.stderr_text);

  const auto parsed = nlohmann::json::parse(list.stdout_text);
  std::vector<std::string> export_names;
  collect_export_names(parsed, export_names);

  CHECK(std::find(export_names.begin(), export_names.end(), file_a) !=
        export_names.end());
  CHECK(std::find(export_names.begin(), export_names.end(), file_b) !=
        export_names.end());
  CHECK(std::find(export_names.begin(), export_names.end(), dir_name) ==
        export_names.end());
}

TEST_CASE("nbdkit plugin reads and writes export data with nbdcopy",
          "[nbdkit][integrated][copy]") {
  if (!nbdkit_integrated_tools_available()) {
    SKIP("nbdkit/nbdinfo/nbdcopy not available");
  }

  auto &services = nbdkit_integrated_services();
  const std::string export_name = "nbd-roundtrip";
  auto inode = services.create_regular_file(export_name);

  const size_t file_size = BLOCKSIZE * 3;
  auto initial = make_pattern(file_size, 23);
  services.write_file(inode.inode(), initial);

  NbdkitProcess server(services.prefix());

  CommandResult size_info = server.nbdinfo({"--size", server.export_uri(export_name)});
  REQUIRE(WIFEXITED(size_info.status));
  REQUIRE(WEXITSTATUS(size_info.status) == 0);
  REQUIRE(std::stoull(size_info.stdout_text) == file_size);

  const fs::path temp_dir =
      fs::temp_directory_path() / ("fdbfs-nbdcopy-" + export_name);
  fs::create_directories(temp_dir);
  const fs::path readback_a = temp_dir / "readback-a.bin";
  const fs::path write_src = temp_dir / "write-src.bin";
  const fs::path readback_b = temp_dir / "readback-b.bin";

  CommandResult read_first =
      server.nbdcopy({server.export_uri(export_name), readback_a.string()});
  REQUIRE(WIFEXITED(read_first.status));
  REQUIRE(WEXITSTATUS(read_first.status) == 0);
  CHECK(read_local_file(readback_a) == initial);

  auto replacement = make_pattern(file_size, 91);
  write_local_file(write_src, replacement);
  CommandResult write_second =
      server.nbdcopy({write_src.string(), server.export_uri(export_name)});
  REQUIRE(WIFEXITED(write_second.status));
  REQUIRE(WEXITSTATUS(write_second.status) == 0);

  CommandResult read_second =
      server.nbdcopy({server.export_uri(export_name), readback_b.string()});
  REQUIRE(WIFEXITED(read_second.status));
  REQUIRE(WEXITSTATUS(read_second.status) == 0);
  CHECK(read_local_file(readback_b) == replacement);
}

TEST_CASE("nbdkit plugin reports expected capabilities",
          "[nbdkit][integrated][capabilities]") {
  if (!nbdkit_integrated_tools_available()) {
    SKIP("nbdkit/nbdinfo/nbdcopy not available");
  }

  auto &services = nbdkit_integrated_services();
  const std::string export_name = "nbd-capabilities";
  auto inode = services.create_regular_file(export_name);
  services.write_file(inode.inode(), make_pattern(BLOCKSIZE, 7));

  NbdkitProcess server(services.prefix());
  const std::string uri = server.export_uri(export_name);

  auto require_nbdinfo_ok = [&](std::vector<std::string> args) {
    CommandResult result = server.nbdinfo(std::move(args));
    INFO("stdout:\n" << result.stdout_text);
    INFO("stderr:\n" << result.stderr_text);
    REQUIRE(WIFEXITED(result.status));
    REQUIRE(WEXITSTATUS(result.status) == 0);
  };

  require_nbdinfo_ok({"--can", "write", uri});
  require_nbdinfo_ok({"--can", "flush", uri});
  require_nbdinfo_ok({"--can", "trim", uri});
  require_nbdinfo_ok({"--can", "zero", uri});
  require_nbdinfo_ok({"--can", "fua", uri});
  require_nbdinfo_ok({"--can", "multi-conn", uri});
  require_nbdinfo_ok({"--isnt", "read-only", uri});
  require_nbdinfo_ok({"--isnt", "rotational", uri});
}
