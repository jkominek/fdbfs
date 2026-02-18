#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "test_support.h"

namespace {

std::vector<std::string> parse_listxattr_result(const std::vector<char> &buf) {
  std::vector<std::string> out;
  size_t i = 0;
  while (i < buf.size()) {
    const char *p = buf.data() + i;
    const size_t len = std::strlen(p);
    if (len == 0) {
      break;
    }
    out.emplace_back(p, p + len);
    i += len + 1;
  }
  return out;
}

bool contains_name(const std::vector<std::string> &names,
                   const std::string &needle) {
  for (const auto &name : names) {
    if (name == needle) {
      return true;
    }
  }
  return false;
}

} // namespace

TEST_CASE("setxattr/getxattr/listxattr/removexattr lifecycle",
          "[integration][setxattr][getxattr][listxattr][removexattr]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("xfile");
    const std::string xname = "user.integration";
    const std::string val1 = "value-one";
    const std::string val2 = "value-two-extended";

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    FDBFS_REQUIRE_OK(::setxattr(p.c_str(), xname.c_str(), val1.data(),
                                val1.size(), XATTR_CREATE));

    const ssize_t size_only = ::getxattr(p.c_str(), xname.c_str(), nullptr, 0);
    REQUIRE(size_only >= 0);
    CHECK(size_only == static_cast<ssize_t>(val1.size()));

    std::vector<char> got1(val1.size(), '\0');
    REQUIRE(::getxattr(p.c_str(), xname.c_str(), got1.data(), got1.size()) ==
            static_cast<ssize_t>(got1.size()));
    CHECK(std::string(got1.begin(), got1.end()) == val1);

    errno = 0;
    std::vector<char> tiny(2, '\0');
    CHECK(::getxattr(p.c_str(), xname.c_str(), tiny.data(), tiny.size()) == -1);
    FDBFS_CHECK_ERRNO(ERANGE);

    errno = 0;
    CHECK(::setxattr(p.c_str(), xname.c_str(), val2.data(), val2.size(),
                     XATTR_CREATE) == -1);
    FDBFS_CHECK_ERRNO(EEXIST);

    FDBFS_REQUIRE_OK(::setxattr(p.c_str(), xname.c_str(), val2.data(),
                                val2.size(), XATTR_REPLACE));

    std::vector<char> got2(val2.size(), '\0');
    REQUIRE(::getxattr(p.c_str(), xname.c_str(), got2.data(), got2.size()) ==
            static_cast<ssize_t>(got2.size()));
    CHECK(std::string(got2.begin(), got2.end()) == val2);

    const ssize_t list_size = ::listxattr(p.c_str(), nullptr, 0);
    REQUIRE(list_size > 0);

    errno = 0;
    std::vector<char> tiny_list(static_cast<size_t>(list_size) - 1, '\0');
    CHECK(::listxattr(p.c_str(), tiny_list.data(), tiny_list.size()) == -1);
    FDBFS_CHECK_ERRNO(ERANGE);

    std::vector<char> names_buf(static_cast<size_t>(list_size), '\0');
    REQUIRE(::listxattr(p.c_str(), names_buf.data(), names_buf.size()) ==
            list_size);
    const auto names = parse_listxattr_result(names_buf);
    CHECK(contains_name(names, xname));

    errno = 0;
    CHECK(::setxattr(p.c_str(), "user.missing", val1.data(), val1.size(),
                     XATTR_REPLACE) == -1);
    FDBFS_CHECK_ERRNO(ENODATA);

    FDBFS_REQUIRE_OK(::removexattr(p.c_str(), xname.c_str()));
    errno = 0;
    CHECK(::getxattr(p.c_str(), xname.c_str(), got2.data(), got2.size()) == -1);
    FDBFS_CHECK_ERRNO(ENODATA);

    errno = 0;
    CHECK(::removexattr(p.c_str(), xname.c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENODATA);
  });
}

TEST_CASE("xattr enforces name length limit", "[integration][xattr]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("xlong");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    const std::string max_name(255, 'n');
    const std::string too_long_name(256, 'n');
    const std::string payload = "v";

    FDBFS_REQUIRE_OK(::setxattr(p.c_str(), max_name.c_str(), payload.data(),
                                payload.size(), 0));

    errno = 0;
    CHECK(::setxattr(p.c_str(), too_long_name.c_str(), payload.data(),
                     payload.size(), 0) == -1);
    INFO("errno=" << errno_with_message(errno));
    CHECK((errno == ENAMETOOLONG || errno == ERANGE));
  });
}
