#include <catch2/catch_test_macros.hpp>

#include <string>

#include "test_support.h"

TEST_CASE("sqlite3 shell can load fdbfs and run a read-only query",
          "[sqlite3_integrated][basic][smoke]") {
  sqlite3_integrated_reset_database();

  Sqlite3CliProcess sqlite("smoke.db");
  auto result = sqlite.run("SELECT 1;");

  CHECK(result.stderr_text.empty());
  CHECK(result.stdout_text == "1\n");
}

TEST_CASE("sqlite3 shell can open fdbfs database and round-trip rows",
          "[sqlite3_integrated][basic]") {
  sqlite3_integrated_reset_database();

  Sqlite3CliProcess sqlite("basic.db");
  auto result = sqlite.run(
      "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);\n"
      "INSERT INTO t(value) VALUES ('alpha'), ('beta');\n"
      "SELECT id, value FROM t ORDER BY id;");

  CHECK(result.stderr_text.empty());
  CHECK(result.stdout_text == "1|alpha\n2|beta\n");
}

TEST_CASE("sqlite3 shell processes can share the same fdbfs database",
          "[sqlite3_integrated][basic][multiprocess]") {
  sqlite3_integrated_reset_database();

  Sqlite3CliProcess writer("shared.db");
  Sqlite3CliProcess reader("shared.db");

  auto writer_result = writer.run(
      "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);\n"
      "INSERT INTO t(id, value) VALUES (1, 'one');");
  CHECK(writer_result.stderr_text.empty());

  auto reader_result = reader.run(
      "INSERT INTO t(id, value) VALUES (2, 'two');\n"
      "SELECT id, value FROM t ORDER BY id;");

  CHECK(reader_result.stderr_text.empty());
  CHECK(reader_result.stdout_text == "1|one\n2|two\n");
}

TEST_CASE("sqlite3 shell processes can alternate inserts into one database",
          "[sqlite3_integrated][basic][multiprocess][writes]") {
  sqlite3_integrated_reset_database();

  Sqlite3CliProcess first("alternating.db");
  Sqlite3CliProcess second("alternating.db");

  auto create_result =
      first.run("CREATE TABLE t(id INTEGER PRIMARY KEY, source TEXT);");
  REQUIRE(create_result.stderr_text.empty());

  for (int i = 0; i < 100; ++i) {
    const int first_id = i * 2 + 1;
    const int second_id = i * 2 + 2;

    auto first_result =
        first.run("INSERT INTO t(id, source) VALUES (" +
                  std::to_string(first_id) + ", 'first');");
    REQUIRE(first_result.stderr_text.empty());

    auto second_result =
        second.run("INSERT INTO t(id, source) VALUES (" +
                   std::to_string(second_id) + ", 'second');");
    REQUIRE(second_result.stderr_text.empty());
  }

  auto verify_result = first.run(
      "SELECT COUNT(*), "
      "SUM(CASE WHEN source='first' THEN 1 ELSE 0 END), "
      "SUM(CASE WHEN source='second' THEN 1 ELSE 0 END) "
      "FROM t;\n"
      "SELECT id, source FROM t ORDER BY id;");

  REQUIRE(verify_result.stderr_text.empty());

  std::string expected = "200|100|100\n";
  for (int i = 0; i < 100; ++i) {
    expected += std::to_string(i * 2 + 1);
    expected += "|first\n";
    expected += std::to_string(i * 2 + 2);
    expected += "|second\n";
  }
  CHECK(verify_result.stdout_text == expected);
}

TEST_CASE("sqlite3 shell can scan many medium blobs and sum their lengths",
          "[sqlite3_integrated][basic][reads]") {
  sqlite3_integrated_reset_database();

  Sqlite3CliProcess sqlite("large-scan.db");
  auto result = sqlite.run(
      "CREATE TABLE t(data BLOB);\n"
      "WITH RECURSIVE cnt(x) AS ("
      "  VALUES(1)"
      "  UNION ALL"
      "  SELECT x + 1 FROM cnt WHERE x < 100"
      ")\n"
      "INSERT INTO t(data) SELECT randomblob(32768) FROM cnt;\n"
      "SELECT COUNT(*), SUM(length(data)) FROM t;");

  CHECK(result.stderr_text.empty());
  CHECK(result.stdout_text == "100|3276800\n");
}
