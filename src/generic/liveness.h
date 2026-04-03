#ifndef _LIVENESS_H_
#define _LIVENESS_H_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "values.pb.h"

enum class PidLiveness { Alive, Dead, Unknown };

struct ObservedPidState {
  std::optional<ProcessTableEntry> entry;
  std::optional<uint64_t> max_counter_seen;
  std::optional<struct timespec> max_counter_increase_boottime;
  uint64_t seen_in_scan_epoch = 0;
  std::optional<struct timespec> missing_since_boottime;
  bool malformed_entry = false;
};

struct RawPidRecord {
  std::vector<uint8_t> key;
  std::vector<uint8_t> value;
};

struct StalePidCandidate {
  std::vector<uint8_t> observed_pid;
  uint64_t expected_counter;
};

struct PidScanResult {
  std::vector<RawPidRecord> records;
  bool complete = false;
  struct timespec scan_boottime = {};
};

class LivenessService {
public:
  explicit LivenessService(std::function<void()> shutdown_cb);
  ~LivenessService();

  LivenessService(const LivenessService &) = delete;
  LivenessService &operator=(const LivenessService &) = delete;
  LivenessService(LivenessService &&) = delete;
  LivenessService &operator=(LivenessService &&) = delete;

  [[nodiscard]] const std::vector<uint8_t> &current_pid() const;
  [[nodiscard]] PidLiveness
  classify_pid(const std::vector<uint8_t> &candidate_pid);

private:
  [[nodiscard]] PidScanResult scan_pid_table();
  void merge_pid_table_scan(const std::vector<RawPidRecord> &records,
                            bool complete_scan,
                            const struct timespec &scan_boottime);
  void refresh_observed_pid_table();
  [[nodiscard]] bool
  reap_pid_if_still_stale(const std::vector<uint8_t> &observed_pid,
                          uint64_t expected_counter);
  void reap_stale_pid_entries();
  void request_shutdown_due_to_liveness_failure();
  bool send_pt_entry(bool startup);
  void update_pt_entry_time();
  void liveness_manager(std::stop_token stop_token);

  std::function<void()> shutdown_system;
  std::jthread liveness_thread;
  std::atomic<bool> terminate{true};
  std::mutex liveness_sleep_mutex;
  std::condition_variable liveness_sleep_cv;
  bool started = false;
  std::vector<uint8_t> current_pid_;
  ProcessTableEntry pt_entry;
  std::mutex observed_pid_table_mutex;
  std::map<std::vector<uint8_t>, ObservedPidState> observed_pid_table;
  std::map<std::vector<uint8_t>, struct timespec> bogon_pids;
  uint64_t observed_pid_scan_epoch = 0;
  std::optional<struct timespec> observed_pid_last_scan_complete_boottime;
  std::atomic<uint64_t> observed_pid_consecutive_scan_failures = 0;
};

#endif
