#ifndef _LIVENESS_H_
#define _LIVENESS_H_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

enum class PidLiveness { Alive, Dead, Unknown };

extern std::vector<uint8_t> pid; // our unique id for use records

extern PidLiveness classify_pid(const std::vector<uint8_t> &);

class LivenessService {
public:
  explicit LivenessService(std::function<void()> shutdown_cb);
  ~LivenessService();

  LivenessService(const LivenessService &) = delete;
  LivenessService &operator=(const LivenessService &) = delete;
  LivenessService(LivenessService &&) = delete;
  LivenessService &operator=(LivenessService &&) = delete;

private:
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
};

#endif
