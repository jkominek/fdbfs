#ifndef __GARBAGE_COLLECTOR_H__
#define __GARBAGE_COLLECTOR_H__

#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

class GarbageCollectorService {
public:
  GarbageCollectorService();
  ~GarbageCollectorService();

  GarbageCollectorService(const GarbageCollectorService &) = delete;
  GarbageCollectorService &operator=(const GarbageCollectorService &) = delete;
  GarbageCollectorService(GarbageCollectorService &&) = delete;
  GarbageCollectorService &operator=(GarbageCollectorService &&) = delete;

private:
  void garbage_scanner(std::stop_token stop_token);
  void process_gc_candidate(const std::vector<uint8_t> &garbage_key);

  std::jthread gc_thread;
  std::mutex gc_sleep_mutex;
  std::condition_variable gc_sleep_cv;
};

#endif // __GARBAGE_COLLECTOR_H__
