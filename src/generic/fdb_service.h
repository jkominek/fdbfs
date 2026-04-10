#ifndef __FDB_SERVICE_H__
#define __FDB_SERVICE_H__

#include <foundationdb/fdb_c.h>
#include <pthread.h>

#include "util.h"

class FdbService {
public:
  explicit FdbService(bool buggify);
  FdbService(const char *cluster_file, bool buggify);
  ~FdbService();

  FdbService(const FdbService &) = delete;
  FdbService &operator=(const FdbService &) = delete;
  FdbService(FdbService &&) = delete;
  FdbService &operator=(FdbService &&) = delete;

  [[nodiscard]] unique_transaction make_transaction() const;

private:
  FDBDatabase *database_ = nullptr;
  pthread_t network_thread_{};
  bool network_thread_created_ = false;
  bool network_setup_done_ = false;
};

#endif // __FDB_SERVICE_H__
