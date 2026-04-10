#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <pthread.h>

#include <stdexcept>

#include "fdb_service.h"

namespace {

/* Purely to get the FoundationDB network stuff running in a
 * background thread. Passing fdb_run_network straight to
 * pthread_create kind of works, but let's pretend this will
 * be robust cross platform code someday.
 */
void *network_runner(void *ignore) {
  (void)ignore;
  // TODO capture the return code and do something
  if (fdb_run_network()) {
    ;
  }
  return nullptr;
}

} // namespace

FdbService::FdbService(bool buggify) : FdbService(nullptr, buggify) {}

FdbService::FdbService(const char *cluster_file, bool buggify) {
  if (fdb_select_api_version(FDB_API_VERSION)) {
    throw std::runtime_error("fdb_select_api_version failed");
  }

  if (buggify &&
      fdb_network_set_option(FDB_NET_OPTION_CLIENT_BUGGIFY_ENABLE, nullptr,
                             0)) {
    throw std::runtime_error("fdb_network_set_option(buggify) failed");
  }

  if (fdb_setup_network()) {
    throw std::runtime_error("fdb_setup_network failed");
  }
  network_setup_done_ = true;

  if (pthread_create(&network_thread_, nullptr, network_runner, nullptr)) {
    throw std::runtime_error("pthread_create(network_thread) failed");
  }
  network_thread_created_ = true;

  if (fdb_create_database(cluster_file, &database_)) {
    database_ = nullptr;
    throw std::runtime_error("fdb_create_database failed");
  }
}

FdbService::~FdbService() {
  if (database_) {
    fdb_database_destroy(database_);
    database_ = nullptr;
  }

  if (network_setup_done_) {
    const fdb_error_t err = fdb_stop_network();
    (void)err;
  }
  if (network_thread_created_) {
    (void)pthread_join(network_thread_, nullptr);
  }
}

unique_transaction FdbService::make_transaction() const {
  unique_transaction ut;
  FDBTransaction *t;
  if (fdb_database_create_transaction(database_, &t)) {
    // this is catastrophic. we can no longer communicate with the database.
    std::terminate();
  }
  ut.reset(t);
  return ut;
}
