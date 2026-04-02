#ifndef __FDBFS_RUNTIME_H__
#define __FDBFS_RUNTIME_H__

#include <functional>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

class FdbfsRuntime {
public:
  FdbfsRuntime() = default;
  ~FdbfsRuntime();

  FdbfsRuntime(const FdbfsRuntime &) = delete;
  FdbfsRuntime &operator=(const FdbfsRuntime &) = delete;
  FdbfsRuntime(FdbfsRuntime &&) = delete;
  FdbfsRuntime &operator=(FdbfsRuntime &&) = delete;

  template <typename T, typename Factory>
  void add_persistent(Factory &&factory) {
    add_service<T>(persistent_services, std::forward<Factory>(factory));
  }

  template <typename T, typename Factory>
  void add_restartable(Factory &&factory) {
    add_service<T>(restartable_services, std::forward<Factory>(factory));
  }

  void start_persistent();
  void start_restartable();
  void start_all();
  void stop_restartable() noexcept;
  void restart_restartable();

  template <typename T> [[nodiscard]] T *get() {
    void *p = get_raw(typeid(T));
    return static_cast<T *>(p);
  }

  template <typename T> [[nodiscard]] const T *get() const {
    void *p = get_raw(typeid(T));
    return static_cast<const T *>(p);
  }

  template <typename T> [[nodiscard]] T &require() {
    T *p = get<T>();
    if (p == nullptr) {
      throw std::logic_error("required service is not running");
    }
    return *p;
  }

  template <typename T> [[nodiscard]] const T &require() const {
    const T *p = get<T>();
    if (p == nullptr) {
      throw std::logic_error("required service is not running");
    }
    return *p;
  }

private:
  struct ServiceSlot {
    std::type_index type;
    std::function<void *()> create;
    std::function<void(void *)> destroy;
    void *instance = nullptr;
  };

  template <typename T, typename Factory>
  void add_service(std::vector<ServiceSlot> &slots, Factory &&factory) {
    static_assert(std::is_same_v<T, std::remove_cv_t<T>>,
                  "service type must be unqualified");
    static_assert(!std::is_reference_v<T>,
                  "service type must not be a reference");

    if (persistent_started || restartable_started) {
      throw std::logic_error("cannot add services after runtime has started");
    }
    if (has_service_type(typeid(T))) {
      throw std::logic_error("duplicate service type registration");
    }

    using FactoryResult = std::invoke_result_t<Factory &>;
    static_assert(std::is_convertible_v<FactoryResult, std::unique_ptr<T>>,
                  "factory must return std::unique_ptr<T>");

    slots.push_back(ServiceSlot{
        .type = typeid(T),
        .create =
            [f = std::forward<Factory>(factory)]() mutable -> void * {
              std::unique_ptr<T> service = f();
              return service.release();
            },
        .destroy = [](void *p) { delete static_cast<T *>(p); },
        .instance = nullptr,
    });
  }

  [[nodiscard]] bool has_service_type(std::type_index type) const;
  [[nodiscard]] void *get_raw(std::type_index type) const;

  static void start_slots(std::vector<ServiceSlot> &slots);
  static void stop_slots(std::vector<ServiceSlot> &slots) noexcept;

  std::vector<ServiceSlot> persistent_services;
  std::vector<ServiceSlot> restartable_services;
  bool persistent_started = false;
  bool restartable_started = false;
};

extern FdbfsRuntime *g_fdbfs_runtime;

#endif // __FDBFS_RUNTIME_H__
