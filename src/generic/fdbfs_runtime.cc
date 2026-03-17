#include "fdbfs_runtime.h"

#include <exception>

FdbfsRuntime::~FdbfsRuntime() {
  stop_restartable();
  stop_slots(persistent_services);
  persistent_started = false;
}

void FdbfsRuntime::start_persistent() {
  if (persistent_started) {
    return;
  }
  start_slots(persistent_services);
  persistent_started = true;
}

void FdbfsRuntime::start_restartable() {
  if (restartable_started) {
    return;
  }
  start_slots(restartable_services);
  restartable_started = true;
}

void FdbfsRuntime::start_all() {
  start_persistent();
  start_restartable();
}

void FdbfsRuntime::stop_restartable() noexcept {
  if (!restartable_started) {
    return;
  }
  stop_slots(restartable_services);
  restartable_started = false;
}

void FdbfsRuntime::restart_restartable() {
  stop_restartable();
  start_restartable();
}

bool FdbfsRuntime::has_service_type(std::type_index type) const {
  for (const auto &slot : persistent_services) {
    if (slot.type == type) {
      return true;
    }
  }
  for (const auto &slot : restartable_services) {
    if (slot.type == type) {
      return true;
    }
  }
  return false;
}

void *FdbfsRuntime::get_raw(std::type_index type) const {
  for (const auto &slot : restartable_services) {
    if (slot.type == type) {
      return slot.instance;
    }
  }
  for (const auto &slot : persistent_services) {
    if (slot.type == type) {
      return slot.instance;
    }
  }
  return nullptr;
}

void FdbfsRuntime::start_slots(std::vector<ServiceSlot> &slots) {
  std::vector<size_t> started_indices;
  try {
    for (size_t i = 0; i < slots.size(); ++i) {
      auto &slot = slots[i];
      if (slot.instance != nullptr) {
        continue;
      }
      slot.instance = slot.create();
      started_indices.push_back(i);
    }
  } catch (...) {
    // Roll back only the services started in this invocation, reverse order.
    for (size_t i = started_indices.size(); i > 0; --i) {
      auto &slot = slots[started_indices[i - 1]];
      if (slot.instance != nullptr) {
        slot.destroy(slot.instance);
        slot.instance = nullptr;
      }
    }
    throw;
  }
}

void FdbfsRuntime::stop_slots(std::vector<ServiceSlot> &slots) noexcept {
  for (size_t i = slots.size(); i > 0; --i) {
    auto &slot = slots[i - 1];
    if (slot.instance == nullptr) {
      continue;
    }
    try {
      slot.destroy(slot.instance);
    } catch (...) {
      // Destructors should not throw, but keep shutdown best-effort.
    }
    slot.instance = nullptr;
  }
}
