#ifndef HASH_SET_REFINABLE_H
#define HASH_SET_REFINABLE_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <vector>
#include <memory>

#include "src/hash_set_base.h"

// Hold lock and index to avoid recalculations
struct IndexedLock {
  std::unique_lock<std::mutex> lock;
  size_t idx;
};

template <typename T>
class HashSetRefinable : public HashSetBase<T> {
  using Bucket = std::vector<T>;
  static constexpr double kResizeThreshold = 0.75;
  static constexpr size_t kCountResize = 2;

public:
  explicit HashSetRefinable(size_t initial_buckets = 16)
      : bucket_count_(initial_buckets) {
    assert(initial_buckets > 0);
    buckets_    = std::make_shared<std::vector<Bucket>>(initial_buckets);
    lock_array_ = std::make_shared<std::vector<std::mutex>>(initial_buckets);
  }

  bool Add(T elem) override {
    if (Policy()) {
      Resize();
    }

    IndexedLock acquired = Acquire(elem);

    Bucket& bucket = (*buckets_)[acquired.idx];
    if (GetIndex(bucket, elem) != bucket.end()) {
      return false;
    }

    bucket.push_back(elem);
    size_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  bool Remove(T elem) override {
    IndexedLock acquired = Acquire(elem);

    Bucket& bucket = (*buckets_)[acquired.idx];
    auto it = GetIndex(bucket, elem);
    if (it != bucket.end()) {
      bucket.erase(it);
      size_.fetch_sub(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  bool Contains(T elem) override {
    IndexedLock acquired = Acquire(elem);
    auto& bucket = (*buckets_)[acquired.idx];
    return GetIndex(bucket, elem) != bucket.end();
  }

  size_t Size() const override {
    return size_.load(std::memory_order_acquire);
  }

private:
  std::atomic<size_t> size_{0};
  std::atomic<size_t> bucket_count_{0};
  std::atomic<bool> resizing_{false};
  std::atomic<size_t> resize_counter_{0};

  std::shared_ptr<std::vector<Bucket>> buckets_;
  std::shared_ptr<std::vector<std::mutex>> lock_array_;

  bool Policy() const {
    return static_cast<double>(size_.load(std::memory_order_relaxed)) /
               static_cast<double>(bucket_count_.load(std::memory_order_relaxed)) >
           kResizeThreshold;
  }

  IndexedLock Acquire(const T& key) {
    for (;;) {
      // wait out any ongoing resize
      while (resizing_.load(std::memory_order_acquire)) {

      }

      size_t pre_count = resize_counter_.load(std::memory_order_acquire);
      auto locks = std::atomic_load_explicit(&lock_array_,
                                            std::memory_order_acquire);
      size_t idx = std::hash<T>()(key) % locks->size();
      std::unique_lock<std::mutex> lock((*locks)[idx]);

      // if a resize did not start after we grabbed the lock, we're good
      if (!resizing_.load(std::memory_order_acquire) &&
          pre_count == resize_counter_.load(std::memory_order_acquire)) {
        return IndexedLock{std::move(lock), idx};
      }
      // else: a resize started after we locked; lock is released here (RAII),
      // and we retry
    }
  }

  void Quiesce() {
    auto locks = std::atomic_load_explicit(&lock_array_, std::memory_order_acquire);
    for (;;) {
      bool all_free = true;
      for (auto &m : *locks) {
        if (!m.try_lock()) {
          all_free = false;
          break;
        }
        m.unlock();
      }

      if (all_free) break;
    }
  }

  void Resize() {
    bool expected = false;
    if (!resizing_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      return;
    }

    if (!Policy()) {
      resizing_.store(false, std::memory_order_release);
      return;
    }

    Quiesce();

    size_t new_size = (*buckets_).size() * kCountResize;

    auto new_buckets = std::make_shared<std::vector<Bucket>>(new_size);
    for (const auto& bucket : *buckets_) {
      for (const auto& elem : bucket) {
        (*new_buckets)[std::hash<T>()(elem) % new_size].push_back(elem);
      }
    }

    std::atomic_store_explicit(&buckets_, new_buckets, std::memory_order_release);
    bucket_count_.store(new_size, std::memory_order_relaxed);

    auto new_locks = std::make_shared<std::vector<std::mutex>>(new_size);
    std::atomic_store_explicit(&lock_array_, new_locks, std::memory_order_release);

    resize_counter_.fetch_add(1, std::memory_order_release);
    resizing_.store(false, std::memory_order_release);
  }

  typename Bucket::const_iterator GetIndex(const Bucket& list, const T& item) const {
    return std::find(list.begin(), list.end(), item);
  }
};

#endif // HASH_SET_REFINABLE_H
