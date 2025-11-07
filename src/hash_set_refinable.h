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

template <typename T> class HashSetRefinable : public HashSetBase<T> {
  using Bucket = std::vector<T>;
  static constexpr double kResizeThreshold = 0.75;
  static constexpr size_t kCountResize = 2;

public:
  explicit HashSetRefinable(size_t initial_buckets = 16) 
  : bucket_count_(initial_buckets),
    buckets_(initial_buckets) {
    assert(initial_buckets > 0);
    lock_array_ = std::make_shared<std::vector<std::mutex>>(initial_buckets);
  }

  bool Add(T elem) override {
    if (Policy()) {
        Resize();
    }

    std::unique_lock<std::mutex> lock = Acquire(elem);

    Bucket& bucket = GetBucket(elem);

    if (GetIndex(bucket, elem) != bucket.end()) {
        return false;
    }

    bucket.push_back(elem);
    ++size_;
    return true;
  }

  bool Remove(T elem) override {
    std::unique_lock<std::mutex> lock = Acquire(elem);

    Bucket& bucket = GetBucket(elem);
    auto it = GetIndex(bucket, elem);
    if (it != bucket.end()) {
      bucket.erase(it);
      --size_;
      return true;
    }
    return false;
  }

  bool Contains(T elem) override {
    std::unique_lock<std::mutex> lock = Acquire(elem);

    auto& bucket = GetBucket(elem);
    return GetIndex(bucket, elem) != bucket.end();
  }

  size_t Size() const override {
    return size_.load(std::memory_order_acquire);
  }

private:
  std::atomic<size_t> size_{0};
  std::atomic<size_t> bucket_count_{0};
  std::atomic<bool> resizing_{false};

  std::vector<Bucket> buckets_;
  std::shared_ptr<std::vector<std::mutex>> lock_array_;

  bool Policy() const {
    return static_cast<double>(size_.load(std::memory_order_relaxed)) / 
           static_cast<double>(bucket_count_.load(std::memory_order_relaxed)) 
           > kResizeThreshold;
  }

  std::unique_lock<std::mutex> Acquire(const T& key) {
    for (;;) {
      // wait out any ongoing resize
      while (resizing_.load(std::memory_order_acquire)) {
        
      }

      auto locks = std::atomic_load_explicit(&lock_array_,
                                            std::memory_order_acquire);
      size_t idx = std::hash<T>()(key) % locks->size();
      std::unique_lock<std::mutex> lock((*locks)[idx]);

      // if a resize did not start after we grabbed the lock, we’re good
      if (!resizing_.load(std::memory_order_acquire)) {
        return lock;
      }
      // else: a resize started after we locked; lock is released here (RAII),
      // and we retry
    }
  }

  void Quiesce() {
    auto locks = std::atomic_load_explicit(&lock_array_,
                                          std::memory_order_acquire);
    for (;;) {
      bool all_free = true;
      for (auto &m : *locks) {
        if (m.try_lock()) {
          m.unlock();
        } else {
          all_free = false;
        }
      }
      if (all_free) break;
    }
  }

  void Resize() {
    bool expected = false;
    if (!resizing_.compare_exchange_strong(expected, true,
                                           std::memory_order_acq_rel)) {
      return;
    }

    if (!Policy()) {
      resizing_.store(false, std::memory_order_release);
      return;
    }

    Quiesce();

    size_t new_size = buckets_.size() * kCountResize;

    std::vector<Bucket> new_buckets(new_size);
    for (const auto &bucket : buckets_) {
      for (const auto &elem : bucket) {
        new_buckets[std::hash<T>()(elem) % new_size].push_back(elem);
      }
    }
    
    buckets_ = std::move(new_buckets);
    bucket_count_.store(new_size, std::memory_order_relaxed);
    
    auto new_locks = std::make_shared<std::vector<std::mutex>>(new_size);
    std::atomic_store_explicit(&lock_array_, new_locks,
                              std::memory_order_release);

    resizing_.store(false, std::memory_order_release);
  }

  Bucket& GetBucket(const T &elem) {
    return buckets_[std::hash<T>()(elem) % bucket_count_.load(std::memory_order_acquire)];
  }

  std::mutex& GetBucketMutex(const T &elem) {
    auto locks = std::atomic_load_explicit(&lock_array_,
                                           std::memory_order_acquire);
    size_t idx = std::hash<T>()(elem) % bucket_count_.load(std::memory_order_acquire);
    return (*locks)[idx];
  }

  typename Bucket::const_iterator GetIndex(const Bucket &list,
                                           const T &item) const {
    return std::find(list.begin(), list.end(), item);
  }
};

#endif // HASH_SET_REFINABLE_H
