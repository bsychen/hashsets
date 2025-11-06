#ifndef HASH_SET_REFINABLE_H
#define HASH_SET_REFINABLE_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>    
#include <vector>

#include "src/hash_set_base.h"

template <typename T> class HashSetRefinable : public HashSetBase<T> {
  using Bucket = std::vector<T>;
  static constexpr double kResizeThreshold = 0.75;
  static constexpr size_t kCountResize = 2;

public:
  explicit HashSetRefinable(size_t initial_buckets = 16) 
    : bucket_count_(initial_buckets), 
      buckets_(initial_buckets), 
      mutexes_(initial_buckets) { 
    assert(initial_buckets > 0);
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
  std::vector<std::mutex> mutexes_;

  bool Policy() const {
    return static_cast<double>(size_.load(std::memory_order_relaxed)) / 
           static_cast<double>(bucket_count_.load(std::memory_order_relaxed)) 
           > kResizeThreshold;
  }

  std::unique_lock<std::mutex> Acquire(const T& mutex) {
    for (;;) {
        while (resizing_.load(std::memory_order_acquire)) {
            // wait out resize
        }
        
        auto& locks = mutexes_; 
        std::unique_lock<std::mutex> lock(locks[std::hash<T>()(mutex) % locks.size()]);
        
        if (!resizing_.load(std::memory_order_acquire)) {
            return lock;
        }
    }
  }

  void Quiesce() {
    for (;;) {
      bool all_free = true;
      for (auto &m : mutexes_) {
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
      // someone else is resizing
      return;
    }

    if (!Policy()) { 
      resizing_.store(false, std::memory_order_release);
      return; 
    }

    Quiesce();

    size_t new_size = buckets_.size() * kCountResize;

    std::vector<Bucket> new_buckets(new_size, Bucket());
    for (const auto &bucket : buckets_) {
      for (const auto &elem : bucket) {
        new_buckets[std::hash<T>()(elem) % new_size].push_back(elem);
      }
    }

    buckets_ = std::move(new_buckets);
    mutexes_ = std::vector<std::mutex>(new_size);
    bucket_count_.store(new_size, std::memory_order_relaxed);
    resizing_.store(false, std::memory_order_release);
  }

  Bucket& GetBucket(const T &elem) {
    return buckets_[std::hash<T>()(elem) % buckets_.size()];
  }

  std::mutex& GetBucketMutex(const T &elem) {
    return mutexes_[std::hash<T>()(elem) % mutexes_.size()];
  }

  typename Bucket::const_iterator GetIndex(const Bucket &list,
                                           const T &item) const {
    return std::find(list.begin(), list.end(), item);
  }
};

#endif // HASH_SET_REFINABLE_H
