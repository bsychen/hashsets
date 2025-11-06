#ifndef HASH_SET_STRIPED_H
#define HASH_SET_STRIPED_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>    
#include <vector>

#include "src/hash_set_base.h"

template <typename T> class HashSetStriped : public HashSetBase<T> {
  using Bucket = std::vector<T>;
  static constexpr double kResizeThreshold = 0.75;
  static constexpr size_t kCountResize = 2;

public:
  explicit HashSetStriped(size_t initial_buckets = 16) 
    : bucket_count_(initial_buckets), 
      buckets_(initial_buckets), 
      mutexes_(initial_buckets) { 
    assert(initial_buckets > 0);
  }

  bool Add(T elem) override {
    if (Policy()) {
        Resize();
    }

    std::mutex& bucket_mutex = GetBucketMutex(elem);
    std::unique_lock<std::mutex> lock(bucket_mutex);

    Bucket& bucket = GetBucket(elem);

    if (GetIndex(bucket, elem) != bucket.end()) {
        return false;
    }

    bucket.push_back(elem);
    ++size_;
    return true;
  }

  bool Remove(T elem) override {
    std::mutex* bucket_mutex = &GetBucketMutex(elem);
    std::scoped_lock<std::mutex> bucket_lock(*bucket_mutex);

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
    std::mutex* bucket_mutex = &GetBucketMutex(elem);
    std::scoped_lock<std::mutex> bucket_lock(*bucket_mutex);

    auto& bucket = GetBucket(elem);
    return GetIndex(bucket, elem) != bucket.end();
  }

  size_t Size() const override { 
    return size_.load(std::memory_order_relaxed); 
  }

private:
  std::atomic<size_t> size_{0};
  std::atomic<size_t> bucket_count_{0};

  std::vector<Bucket> buckets_;
  std::vector<std::mutex> mutexes_;

  bool Policy() const {
    return static_cast<double>(size_.load(std::memory_order_relaxed)) / 
           static_cast<double>(bucket_count_.load(std::memory_order_relaxed)) 
           > kResizeThreshold;
  }

  void Resize() {
    std::vector<std::unique_lock<std::mutex>> locks;
    locks.reserve(mutexes_.size());
    for (auto& mutex : mutexes_) {
      locks.emplace_back(mutex);
    }

    size_t new_size = buckets_.size() * kCountResize;

    std::vector<Bucket> new_buckets(new_size, Bucket());

    for (const auto &bucket : buckets_) {
      for (const auto &elem : bucket) {
        new_buckets[std::hash<T>()(elem) % new_size].push_back(elem);
      }
    }

    buckets_ = std::move(new_buckets);
    bucket_count_.store(new_size, std::memory_order_relaxed);
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

#endif // HASH_SET_STRIPED_H
