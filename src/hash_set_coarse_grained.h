#ifndef HASH_SET_COARSE_GRAINED_H
#define HASH_SET_COARSE_GRAINED_H

#include <algorithm>
#include <cassert>
#include <functional>
#include <mutex>
#include <vector>

#include "src/hash_set_base.h"

// A coarse-grained lock hash set implementation
// Wraps all public functions with the acquiring of a global mutex
template <typename T> class HashSetCoarseGrained : public HashSetBase<T> {
  using Bucket = std::vector<T>;
  // Average number of elements per bucket before resizing
  static constexpr double kResizeThreshold = 0.75;
  // Factor to increase bucket count by during resize
  static constexpr size_t kCountResize = 2;

public:
  explicit HashSetCoarseGrained(size_t initial_buckets = 16) {
    assert(initial_buckets > 0);
    buckets_ = std::vector<Bucket>(initial_buckets, Bucket());
  }

  // Using a scoped lock ensures only one thread is modifying the buckets
  // at any time and automatically unlocks at the end of the function's scope
  bool Add(T elem) override {
    std::scoped_lock<std::mutex> lock(mutex_);

    if (Policy()) {
      Resize();
    }

    Bucket *bucket = &GetBucket(elem);
    if (GetIndex(*bucket, elem) != bucket->end()) {
      return false;
    }

    bucket->push_back(elem);
    ++size_;
    return true;
  }

  // Using a scoped lock ensures only one thread is modifying the buckets
  // at any time and automatically unlocks at the end of the function's scope
  bool Remove(T elem) override {
    std::scoped_lock<std::mutex> lock(mutex_);

    Bucket &bucket = GetBucket(elem);
    auto it = GetIndex(bucket, elem);
    if (it != bucket.end()) {
      bucket.erase(it);
      --size_;
      return true;
    }
    return false;
  }

  // Using a scoped lock ensures only one thread is modifying the buckets
  // at any time and automatically unlocks at the end of the function's scope
  bool Contains(T elem) override {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto &bucket = GetBucket(elem);
    return GetIndex(bucket, elem) != bucket.end();
  }

  // Using a scoped lock ensures no other thread is modifying the buckets at
  // this time and automatically unlocks at the end of the function's scope
  size_t Size() const override {
    std::scoped_lock<std::mutex> lock(mutex_);
    return size_;
  }

private:
  size_t size_ = 0;
  mutable std::mutex mutex_;
  std::vector<Bucket> buckets_;

  // Heuristic for determining if an addition would exceed the resize threshold
  // Value is calculated as: number of elements / number of buckets
  // UNSAFE TO CALL WITHOUT EXTERNAL LOCKING
  bool Policy() const {
    return kResizeThreshold < static_cast<double>(size_ + 1) /
                                  static_cast<double>(buckets_.size());
  }

  // Resizes the bucket array by increasing its size and rehashing all elements
  // UNSAFE TO CALL WITHOUT EXTERNAL LOCKING
  void Resize() {
    // Double the size of buckets
    size_t new_size = buckets_.size() * kCountResize;
    std::vector<Bucket> new_buckets(new_size, Bucket());

    // Rehash all existing elements into the new buckets
    for (const auto &bucket : buckets_) {
      for (const auto &elem : bucket) {
        new_buckets[std::hash<T>()(elem) % new_size].push_back(elem);
      }
    }

    buckets_ = std::move(new_buckets);
  }

  // Returns the bucket corresponding to the given element
  // UNSAFE TO CALL WITHOUT EXTERNAL LOCKING
  Bucket &GetBucket(const T &elem) {
    return buckets_[std::hash<T>()(elem) % buckets_.size()];
  }

  // Returns the index of an element in a bucket (list.end() if not found)
  // UNSAFE TO CALL WITHOUT EXTERNAL LOCKING (Ensure lock acquired)
  static auto GetIndex(const Bucket &list, const T &item) {
    return std::find(list.begin(), list.end(), item);
  }
};

#endif // HASH_SET_COARSE_GRAINED_H
