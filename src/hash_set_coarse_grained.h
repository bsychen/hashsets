#ifndef HASH_SET_COARSE_GRAINED_H
#define HASH_SET_COARSE_GRAINED_H

#include <algorithm>
#include <cassert>
#include <functional>
#include <mutex>    
#include <vector>

#include "src/hash_set_base.h"

template <typename T> class HashSetCoarseGrained : public HashSetBase<T> {
  using Bucket = std::vector<T>;
  static constexpr double kResizeThreshold = 0.75;
  static constexpr size_t kCountResize = 2;

public:
  explicit HashSetCoarseGrained(size_t initial_buckets = 16) {
    assert(initial_buckets > 0);
    buckets_ = std::vector<Bucket>(initial_buckets, Bucket());
  }

  bool Add(T elem) override {
    std::scoped_lock<std::mutex> lock(mutex_);

    Bucket *bucket = &GetBucket(elem);

    if (GetIndex(*bucket, elem) != bucket->end()) { return false; }

    if (Policy()) { Resize(); bucket = &GetBucket(elem); }

    bucket->push_back(elem);
    ++size_;
    return true;
  }

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

  bool Contains(T elem) override {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto &bucket = GetBucket(elem);
    return GetIndex(bucket, elem) != bucket.end();
  }

  size_t Size() const override { 
    std::scoped_lock<std::mutex> lock(mutex_);
    return size_; 
  }

private:
  mutable std::mutex mutex_;
  size_t size_ = 0;
  std::vector<Bucket> buckets_;

  bool Policy() const {
    return static_cast<double>(size_) / static_cast<double>(buckets_.size()) > kResizeThreshold;
  }

  void Resize() {
    size_t new_size = buckets_.size() * kCountResize;
    std::vector<Bucket> new_buckets(new_size, Bucket());

    for (const auto &bucket : buckets_) {
      for (const auto &elem : bucket) {
        new_buckets[std::hash<T>()(elem) % new_size].push_back(elem);
      }
    }

    buckets_ = std::move(new_buckets);
  }

  Bucket &GetBucket(const T &elem) {
    return buckets_[std::hash<T>()(elem) % buckets_.size()];
  }

  typename Bucket::const_iterator GetIndex(const Bucket &list,
                                             const T &item) const {
    return std::find(list.begin(), list.end(), item);
  }
};

#endif  // HASH_SET_COARSE_GRAINED_H
