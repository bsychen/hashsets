#ifndef HASH_SET_COARSE_GRAINED_H
#define HASH_SET_COARSE_GRAINED_H

#include <cassert>
#include <mutex>

#include "src/hash_set_base.h"

template <typename T> class HashSetCoarseGrained : public HashSetBase<T> {
public:
  explicit HashSetCoarseGrained(size_t initial_buckets = 16) {
    assert(initial_buckets > 0);
    buckets_ = std::vector<bucket_t>(initial_buckets, bucket_t());
  }

  bool Add(T elem) override {
    bucket_t *bucket = &GetBucket(elem);

    if (GetIndex(*bucket, elem) != bucket->end()) {
      return false;
    }

    if (size_ > kResizeThreshold * buckets_.size()) {
      Resize();
      bucket = &GetBucket(elem);
    }

    bucket->push_back(elem);
    ++size_;
    return true;
  }

  bool Remove(T elem) override {
    bucket_t &bucket = GetBucket(elem);
    auto it = GetIndex(bucket, elem);
    if (it != bucket.end()) {
      bucket.erase(it);
      --size_;
      return true;
    }
    return false;
  }

  bool Contains(T elem) override {
    auto &bucket = GetBucket(elem);
    return GetIndex(bucket, elem) != bucket.end();
  }

  size_t Size() const override { return size_; }

private:
  std::mutex mutex_;
};

#endif  // HASH_SET_COARSE_GRAINED_H
