#ifndef HASH_SET_STRIPED_H
#define HASH_SET_STRIPED_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <vector>

#include "src/hash_set_base.h"

// A striped lock hash set implementation
// Mutex for each initial bucket per "Art of Multiprocessor Programming"
// Mods by initial bucket count to map elements to mutexes
// Ensures public method safety via stripe locks
// No unnecessary Acquire/Release wrappers like the book, used RAII scoped locks 
template <typename T> class HashSetStriped : public HashSetBase<T> {
  using Bucket = std::vector<T>;
  // Average number of elements per bucket before resizing
  static constexpr double kResizeThreshold = 0.75;
  // Factor to increase bucket count by during resize
  static constexpr size_t kCountResize = 2;

public:
  explicit HashSetStriped(size_t initial_buckets = 16)
      : bucket_count_(initial_buckets), buckets_(initial_buckets),
        mutexes_(initial_buckets) {
    assert(initial_buckets > 0);
  }

  // Using a scoped lock on the bucket's (mod initial bucket count) mutex
  // The lock is automatically released at the end of the function's scope.
  bool Add(T elem) override {
    // Resize if needed (checked lock to prevent nested locking)
    if (Policy()) {
      Resize();
    }

    // Find and acquire lock for the bucket (mod initial bucket count)
    std::mutex &bucket_mutex = GetBucketMutex(elem);
    std::scoped_lock<std::mutex> lock(bucket_mutex);

    // Early return if element already exists
    Bucket &bucket = GetBucket(elem);
    if (GetIndex(bucket, elem) != bucket.end()) {
      return false;
    }

    // Insert the element, incrementing size (atomic to avoid race with Size())
    bucket.push_back(elem);
    size_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  // Using a scoped lock on the bucket's (mod initial bucket count) mutex
  // The lock is automatically released at the end of the function's scope.
  bool Remove(T elem) override {
    // Find and acquire lock for the bucket (mod initial bucket count)
    std::mutex *bucket_mutex = &GetBucketMutex(elem);
    std::scoped_lock<std::mutex> bucket_lock(*bucket_mutex);

    Bucket &bucket = GetBucket(elem);
    auto it = GetIndex(bucket, elem);
    if (it != bucket.end()) {
      // Remove the element, decrementing size (atomic avoids race with Size())
      bucket.erase(it);
      size_.fetch_sub(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  // Using a scoped lock on the bucket's (mod initial bucket count) mutex
  // The lock is automatically released at the end of the function's scope.
  bool Contains(T elem) override {
    std::mutex *bucket_mutex = &GetBucketMutex(elem);
    std::scoped_lock<std::mutex> bucket_lock(*bucket_mutex);

    auto &bucket = GetBucket(elem);
    return GetIndex(bucket, elem) != bucket.end();
  }

  // Uses atomic load to read approximate size without needing to get every lock
  size_t Size() const override { return size_.load(std::memory_order_relaxed); }

private:
  // Atomic to avoid races in size()
  std::atomic<size_t> size_{0};
  // Lock free snapshot for policy checks
  std::atomic<size_t> bucket_count_{0};

  std::vector<Bucket> buckets_;
  // One mutex per initial bucket as per book, however these are not reentrant
  // locks as in the book, rather standard mutexes as reentrancy isn't desired
  std::vector<std::mutex> mutexes_;

  // Heuristic for determining if an addition would exceed the resize threshold
  // Value is calculated as: number of elements / number of buckets
  // SAFE TO CALL WITHOUT EXTERNAL LOCKING (size_ and bucket_count_ are atomic)
  bool Policy() const {
    return static_cast<double>(size_.load(std::memory_order_relaxed)) /
               static_cast<double>(
                   bucket_count_.load(std::memory_order_relaxed)) >
           kResizeThreshold;
  }

  // Resizes the bucket array by doubling its size and rehashing all elements.
  // Acquires all locks sequentially to prevent races.
  // MUST NOT BE CALLED WITH EXTERNAL LOCKING (Deadlocks)
  void Resize() {
    // Snapshot current bucket count
    size_t pre_bucket_count = bucket_count_.load(std::memory_order_relaxed);

    // Acquire locks in order by index to avoid deadlocks, protects all buckets
    // Reserve increases performance, unique because of vector usage (movable)
    std::vector<std::unique_lock<std::mutex>> locks;
    locks.reserve(mutexes_.size());
    for (auto &mutex : mutexes_) {
      locks.emplace_back(mutex);
    }

    // Check if a resize has occurred after getting locks avoids double resizes
    // Caused by another add occurring before the previous resize affects policy
    // Direct .size() call is ok as we have all locks
    if (buckets_.size() != pre_bucket_count) {
      return;
    }

    // Create new bucket array and rehash all elements
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

  // Returns the bucket corresponding to the given element
  // UNSAFE TO CALL WITHOUT EXTERNAL LOCKING (Ensure no concurrent resize)
  Bucket &GetBucket(const T &elem) {
    return buckets_[std::hash<T>()(elem) % buckets_.size()];
  }

  // Returns the mutex protecting the bucket containing the given element
  // UNSAFE TO CALL WITHOUT EXTERNAL LOCKING (Ensure no concurrent resize)
  std::mutex &GetBucketMutex(const T &elem) {
    return mutexes_[std::hash<T>()(elem) % mutexes_.size()];
  }

  // Returns the index of an element in a bucket (list.end() if not found)
  // UNSAFE TO CALL WITHOUT EXTERNAL LOCKING (Ensure bucket lock acquired)
  static auto GetIndex(const Bucket &list, const T &item) {
    return std::find(list.begin(), list.end(), item);
  }
};

#endif // HASH_SET_STRIPED_H
