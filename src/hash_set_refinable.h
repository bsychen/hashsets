#ifndef HASH_SET_REFINABLE_H
#define HASH_SET_REFINABLE_H

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "src/hash_set_base.h"

// Hold lock and index to avoid recalculations
struct IndexedLock {
  std::unique_lock<std::mutex> lock;
  size_t idx;
};

// A refinable hash set implementation
// Mutex for each bucket per "Art of Multiprocessor Programming"
// Ensures public method safety via per bucket locks and resize synchronization
// Simplifications are made due to lack of reentrancy requirements 
// RAII adds simplicity avoiding need for try / finally.
template <typename T> class HashSetRefinable : public HashSetBase<T> {
  using Bucket = std::vector<T>;
  // Average number of elements per bucket before resizing
  static constexpr double kResizeThreshold = 0.75;
  // Factor to increase bucket count by during resize
  static constexpr size_t kCountResize = 2;

public:
  explicit HashSetRefinable(size_t initial_buckets = 16)
      : bucket_count_(initial_buckets) {
    assert(initial_buckets > 0);
    buckets_ = std::make_shared<std::vector<Bucket>>(initial_buckets);
    locks_ = std::make_shared<std::vector<std::mutex>>(initial_buckets);
  }

  // Using an indexed lock on the bucket's mutex
  // The lock is automatically released at the end of the function's scope.
  bool Add(T elem) override {
    // Resize if needed (before locking to prevent nested locking)
    if (Policy()) {
      Resize();
    }

    // Acquires lock (and index) for the bucket (auto release out of scope)
    IndexedLock acquired = Acquire(elem);

    // Early return if element already exists
    Bucket &bucket = (*buckets_)[acquired.idx];
    if (GetIndex(bucket, elem) != bucket.end()) {
      return false;
    }

    // Insert the element, incrementing size (atomic avoids races with 
    //  functions like Add/Size/Remove)
    bucket.push_back(elem);
    size_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  // Using an indexed lock on the bucket's mutex
  // The lock is automatically released at the end of the function's scope.
  bool Remove(T elem) override {
    // Acquires lock (and index) for the bucket (auto release out of scope)
    IndexedLock acquired = Acquire(elem);

    Bucket &bucket = (*buckets_)[acquired.idx];
    auto it = GetIndex(bucket, elem);
    if (it != bucket.end()) {
      // Remove the element if found, decrementing size (atomic avoids races 
      //  with functions like Add/Size/Remove)
      bucket.erase(it);
      size_.fetch_sub(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  // Using an indexed lock on the bucket's mutex
  // The lock is automatically released at the end of the function's scope.
  bool Contains(T elem) override {
    IndexedLock acquired = Acquire(elem);
    auto &bucket = (*buckets_)[acquired.idx];
    return GetIndex(bucket, elem) != bucket.end();
  }

  // Loads the current size (snapshot) atomically, avoids needing to 
  // synchronise with all the modifying operations
  size_t Size() const override { return size_.load(std::memory_order_acquire); }

private:
  // Atomic snapshot for the number of elements in the set
  std::atomic<size_t> size_{0};
  // Atomic snapshot for the number of buckets (for policy which does not lock)
  std::atomic<size_t> bucket_count_{0};
  // Atomic flag for if a resize is occurring (stop new operations starting)
  // Deviated from "Art of Multiprocessor Programming" which stored a thread ID 
  //   too which is unneeded as that is only required for reentrant locks
  std::atomic<bool> resizing_{false};
  // Atomic counter incremented on each resize (to avoid concurrent resizes)
  std::atomic<size_t> resize_counter_{0};

  std::shared_ptr<std::vector<Bucket>> buckets_;
  // One lock per bucket as per "Art of Multiprocessor Programming"
  // Allows finer grained locking during normal operations
  // Modified each resize to match bucket count (shared ptr avoids early frees)
  // Uses normal mutexes over reentrant locks as reentrancy is not desired
  std::shared_ptr<std::vector<std::mutex>> locks_;

  // Heuristic for determining if an addition would exceed the resize threshold
  // Value is calculated as: number of elements / number of buckets
  // SAFE TO CALL WITHOUT EXTERNAL LOCKING (size_ and bucket_count_ are atomic)
  bool Policy() const {
    return static_cast<double>(size_.load(std::memory_order_relaxed) + 1) /
               static_cast<double>(
                   bucket_count_.load(std::memory_order_relaxed)) >
           kResizeThreshold;
  }

  // Helper function as described in "Art of Multiprocessor Programming"
  // Waits out resizes then acquires and returns the key's lock for its bucket
  //  and index in that bucket
  // Does not check lock owners etc. due to lack of reentrancy
  // MUST CALL WITHOUT EXTERNAL LOCKING (deadlock in spin, nested acquires...)
  IndexedLock Acquire(const T &key) {
    for (;;) {
      // wait out any ongoing resize
      while (resizing_.load(std::memory_order_acquire)) {
        // spin
      }

      // Snapshot the resize counter before getting the locks
      size_t pre_count = resize_counter_.load(std::memory_order_acquire);
      // Loaded atomically to avoid data races with resize (use after free etc.)
      auto locks =
          std::atomic_load_explicit(&locks_, std::memory_order_acquire);
      
      // Get the lock, will not have been freed as we have a pointer to it
      size_t idx = std::hash<T>()(key) % locks->size();
      std::unique_lock<std::mutex> lock((*locks)[idx]);

      // if no resize started between spinning and grabbing the lock we are good
      //  Check no resize is ongoing 
      if (!resizing_.load(std::memory_order_acquire) &&
          // Check no resize occured completely in that time
          pre_count == resize_counter_.load(std::memory_order_acquire)) {
        // Return lock and index
        return IndexedLock{std::move(lock), idx};
      }
      // else: a resize started after we locked; lock is released (RAII), retry
    }
  }

  // Waits until no locks are held by any thread
  // MUST CALL WITHOUT EXTERNAL LOCKING (obvious deadlock if a bucket lock held)
  void Quiesce() {
    for (;;) { 
      // Check if all locks are free
      bool all_free = true;
      // by invariants locks_ are safe for loops if Quiesce is running
      // as resize has been set and this resize is running Quiesce
      for (auto &m : *locks_) {
        if (!m.try_lock()) {
          // Found a lock held by another thread, try again
          all_free = false;
          break;
        }
        m.unlock();
      }
      
      // All locks are free, we can proceed
      if (all_free)
        break;
    }
  }

  // Resizes the bucket array by increasing its size and rehashing all elements
  // Resizes the locks_ array too to match new bucket count (as per book)
  // MUST CALL WITHOUT EXTERNAL LOCKING (obvious deadlock if a bucket lock held)
  void Resize() {
    // Snapshot current bucket count
    size_t pre_bucket_count = bucket_count_.load(std::memory_order_relaxed);
    
    // Early return if another resize is occurring
    bool exchange = false;
    if (!resizing_.compare_exchange_strong(exchange, true, std::memory_order_acq_rel)) {
      return;
    }

    // Check if a resize has occurred now we have resizing avoids double resizes
    // Caused by another add occurring before the previous resize affects policy
    // Direct .size() call is ok as we have resizing 
    if ((*buckets_).size() != pre_bucket_count) {
      resizing_.store(false, std::memory_order_release);
      return;
    }

    // Wait until all locks are free (other modifying operations complete)
    Quiesce();

    // Create new bucket array and rehash all elements
    size_t new_size = (*buckets_).size() * kCountResize;
    auto new_buckets = std::make_shared<std::vector<Bucket>>(new_size);
    for (const auto &bucket : *buckets_) {
      for (const auto &elem : bucket) {
        (*new_buckets)[std::hash<T>()(elem) % new_size].push_back(elem);
      }
    }

    // Update buckets_ and locks_ with new bucket array (atomically)
    std::atomic_store_explicit(&buckets_, new_buckets,
                               std::memory_order_release);
    bucket_count_.store(new_size, std::memory_order_relaxed);

    auto new_locks = std::make_shared<std::vector<std::mutex>>(new_size);
    std::atomic_store_explicit(&locks_, new_locks, std::memory_order_release);

    // Update the resize counter and clear the resizing flag
    resize_counter_.fetch_add(1, std::memory_order_release);
    resizing_.store(false, std::memory_order_release);
  }

  // Returns the index of an element in a bucket (list.end() if not found)
  // UNSAFE TO CALL WITHOUT EXTERNAL LOCKING (Ensure bucket lock acquired)
  static auto GetIndex(const Bucket &list, const T &item) {
    return std::find(list.begin(), list.end(), item);
  }
};

#endif // HASH_SET_REFINABLE_H
