#ifndef HASH_SET_BASE_H
#define HASH_SET_BASE_H

#include <algorithm>
#include <cstddef>
#include <functional>
#include <vector>

const size_t kResizeThreshold = 3;
const size_t kCountResize = 2;

template <typename T> class HashSetBase {
  using bucket_t = std::vector<T>;

public:
  explicit HashSetBase() {
    buckets_ = std::vector<bucket_t>(n_buckets_, bucket_t());
  }

  virtual ~HashSetBase() = default;

  // Adds |elem| to the hash set. Returns true if |elem| was absent, and false
  // otherwise.
  virtual bool Add(T elem) {
    // Short circuit if elem already present
    bucket_t *bucket = &GetBucket(elem);

    if (GetIndex(*bucket, elem) != bucket->end()) {
      return false;
    }

    // Resize if load factor exceeded
    if (size_ + 1 >= kResizeThreshold * n_buckets_) {
      Resize();
      bucket = &GetBucket(elem);
    }

    // Add element
    bucket->push_back(elem);
    ++size_;
    return true;
  }

  // Removes |elem| from the hash set. Returns true if |elem| was present, and
  // false otherwise.
  virtual bool Remove(T elem) {
    bucket_t &bucket = GetBucket(elem);
    auto it = GetIndex(bucket, elem);
    if (it != bucket.end()) {
      bucket.erase(it);
      --size_;
      return true;
    }
    return false;
  }

  // Returns true if |elem| is present in the hash set, and false otherwise.
  [[nodiscard]] virtual bool Contains(T elem) {
    auto &bucket = GetBucket(elem);
    return GetIndex(bucket, elem) != bucket.end();
  }

  // Returns the size of the hash set.
  [[nodiscard]] virtual size_t Size() const { return size_; }

private:
  size_t size_ = 0;
  size_t n_buckets_ = 16;
  std::vector<bucket_t> buckets_;

  // Redistributes the elements in the hash set into a larger number of buckets.
  void Resize() {
    n_buckets_ *= kCountResize;
    std::vector<bucket_t> new_buckets(n_buckets_, bucket_t());

    for (const auto &bucket : buckets_) {
      for (const auto &elem : bucket) {
        new_buckets[std::hash<T>()(elem) % n_buckets_].push_back(elem);
      }
    }

    buckets_ = std::move(new_buckets);
  }

  // Returns the bucket index for the given element.
  virtual bucket_t &GetBucket(T elem) {
    return buckets_[std::hash<T>()(elem) % n_buckets_];
  }

  // Gets the index of an item in a list
  typename bucket_t::const_iterator GetIndex(const bucket_t &list, T item) const {
    return std::find(list.begin(), list.end(), item);
  }
};

#endif // HASH_SET_BASE_H
