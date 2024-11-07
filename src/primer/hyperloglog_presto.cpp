#include "primer/hyperloglog_presto.h"
#include <bitset>
#include <cstdint>

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  if (n_leading_bits >= 0) {
    this->n_leading_bits_ = n_leading_bits;
    p_ = pow(2, n_leading_bits);
    //寄存器中添加元素0
    for (unsigned int i = 0; i < p_; i++) {
      std::bitset<DENSE_BUCKET_SIZE> bset;
      dense_bucket_.push_back(bset);
    }
  } else {
    this->n_leading_bits_ = 0;
    p_ = 0;
  }
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash = CalculateHash(val);
  //得到hash的二进制表示
  std::bitset<BIT_SIZE> bset(hash);
  for (unsigned int i = 0; i < BIT_SIZE; i++) {
    std::cout << bset[i];
  }
  std::cout << std::endl;
  //得到bucket的索引
  uint16_t index = 0;
  uint64_t start = BIT_SIZE - n_leading_bits_;
  for (uint64_t i = start; i < BIT_SIZE; i++) {
    index += static_cast<double>(bset[i]) * pow(2, (i - start));
  }

  //得到连续的0的数量
  uint64_t count = 0;
  for (uint64_t i = 0; i < BIT_SIZE; i++) {
    if (static_cast<int>(bset[i]) == 1) {
      count = i;
      break;
    }
  }

  //得到连续0的二进制表示
  std::bitset<TOTAL_BUCKET_SIZE> bset1(count);
  std::bitset<DENSE_BUCKET_SIZE> dbset;
  // dbset和obset构造
  for (unsigned int i = 0; i < DENSE_BUCKET_SIZE; i++) {
    dbset[i] = bset1[i];
  }
  if (dbset.to_ullong() > dense_bucket_[index].to_ullong()) {
    for (unsigned int i = 0; i < DENSE_BUCKET_SIZE; i++) {
      dense_bucket_[index][i] = bset1[i];
    }
  }
  //标记是否溢出
  int flag = 0;
  std::bitset<OVERFLOW_BUCKET_SIZE> obset;
  for (unsigned int i = DENSE_BUCKET_SIZE; i < TOTAL_BUCKET_SIZE; i++) {
    obset[i - DENSE_BUCKET_SIZE] = bset1[i];
    if (static_cast<int>(bset1[i]) == 1) {
      flag = 1;
    }
  }

  //插入溢出
  if (flag == 1) {
    if (overflow_bucket_.count(index) == 1) {
      overflow_bucket_[index] =
          overflow_bucket_[index].to_ullong() > obset.to_ullong() ? overflow_bucket_[index] : obset;
    } else {
      overflow_bucket_[index] = obset;
    }
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  float sum = 0;
  for (unsigned int i = 0; i < p_; i++) {
    uint64_t num = dense_bucket_[i].to_ullong();
    if (num == 0) {
      sum += 1;
    } else {
      int n = static_cast<int>(num);
      sum += pow(2, -n);  // 3+1/256
    }
  }
  if (sum == 0) {
    cardinality_ = 0;
  } else {
    cardinality_ = static_cast<size_t>(std::floor(CONSTANT * p_ * p_ / sum));
  }
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
