#include "primer/hyperloglog.h"
#include <algorithm>
#include <bitset>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include "common/util/hash_util.h"
#include "type/vector_type.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  if (n_bits >= 0) {
    this->n_bits_ = n_bits;
    m_ = pow(2, n_bits);
    //寄存器中添加元素0
    for (unsigned int i = 0; i < m_; i++) {
      registers_.push_back(0);
    }
  } else {
    this->n_bits_ = 0;
    m_ = 0;
  }
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  std::bitset<BITSET_CAPACITY> bset(hash);
  return bset;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  int i;
  int start = BITSET_CAPACITY - n_bits_ - 1;
  for (i = start; i >= 0; i--) {
    if (static_cast<int>(bset[i]) == 1) {
      break;
    }
  }
  //索引从1开始计数
  return start - i + 1;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bset = ComputeBinary(hash);
  uint64_t pos = PositionOfLeftmostOne(bset);
  // unsigned int i;
  // for(i=0;i<bset.size();i++)
  // std::cout<<bset[i]<<" ";
  // std::cout<<"bset size:"<<bset.size();
  //计算寄存器的位置
  uint64_t index = 0;
  uint64_t start = BITSET_CAPACITY - this->n_bits_;
  for (uint64_t i = start; i < BITSET_CAPACITY; i++) {
    index += static_cast<double>(bset[i]) * pow(2, (i - start));
  }
  // std::cout<<"index: "<<index;
  registers_[index] = registers_[index] > static_cast<int>(pos) ? registers_[index] : pos;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  float sum = 0;
  for (unsigned int i = 0; i < m_; i++) {
    if (registers_[i] == 0) {
      sum += 1;
    } else {
      sum += pow(2, -registers_[i]);
    }
  }
  if (sum == 0) {
    cardinality_ = 0;
  } else {
    cardinality_ = static_cast<size_t>(std::floor(CONSTANT * m_ * m_ / sum));
  }
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
