//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <cstdint>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { 
    frame_id_t evicted_id=0;
    size_t least_timestamp=UINT_LEAST64_MAX;
    //先遍历不满足k次access，比最近timestamp
    for(frame_id_t id=1;static_cast<size_t>(id)<=replacer_size_;id++)
    {
        if(node_store_.count(id)==1&&node_store_[id].is_evictable()==true)
        {
            LRUKNode&node=node_store_[id];
            if(node.get_access_num()<k_)
            {
                if(node.get_history()<=least_timestamp)
                {
                    least_timestamp=node.get_history();
                    evicted_id=id;
                }
            }
        }
    }
    //不存在access数小于k的
    if(evicted_id==0)
    {
      for(frame_id_t id=1;static_cast<size_t>(id)<=replacer_size_;id++)
     {
        if(node_store_.count(id)==1&&node_store_[id].is_evictable()==true)
        {
            LRUKNode&node=node_store_[id];
            if(node.get_history()<=least_timestamp)
            {
              least_timestamp=node.get_history();
              evicted_id=id;
            }
        }
     }
    }
    //history清空,curr_size-1,evictale置false
    if(evicted_id!=0)
    {
       curr_size_-=1;
       LRUKNode&node=node_store_[evicted_id];
       node.clear_history();
       node.set_access_num(0);
       node.set_evictable(false);
       return std::optional<frame_id_t>(evicted_id);
    }
    else
    return std::nullopt; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    //超出了范围
    if(frame_id<=0||static_cast<size_t>(frame_id)>replacer_size_)
    {
        throw std::out_of_range("RecordAccess:out of range");
    }
    
    if(node_store_.count(frame_id)==0)
    {
        LRUKNode node;
        node.add_history(current_timestamp_);
        size_t num=node.get_access_num();
        node.set_access_num(num+1);   
        node_store_[frame_id]=std::move(node);
    }else {
        LRUKNode&node=node_store_[frame_id];
        node.add_history(current_timestamp_);
        size_t num=node.get_access_num();
        node.set_access_num(num+1);      
    }
    current_timestamp_+=1;//可能需要加锁
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if(frame_id<=0||static_cast<size_t>(frame_id)>replacer_size_)
    {
        throw std::out_of_range("SetEvictable:out of range");
    }

    if(node_store_.count(frame_id)==1)
    {
        LRUKNode&node=node_store_[frame_id];
        if(node.is_evictable()==true&&set_evictable==false)
        {
            curr_size_-=1;
        }else if (node.is_evictable()==false&&set_evictable==true) {
            curr_size_+=1;
        }
        node.set_evictable(set_evictable);
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
   if(frame_id<=0||static_cast<size_t>(frame_id)>replacer_size_)
    {
        throw std::out_of_range("SetEvictable:out of range");
    }

    if(node_store_.count(frame_id)==0)
    {
        return;
    }

    LRUKNode&node=node_store_[frame_id];
    if(node.is_evictable())
    {
       curr_size_-=1;
       node.clear_history();
       node.set_access_num(0);
       node.set_evictable(false);
    }else {
       throw std::out_of_range("Remove:not evictable");
    }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
