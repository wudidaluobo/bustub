//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <optional>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&]
  
   { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  std::optional<DiskRequest> element;
  element.emplace(std::move(r));
  request_queue_.Put(std::move(element));
}

void DiskScheduler::StartWorkerThread() {
  while(1)
  {
    std::optional<DiskRequest>element=request_queue_.Get();
    if(element==std::nullopt)
    {
      break;
    }
    DiskRequest&r=element.value();
    if(r.is_write_)
    {
      disk_manager_->WritePage(r.page_id_,r.data_);
    }else{
      disk_manager_->ReadPage(r.page_id_,r.data_);
    }
    r.callback_.set_value(true);
    
  }
}

}  // namespace bustub
