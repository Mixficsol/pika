// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/thread_pool.h"
#include "net/src/net_thread_name.h"

#include <sys/time.h>

#include <string>
#include <utility>

namespace net {

void* ThreadPool::Worker::WorkerMain(void* arg) {
  auto tp = static_cast<Worker*>(arg);
  tp->runInThread();
  return nullptr;
}

int ThreadPool::Worker::start() {
  if (!start_.load()) {
    if (pthread_create(&thread_id_, nullptr, &WorkerMain, thread_pool_) != 0) {
      return -1;
    } else {
      start_.store(true);
      std::string thread_id_str = std::to_string(reinterpret_cast<unsigned long>(thread_id_));
      SetThreadName(thread_id_, thread_pool_->thread_pool_name() + "_Worker_" + thread_id_str);
    }
  }
  return 0;
}

int ThreadPool::Worker::stop() {
  should_stop_.store(true);
  rsignal_.notify_all();
  wsignal_.notify_all();
  if (start_.load()) {
    if (pthread_join(thread_id_, nullptr) != 0) {
      return -1;
    } else {
      start_.store(false);
    }
  }
  return 0;
}

ThreadPool::ThreadPool(size_t worker_num, size_t max_queue_size, std::string thread_pool_name)
    : worker_num_(worker_num),
      max_queue_size_(max_queue_size),
      thread_pool_name_(std::move(thread_pool_name)),
      running_(false),
      last_thread_(0) {}

ThreadPool::~ThreadPool() { stop_thread_pool(); }

int ThreadPool::start_thread_pool() {
  if (!running_.load()) {
    for (size_t i = 0; i < worker_num_; ++i) {
      workers_.push_back(new Worker(this, max_queue_size_));
      int res = workers_[i]->start();
      if (res != 0) {
        return kCreateThreadError;
      }
    }
    running_.store(true);
  }
  return kSuccess;
}

int ThreadPool::stop_thread_pool() {
  int res = 0;
  if (running_.load()) {
    for (const auto worker : workers_) {
      res = worker->stop();
      if (res != 0) {
        break;
      } else {
        delete worker;
      }
    }
    workers_.clear();
    running_.store(false);
  }
  return res;
}

bool ThreadPool::Worker::should_stop() { return should_stop_.load(); }

void ThreadPool::Worker::set_should_stop() { should_stop_.store(true); }

void ThreadPool::Worker::Schedule(TaskFunc func, void* arg) {
  std::lock_guard lock(mu_);
  //wsignal_.wait(lock, [worker]() { return queue_.size() < thread_pool_->max_queue_size_ || thread_pool_->should_stop(); });
  if (queue_.size() < max_queue_size_ && !should_stop()) {
    queue_.emplace(func, arg);
    rsignal_.notify_one();
  }
}

void ThreadPool::Schedule(TaskFunc func, void* arg) {
  int next_thread = last_thread_;
  bool find = false;
  for (int cnt = 0; cnt < worker_num_; cnt++) {
    Worker* worker = workers_[next_thread];
    worker->Schedule(func, arg);
    next_thread = (next_thread + 1) % worker_num_;
  }
}

/*
 * timeout is in millisecond
 */
void ThreadPool::Worker::DelaySchedule(uint64_t timeout, TaskFunc func, void* arg) {
  auto now = std::chrono::system_clock::now();
  uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
  uint64_t exec_time = unow + timeout * 1000;

  std::lock_guard lock(mu_);
  if (!should_stop()) {
    time_queue_.emplace(exec_time, func, arg);
    rsignal_.notify_all();
  }
}

size_t ThreadPool::Worker::max_queue_size() { return max_queue_size_; }

void ThreadPool::Worker::cur_queue_size(size_t* qsize) {
  std::lock_guard lock(mu_);
  *qsize = queue_.size();
}

void ThreadPool::Worker::cur_time_queue_size(size_t* qsize) {
  std::lock_guard lock(mu_);
  *qsize = time_queue_.size();
}

size_t ThreadPool::max_queue_size() { return max_queue_size_; }
void ThreadPool::cur_queue_size(size_t* qsize) {
  *qsize = 10;
}

void ThreadPool::cur_time_queue_size(size_t* qsize) {
  *qsize = 10;
}

std::string ThreadPool::thread_pool_name() { return thread_pool_name_; }

void ThreadPool::Worker::runInThread() {
  while (!should_stop()) {
    std::unique_lock lock(mu_);
    rsignal_.wait(lock, [this]() { return !queue_.empty() || !time_queue_.empty() || should_stop(); });

    if (should_stop()) {
      break;
    }
    if (!time_queue_.empty()) {
      auto now = std::chrono::system_clock::now();
      uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

      auto [exec_time, func, arg] = time_queue_.top();
      if (unow  >= exec_time) {
        time_queue_.pop();
        lock.unlock();
        (*func)(arg);
        continue;
      } else if (queue_.empty() && !should_stop()) {
        rsignal_.wait_for(lock, std::chrono::microseconds(exec_time - unow));
        lock.unlock();
        continue;
      }
    }

    if (!queue_.empty()) {
      auto [func, arg] = queue_.front();
      queue_.pop();
      wsignal_.notify_one();
      lock.unlock();
      (*func)(arg);
    }
  }
}
}  // namespace net
