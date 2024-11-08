// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_THREAD_POOL_H_
#define NET_INCLUDE_THREAD_POOL_H_

#include <pthread.h>
#include <atomic>
#include <queue>
#include <string>

#include "net/include/net_define.h"
#include "pstd/include/pstd_mutex.h"

namespace net {

using TaskFunc = void (*)(void *);

struct Task {
  Task() = default;
  TaskFunc func = nullptr;
  void* arg = nullptr;
  Task(TaskFunc _func, void* _arg) : func(_func), arg(_arg) {}
};

struct TimeTask {
  uint64_t exec_time;
  TaskFunc func;
  void* arg;
  TimeTask(uint64_t _exec_time, TaskFunc _func, void* _arg) : exec_time(_exec_time), func(_func), arg(_arg) {}
  bool operator<(const TimeTask& task) const { return exec_time > task.exec_time; }
};

class ThreadPool : public pstd::noncopyable {
 public:
  class Worker {
   public:
    explicit Worker(ThreadPool* tp, size_t size) : start_(false), thread_pool_(tp), should_stop_(false), max_queue_size_(size) {};
    static void* WorkerMain(void* arg);

    int start();
    int stop();
    void Schedule(TaskFunc func, void* arg);
    size_t cur_queue_size();
    size_t cur_time_queue_size();


   private:
    void runInThread();
    size_t max_queue_size();
    void DelaySchedule(uint64_t timeout, TaskFunc func, void* arg);
    bool should_stop();
    void set_should_stop();

    pthread_t thread_id_;
    std::atomic<bool> start_;
    ThreadPool* const thread_pool_;
    std::string worker_name_;
    std::queue<Task> queue_;
    std::priority_queue<TimeTask> time_queue_;
    pstd::Mutex mu_;
    pstd::CondVar rsignal_;
    pstd::CondVar wsignal_;
    std::atomic<bool> should_stop_;
    size_t max_queue_size_; 
  };

  explicit ThreadPool(size_t worker_num, size_t max_queue_size, std::string  thread_pool_name = "ThreadPool");
  virtual ~ThreadPool();
  void Schedule(TaskFunc func, void* arg);
  int start_thread_pool();
  int stop_thread_pool();
  size_t max_queue_size();
  size_t worker_size();
  std::string thread_pool_name();
  void cur_queue_size(size_t* qsize);
  void cur_time_queue_size(size_t* qsize);

 private:
  /*
   * Here we used auto poll to find the next work thread,
   * last_thread_ is the last work thread
   */
  int last_thread_;
  size_t worker_num_;
  size_t max_queue_size_;

  std::string thread_pool_name_;
  std::vector<Worker*> workers_;
  std::atomic<bool> running_;
};

}  // namespace net

#endif  // NET_INCLUDE_THREAD_POOL_H_
