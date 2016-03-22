
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>
#include <pthread.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "tools/work_queue.h"

#define MAX_THREADS 24


pthread_mutex_t queue_mutex;

static struct Worker_state {
  WorkQueue<Request_msg> reqQueue;
  WorkQueue<Request_msg> cacheIntenseQueue;
  int num_cache_intense_requests;
  int num_requests;
  int running_cache_intensive;
} wstate;


// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// Implements logic required by compareprimes command via multiple
// calls to execute_work.  This function fills in the appropriate
// response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      Request_msg dummy_req(0);
      Response_msg dummy_resp(0);
      create_computeprimes_req(dummy_req, params[i]);
      execute_work(dummy_req, dummy_resp);
      counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}

void* thread_start(void* args){
  while(1){
    bool hasJob = false;
    bool hasCacheIntenseJob = false;;
    Request_msg req;
    pthread_mutex_lock(&queue_mutex);

    if(wstate.num_cache_intense_requests > 0 && !wstate.running_cache_intensive){
      wstate.running_cache_intensive = true;
      wstate.num_cache_intense_requests--;
      req = wstate.cacheIntenseQueue.get_work();
      hasJob = true;
      hasCacheIntenseJob = true;
      //need to fix this so you can reset runnning_cache_intenseive to false
    }
    else if(wstate.num_requests > 0){
      req = wstate.reqQueue.get_work();
      wstate.num_requests--;
      hasJob = true;
    }
    pthread_mutex_unlock(&queue_mutex);

    if (hasJob) {
      Response_msg resp(req.get_tag());
      if (req.get_arg("cmd").compare("compareprimes") == 0) {
        // The compareprimes command needs to be special cased since it is
        // built on four calls to execute_execute work.  All other
        // requests from the client are one-to-one with calls to  execute_work.
        execute_compareprimes(req, resp);
      } else {
        // actually perform the work.  The response string is filled in by
        // 'execute_work'
        execute_work(req, resp);
      }
      worker_send_response(resp);
      if(hasCacheIntenseJob){
        wstate.running_cache_intensive = false;
      }
    }
  }
  return NULL;
}

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";
  wstate.reqQueue = WorkQueue<Request_msg>();  
  wstate.cacheIntenseQueue = WorkQueue<Request_msg>();
  wstate.num_requests = 0;
  wstate.num_cache_intense_requests = 0;
  wstate.running_cache_intensive = false;


  pthread_mutex_init(&queue_mutex, NULL);
  pthread_t workers[MAX_THREADS];
  // spawn 23 threads that will be pinned down to specific execution contexts
  // use 23 because 24 execution contexts total and we have a main thread
  for(int i = 0; i < MAX_THREADS - 1; i++){
    pthread_create(&workers[i], NULL, thread_start, NULL);
  }
}

void worker_handle_request(const Request_msg& req) {
  // Make the tag of the reponse match the tag of the request.  This
  // is a way for your master to match worker responses to requests.


  // Enqueue into correct queue based on type of job
  if (req.get_arg("cmd").compare("projectidea") == 0) {
    wstate.cacheIntenseQueue.put_work(req);
    wstate.num_cache_intense_requests++;
  }
  else{
    wstate.reqQueue.put_work(req);
    wstate.num_requests++;
  }
  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  
  /*
  double startTime = CycleTimer::currentSeconds();

    // The compareprimes command needs to be special cased since it is
    // built on four calls to execute_execute work.  All other
    // requests from the client are one-to-one with calls to
    // execute_work.


    // actually perform the work.  The response string is filled in by
    // 'execute_work'


 
  */
}
