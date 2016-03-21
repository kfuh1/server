#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>

#include "server/messages.h"
#include "server/master.h"
#include "tools/work_queue.h"

#define MAX_WORKERS 4
#define MAX_THREADS 24


struct Worker_state {
  bool is_alive;
  int num_pending_requests;
  Worker_handle worker_handle;
};

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int num_pending_client_requests;
  int next_tag;
  int num_alive_workers;

  int queue_size;
  bool last_req_seen;

  WorkQueue<Request_msg> reqQueue;
  std::unordered_map<int,Client_handle> tagMap;

  Worker_state worker_states[MAX_WORKERS];

} mstate;

std::unordered_map<int, std::string> tagToReqMap;

static struct Request_cache {
  int size;
  std::unordered_map<std::string, Response_msg> respMap;
} req_cache;

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;

  mstate.num_alive_workers = 0;

  mstate.queue_size = 0;
  mstate.reqQueue = WorkQueue<Request_msg>();
  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  mstate.last_req_seen = false;
  // fire off a request for a new worker

  // initialize array of workers - bc it dont work elsewhere
  for(int i = 0; i < MAX_WORKERS; i++){
    Worker_state ws;
  
    ws.num_pending_requests = 0;
    ws.is_alive = false;
    mstate.worker_states[i] = ws;
  }

  int tag = random();
  Request_msg req(tag);
  req.set_arg("name", "my worker 0");
  request_new_worker_node(req);

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {
  int idx = mstate.num_alive_workers;
  mstate.worker_states[idx].is_alive = true;

  mstate.worker_states[idx].worker_handle = worker_handle;
  mstate.num_alive_workers++;

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  int tag = resp.get_tag();
  Client_handle client_handle = mstate.tagMap.at(tag);
  send_client_response(client_handle, resp);
  mstate.num_pending_client_requests--;
  mstate.tagMap.erase(tag);

  //at shouldn't throw exception here because we already added tag to map
  std::string req_string = tagToReqMap.at(tag);
  req_cache.respMap.insert(std::pair<std::string, Response_msg>(req_string, resp));
  req_cache.size++;

  //search for the worker
  for(int i = 0; i < MAX_WORKERS; i++){
    Worker_state ws = mstate.worker_states[i];
    if(ws.worker_handle == worker_handle){
      ws.num_pending_requests--;
      break;
    }
  }

  //TODO: fix parameter to kill_worker_node when we get more workers
  if(mstate.last_req_seen && mstate.num_pending_client_requests == 0){
    for(int i = 0; i < MAX_WORKERS; i++){
      //we're not setting is_alive to false because we should be done at this point
      if(mstate.worker_states[i].is_alive){
        kill_worker_node(mstate.worker_states[i].worker_handle);
      }
    }
  }

}

int choose_worker_idx(){
  int curr_min;
  bool has_begun = false;
  int selected_idx;

  for(int i = 0; i < MAX_WORKERS; i++){
    Worker_state ws = mstate.worker_states[i];
    if(ws.is_alive && !has_begun){
      curr_min = ws.num_pending_requests;
      selected_idx = i;
      has_begun = true;
    }
    else if(ws.is_alive){
      if(ws.num_pending_requests < curr_min){
        curr_min = ws.num_pending_requests;
        selected_idx = i;
      }
    }
  }
  return selected_idx;
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    mstate.last_req_seen = true;
    return;
  }

  //Search for response in cache
  std::string req_string = client_req.get_request_string();
  if(req_cache.respMap.find(req_string) != req_cache.respMap.end()){
    send_client_response(client_handle, req_cache.respMap.at(req_string));
    return;
  }

  mstate.tagMap.insert(std::pair<int,Client_handle>(mstate.next_tag, client_handle));
  int tag = mstate.next_tag++;
  tagToReqMap.insert(std::pair<int, std::string>(tag, req_string)); 
  Request_msg worker_req(tag, client_req);
  mstate.reqQueue.put_work(worker_req);
  mstate.queue_size++;
  mstate.num_pending_client_requests++;
  //printf("\n");
  while(mstate.queue_size > 0){
    int idx = choose_worker_idx();
    Worker_state ws = mstate.worker_states[idx];
    ws.num_pending_requests++;
    send_request_to_worker(ws.worker_handle, mstate.reqQueue.get_work());
    mstate.queue_size--;
  }


}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.
  if(mstate.num_alive_workers < MAX_WORKERS){
  bool all_workers_too_loaded = true;
  for(int i = 0; i < MAX_WORKERS; i++){
    Worker_state ws = mstate.worker_states[i];
    if(ws.is_alive){
      all_workers_too_loaded &= (ws.num_pending_requests > MAX_THREADS);
    }
  }
  if(all_workers_too_loaded){
    int tag = random();
    Request_msg req(tag);
    req.set_arg("name", "my worker 0");
    request_new_worker_node(req);
  }
  }

}

