#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>

#include "server/messages.h"
#include "server/master.h"
#include "tools/work_queue.h"

#include "tools/cycle_timer.h"
#include <iostream>

//MAX_WORKERS is just used so we have a static value for creating
//a fixed size array of worker states, but we do use the max_num_workers
//parameter in our code when deciding to launch workers
#define MAX_WORKERS 4 
#define MAX_THREADS 48

double t = 0.0;

struct Worker_state {
  bool is_alive;

  bool to_be_killed; //need to initialize to false
  int num_cpu_intense_requests;
  int num_non_intense_requests; //this would be like tellmenow

  int num_cache_intense_requests;
  int num_pending_requests; //this is the total number of pending reqs
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
  int num_to_be_killed;
  bool last_req_seen;

  std::unordered_map<int,Client_handle> tagMap;

  Worker_state worker_states[MAX_WORKERS];

} mstate;

//stores the countprimes partial results of compareprimes
struct cmp_primes_data {
  int counts[4];
  int num_received;
};

std::unordered_map<int, std::string> tagToReqMap;
std::unordered_map<int, std::string> tagToTypeMap; //the string is type we define
std::unordered_map<int, int> cmpPrimeTagToTagMap;
std::unordered_map<int, cmp_primes_data> tagToCmpPrimesDataMap;


static struct Request_cache {
  int size;
  std::unordered_map<std::string, Response_msg> respMap;
} req_cache;


void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 3;
  
  //std::cout << "\n\n\nHELLLLOjroiewhiohbttiobj43t4\n\n\n";

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;

  mstate.num_alive_workers = 0;
  mstate.num_to_be_killed = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  mstate.last_req_seen = false;
  // fire off a request for a new worker

  // initialize array of workers - bc it dont work elsewhere
  for(int i = 0; i < MAX_WORKERS; i++){
    Worker_state ws;
    ws.num_cache_intense_requests = 0;
    ws.num_cpu_intense_requests = 0;
    ws.num_non_intense_requests = 0;
    ws.num_pending_requests = 0;
    ws.to_be_killed = false;
    ws.is_alive = false;
    mstate.worker_states[i] = ws;
  }

  int tag = random();
  Request_msg req(tag);
  req.set_arg("name", "my worker 0");
  request_new_worker_node(req);

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {
  int idx;
  for(int i = 0; i < mstate.max_num_workers; i++){
    Worker_state ws = mstate.worker_states[i];
    if(!ws.is_alive){
      idx = i;
      break;
    } 
  }
  mstate.worker_states[idx].is_alive = true;

  mstate.worker_states[idx].num_cache_intense_requests = 0;
  mstate.worker_states[idx].num_cpu_intense_requests = 0;
  mstate.worker_states[idx].num_non_intense_requests = 0;
  mstate.worker_states[idx].num_pending_requests = 0;
  mstate.worker_states[idx].to_be_killed = false;
  
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
  
  //flags to make our logic work because we send compareprimes result separately
  bool send_response = true;
  bool sent_cmpprimes_resp = false;
  int tag = resp.get_tag();
  Client_handle client_handle;

  if(mstate.tagMap.find(tag) != mstate.tagMap.end()){
    client_handle = mstate.tagMap.at(tag);
  }

  //check if this response was one of the countprimes subjob for compareprimes
  if(cmpPrimeTagToTagMap.find(tag) != cmpPrimeTagToTagMap.end()){
    send_response = false;
    int parentTag = cmpPrimeTagToTagMap.at(tag);
    int idx = tag - parentTag;
    
    tagToCmpPrimesDataMap.at(parentTag).counts[idx] = atoi(resp.get_response().c_str());
    tagToCmpPrimesDataMap.at(parentTag).num_received++;
    if(tagToCmpPrimesDataMap.at(parentTag).num_received == 4){
      cmp_primes_data data = tagToCmpPrimesDataMap.at(parentTag);
      Response_msg cmpprimes_resp(parentTag);
      if(data.counts[1] - data.counts[0] > data.counts[3] - data.counts[2]){
        cmpprimes_resp.set_response("There are more primes in first range.");
      }
      else{
        cmpprimes_resp.set_response("There are more primes in second range.");
      }
      client_handle = mstate.tagMap.at(parentTag);
      send_client_response(client_handle, cmpprimes_resp);
      tagToCmpPrimesDataMap.erase(parentTag);
      send_response = true;
      sent_cmpprimes_resp = true;
      mstate.tagMap.erase(parentTag);
      tag = parentTag;
    }
    cmpPrimeTagToTagMap.erase(tag);
  }

  //this should only happen when compareprimes is not ready send response yet
  if(!send_response){
    return;
  }
  if(!sent_cmpprimes_resp){
    send_client_response(client_handle, resp);
    mstate.tagMap.erase(tag);
  }
  mstate.num_pending_client_requests--;


  if(tagToReqMap.find(tag) != tagToReqMap.end()){
    std::string req_string = tagToReqMap.at(tag);
    req_cache.respMap.insert(std::pair<std::string, Response_msg>(req_string, resp));
    req_cache.size++;
  }

  //search for the worker
  for(int i = 0; i < mstate.max_num_workers; i++){
    Worker_state ws = mstate.worker_states[i];
    if(ws.is_alive && ws.worker_handle == worker_handle){
      ws.num_pending_requests--;
      //by the way we set this up, every request should have one of these 3 types
      std::string type = tagToTypeMap.at(tag);
      if(type == "cache"){
        ws.num_cache_intense_requests--;
      }  
      else if(type == "cpu"){
        ws.num_cpu_intense_requests--;
      }
      else if(type == "non"){
        ws.num_non_intense_requests--;
      }
      tagToTypeMap.erase(tag);


      //summing up manually because ws.num_pending_requests is wrong
      int totalRequests = ws.num_cache_intense_requests + ws.num_cpu_intense_requests + ws.num_non_intense_requests;
      //kill the worker if it's been flagged and it's done with work
      if(ws.to_be_killed && totalRequests == 0){

        std::cout << "\n---------------KILLING A WORKER-----------------------\n";
        std::cout << "Current tag and timestamp " << mstate.next_tag << "\n";

/*
        t = CycleTimer::currentSeconds(); 
        std::cout << "\n---------------KILLING ALL THE WORKERS-----------------------\n";
        std::cout << "num_pending_client_requests: " << mstate.num_pending_client_requests << "\n";
        std::cout << "Current tag and timestamp " << mstate.next_tag << "\n";
        std::cout << "Time: " << t << "\n";
        std::cout << "Alive workers: " << mstate.num_alive_workers << "\nActually alive: " << mstate.num_alive_workers - mstate.num_to_be_killed << "\nTo be killed: " << mstate.num_to_be_killed << "\n";
        for(int i = 0; i < mstate.max_num_workers; i++){
          if(mstate.worker_states[i].is_alive){
            Worker_state ws = mstate.worker_states[i];
            std::cout << "Worker number: "<< i << "\nCache count: " << ws.num_cache_intense_requests << "\nCPU count: " << ws.num_cpu_intense_requests << "\nOther Count: " << ws.num_non_intense_requests << "\n TOTAL count: " << ws.num_pending_requests << "\n";
            std::cout << "is_alive " << ws.is_alive << "\nto_be_killed " << ws.to_be_killed << "\n";
            }
        }

        std::cout << "\nKILLING THEM ALL NOW\n";
        std::cout << "\n------------------------------------------------------\n\n";
*/
        // update node to indicate done
        ws.to_be_killed = false;
        ws.is_alive = false;
//	std::cout << "\n\nKILLING WORKER HANDLE: " << ws.worker_handle << "\n\n";
        kill_worker_node(ws.worker_handle);
        mstate.num_alive_workers--;
        mstate.num_to_be_killed--;
      }

      mstate.worker_states[i] = ws;
      break;
    }
  }

  //this should be the end
  if(mstate.last_req_seen && mstate.num_pending_client_requests == 0){
    for(int i = 0; i < mstate.max_num_workers; i++){
      //we're not setting is_alive to false because we should be done at this point
      
	//std::cout << "KILLING THEM ALLLLLLLLLLL";
if(mstate.worker_states[i].is_alive){
	//std::cout << "\n\nKILLING WORKER HANDLE " << mstate.worker_states[i].worker_handle << "\n\n";
        kill_worker_node(mstate.worker_states[i].worker_handle);
        mstate.num_to_be_killed--;
      }
    }
  }

}

int find_min_cache_idx(){
  int curr_min;
  int selected_idx;
  bool has_begun = false;
  for(int i = 0; i < mstate.max_num_workers; i++){
    Worker_state ws = mstate.worker_states[i];
    if(!ws.is_alive || ws.to_be_killed){
      continue;
    }
    if(ws.is_alive && !has_begun){
      curr_min = ws.num_cache_intense_requests;
      selected_idx = i;
      has_begun = true;
    }
    else if(ws.is_alive){
      if(ws.num_cache_intense_requests < curr_min){
        curr_min = ws.num_cache_intense_requests;
        selected_idx = i;
      }
    }
  }
  return selected_idx;
}
int find_min_non_idx(){
  int curr_min;
  int selected_idx;
  bool has_begun = false;
  for(int i = 0; i < mstate.max_num_workers; i++){
    Worker_state ws = mstate.worker_states[i];
    if(!ws.is_alive || ws.to_be_killed){
      continue;
    }
    if(ws.is_alive && !has_begun){
      curr_min = ws.num_non_intense_requests;
      selected_idx = i;
      has_begun = true;
    }
    else if(ws.is_alive){
      if(ws.num_non_intense_requests < curr_min){
        curr_min = ws.num_non_intense_requests;
        selected_idx = i;
      }
    }
  }
  return selected_idx;
}
int find_min_cpu_idx(){
  int curr_min;
  bool has_begun = false;
  int selected_idx;
  for(int i = 0; i < mstate.max_num_workers; i++){
    Worker_state ws = mstate.worker_states[i];
    if(!ws.is_alive || ws.to_be_killed){
      continue;
    }
    if(ws.is_alive && !has_begun){
      curr_min = ws.num_cpu_intense_requests;
      selected_idx = i;
      has_begun = true;
    }
    else if(ws.is_alive){
      if(ws.num_cpu_intense_requests < curr_min){
        curr_min = ws.num_cpu_intense_requests;
        selected_idx = i;
      }
    }
  }
  return selected_idx;
}

int find_min_load_idx(){
  int curr_min;
  bool has_begun = false;
  int selected_idx;
  for(int i = 0; i < mstate.max_num_workers; i++){
    Worker_state ws = mstate.worker_states[i];
    if(!ws.is_alive || ws.to_be_killed){
      continue;
    }
    if(ws.is_alive && !has_begun){
      curr_min = ws.num_cpu_intense_requests + ws.num_cache_intense_requests;
      selected_idx = i;
      has_begun = true;
    }
    else if(ws.is_alive){
      if(ws.num_cpu_intense_requests + ws.num_cache_intense_requests < curr_min){
        curr_min = ws.num_cpu_intense_requests + ws.num_cache_intense_requests;
        selected_idx = i;
      }
    }
  }
  return selected_idx;
}
int choose_worker_idx(int tag){
  int curr_min;
  bool has_begun = false;
  int selected_idx;

  std::string type = tagToTypeMap.at(tag);

  if(type == "cache"){
    return find_min_cache_idx();
  }
  else if(type == "cpu"){
    return find_min_cpu_idx();
  }
  else if(type == "non"){
    return find_min_non_idx();
  }
  //shouldn't get here
  else{
    return -1;
  }

  return selected_idx;
}

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  bool is_cache_intense = false; 
  bool is_cpu_intense = false;
  bool not_intense = false;
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
  tagToReqMap.insert(std::pair<int, std::string>(mstate.next_tag, req_string)); 

  mstate.num_pending_client_requests++;

  mstate.tagMap.insert(std::pair<int,Client_handle>(mstate.next_tag, client_handle));
  int tag = mstate.next_tag;
  
  std::string req_name = client_req.get_arg("cmd");
  if(req_name == "compareprimes"){
    tagToTypeMap.insert(std::pair<int,std::string>(tag, "cpu"));
    int idx = choose_worker_idx(tag);

    int parentTag = tag;
    cmp_primes_data data;
    data.num_received = 0;

    mstate.worker_states[idx].num_cpu_intense_requests++;
    mstate.worker_states[idx].num_pending_requests++;

    tagToCmpPrimesDataMap.insert(std::pair<int,cmp_primes_data>(parentTag, data));
    int params[4];
    params[0] = atoi(client_req.get_arg("n1").c_str());
    params[1] = atoi(client_req.get_arg("n2").c_str());
    params[2] = atoi(client_req.get_arg("n3").c_str());
    params[3] = atoi(client_req.get_arg("n4").c_str());
    
    for(int i = 0; i < 4; i++){
      cmpPrimeTagToTagMap.insert(std::pair<int, int>(tag, parentTag));
      Request_msg dummy_req(0);
      create_computeprimes_req(dummy_req, params[i]);
      Request_msg worker_cmpprimes_req(tag++, dummy_req);
      mstate.next_tag++;
      
      Worker_state ws = mstate.worker_states[idx];
      send_request_to_worker(ws.worker_handle, worker_cmpprimes_req);
    }

    return;
  }
  else if(req_name == "projectidea"){
    tagToTypeMap.insert(std::pair<int,std::string>(tag, "cache"));
    is_cache_intense = true;
  } 
  //we're going to fix this, but right now our map assumes everything maps
  //to one of the three types of requests
  else if(req_name == "tellmenow"){
    tagToTypeMap.insert(std::pair<int,std::string>(tag, "non"));
    not_intense = true;
  }
  //countprimes 418wisdom - debating whether or not we want catch-all case 
  else{
    tagToTypeMap.insert(std::pair<int,std::string>(tag, "cpu"));
    is_cpu_intense = true;
  }
  
  Request_msg worker_req(tag, client_req);
  int idx = choose_worker_idx(tag);
  Worker_state ws = mstate.worker_states[idx];

  if(is_cache_intense){
    mstate.worker_states[idx].num_cache_intense_requests++;
  }
  else if(is_cpu_intense){
    mstate.worker_states[idx].num_cpu_intense_requests++;
  }
  else if(not_intense){
    mstate.worker_states[idx].num_non_intense_requests++;
  }
  mstate.worker_states[idx].num_pending_requests++;
  mstate.next_tag++;
  send_request_to_worker(ws.worker_handle, worker_req);
}


void handle_tick() {

        t = CycleTimer::currentSeconds(); 
        std::cout << "\n--------------HANDLE TICK-----------------------\n";
        std::cout << "num_pending_client_requests: " << mstate.num_pending_client_requests << "\n";
        std::cout << "Current tag and timestamp " << mstate.next_tag << "\n";
        std::cout << "Time: " << t << "\n";
        std::cout << "Alive workers: " << mstate.num_alive_workers << "\nActually alive: " << mstate.num_alive_workers - mstate.num_to_be_killed << "\nTo be killed: " << mstate.num_to_be_killed << "\n";
        for(int i = 0; i < mstate.max_num_workers; i++){
          if(mstate.worker_states[i].is_alive){
            Worker_state ws = mstate.worker_states[i];
            std::cout << "Worker number: "<< i << "\nCache count: " << ws.num_cache_intense_requests << "\nCPU count: " << ws.num_cpu_intense_requests << "\nOther Count: " << ws.num_non_intense_requests << "\n TOTAL count: " << ws.num_pending_requests << "\n";
            std::cout << "is_alive " << ws.is_alive << "\nto_be_killed " << ws.to_be_killed << "\n";
            }
        }

        std::cout << "\n------------------------------------------------------\n\n";

  int num_cpu = 0;
  int num_cache = 0;

  for(int i = 0; i < mstate.max_num_workers; i++){
    Worker_state ws = mstate.worker_states[i];
    if(ws.is_alive && !ws.to_be_killed){
      num_cpu += ws.num_cpu_intense_requests;
      num_cache += ws.num_cache_intense_requests;
    }
  }  
  // fast requests like tellmenow are unweighted
  int weighted_total = num_cpu + 3 * num_cache;

  int num_actually_alive = mstate.num_alive_workers - mstate.num_to_be_killed;
  //policy to add more worker nodes
  if(num_actually_alive < mstate.max_num_workers){
    int avg_cpu_intense_work = num_cpu / num_actually_alive;
    int avg_cache_intense_work = num_cache / num_actually_alive;

    int avg_work_per_node = weighted_total / num_actually_alive;
    
    if(avg_cpu_intense_work > MAX_THREADS / 2 || avg_cache_intense_work > 1){
 
        std::cout << "\n---------------ADDING A NEW WORKER-----------------------\n";
        std::cout << "Current tag and timestamp " << mstate.next_tag << "\n";

    /* 
        t = CycleTimer::currentSeconds(); 
        std::cout << "\n---------------ADDING A NEW WORKER-----------------------\n";
        
	std::cout << "Weighted Total: " << weighted_total << "\nAverage Work: " << avg_work_per_node << "\n";
	std::cout << "num_pending_client_requests: " << mstate.num_pending_client_requests << "\n";
        std::cout << "Current tag and timestamp " << mstate.next_tag << "\n";
        std::cout << "Time: " << t << "\n";
        std::cout << "Alive workers: " << mstate.num_alive_workers << "\nActually alive: " << mstate.num_alive_workers - mstate.num_to_be_killed << "\nTo be killed: " << mstate.num_to_be_killed << "\n";
        for(int i = 0; i < mstate.max_num_workers; i++){
          if(mstate.worker_states[i].is_alive){
            Worker_state ws = mstate.worker_states[i];
            std::cout << "Worker number: "<< i << "\nCache count: " << ws.num_cache_intense_requests << "\nCPU count: " << ws.num_cpu_intense_requests << "\nOther Count: " << ws.num_non_intense_requests << "\n TOTAL count: " << ws.num_pending_requests << "\n";
            std::cout << "is_alive " << ws.is_alive << "\nto_be_killed " << ws.to_be_killed << "\n";
            }
        }

        std::cout << "\nADDING IT NOW\n";
        std::cout <<"\n------------------------------------------------------\n\n";
      */
      if(mstate.num_to_be_killed == 0 && mstate.num_alive_workers < mstate.max_num_workers){
        //std::cout << "ACTUALLY REQUESTING NEW WORKER\n\n";


        int tag = random();
        Request_msg req(tag);
        req.set_arg("name", "my worker 0");
        request_new_worker_node(req);
      }
      else{
        for(int i = 0; i < mstate.max_num_workers; i++){
          Worker_state ws = mstate.worker_states[i];
          if(ws.is_alive && ws.to_be_killed){
            
           //std::cout << "RESETTING THE FLAGG NOWWW\n\n"; 
            
            mstate.worker_states[i].to_be_killed = false;
            mstate.num_to_be_killed--;
            num_actually_alive++;
            break;
          }
        }
      }
    }
  }

  //policy to remove worker nodes only if there are more than one nodes
  if(num_actually_alive > 1){
    //what's the average work per node if you did take away a worker
    int avg_work_per_node = weighted_total / (num_actually_alive - 1);
    if(avg_work_per_node < MAX_THREADS - 2){
      
      std::cout << "\n---------------SETTING TO BE KILLED FLAG-----------------------\n";
      std::cout << "Current tag and timestamp " << mstate.next_tag << "\n";

     /*
      t = CycleTimer::currentSeconds(); 
      std::cout << "\n---------------SETTING TO BE KILLED FLAG-----------------------\n";
      std::cout << "num+pending_client+requests" << mstate.num_pending_client_requests << "\n";
      std::cout << "Current tag and timestamp " << mstate.next_tag << "\n";
      std::cout << "Time: " << t << "\n";
      std::cout << "Alive workers: " << mstate.num_alive_workers << "\nActually alive: " << mstate.num_alive_workers - mstate.num_to_be_killed << "\nTo be killed: " << mstate.num_to_be_killed << "\n";
      for(int i = 0; i < mstate.max_num_workers; i++){
        if(mstate.worker_states[i].is_alive){
          Worker_state ws = mstate.worker_states[i];
          std::cout << "Worker number: "<< i << "\nCache count: " << ws.num_cache_intense_requests << "\nCPU count: " << ws.num_cpu_intense_requests << "\nOther Count: " << ws.num_non_intense_requests << "\n TOTAL count: " << ws.num_pending_requests << "\n";
          std::cout << "is_alive " << ws.is_alive << "\nto_be_killed " << ws.to_be_killed << "\n";
          }
      }

      std::cout << "\nSETTING IT NOW\n";
      std::cout << "\n------------------------------------------------------\n\n";
      */
      int idx = find_min_load_idx();
      mstate.worker_states[idx].to_be_killed = true;
      mstate.num_to_be_killed++;
    }
  }
}

