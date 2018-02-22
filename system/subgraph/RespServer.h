//########################################################################
//## Copyright 2018 Da Yan http://www.cs.uab.edu/yanda
//##
//## Licensed under the Apache License, Version 2.0 (the "License");
//## you may not use this file except in compliance with the License.
//## You may obtain a copy of the License at
//##
//## //http://www.apache.org/licenses/LICENSE-2.0
//##
//## Unless required by applicable law or agreed to in writing, software
//## distributed under the License is distributed on an "AS IS" BASIS,
//## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//## See the License for the specific language governing permissions and
//## limitations under the License.
//########################################################################

//this is the server of managing vcache
#ifndef RESPSERVER_H_
#define RESPSERVER_H_

//it receives batches of resps, add new comers to vcache
//- if capacity is less than limit, just insert
//- otherwise, try to replace; if cannot work, then insert (overflow); finally, try again to trim to limit
//this is repeated till no msg-batch is probed, in which case it sleeps for "WAIT_TIME_WHEN_IDLE" usec and probe again

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include <unistd.h> //for usleep()
#include <thread>
#include "adjCache.h"
#include "Comper.h"

using namespace std;

template <class ComperT>
class RespServer {
public:
	typedef RespServer<ComperT> RespServerT;

	typedef typename ComperT::TaskType TaskT;
	typedef typename ComperT::TaskMapT TaskMapT;

	typedef typename TaskT::VertexType VertexT;
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;
	typedef AdjCache<TaskT> CTable;

	CTable & cache_table;
	//thread_counter counter; //used for trim-to-vcache-limit
	thread main_thread;

	void thread_func(char * buf, int size)
	{
		//get Comper objects of all threads
		TaskMapT ** taskmap_vec = (TaskMapT **)global_taskmap_vec;
		//insert received batch
		obinstream m(buf, size);
		VertexT* v;
		while(m.end() == false)
		{
			m >> v;
			//step 1: move vertex from pcache to vcache
			vector<long long> tid_collector;
			cache_table.insert(v->id, v, tid_collector);
			//now "tid_collector" contains the ID of the tasks that requested for v
			//why use tid_collector?
			//>> pcache.erase(.) is called by vcache.insert(.), where AdjValue-constructor is called
			//>> (1) need to insert to vcache first, and then notify; (2) vcache.insert(.) triggers pcache.erase(.), erase the thread-list before vertex-insertion
			//>> Solution to above: let vcache.insert(.) return the thread list (vec.swap), and notify them in the current function
			//------
			//step 2: notify those tasks
			for(int i=0; i<tid_collector.size(); i++)
			{
				long long task_id = tid_collector[i];
				int thread_id = (task_id >> 48);
				//get the thread's Comper object
				TaskMapT* tmap = taskmap_vec[thread_id];
				tmap->update(task_id); //add the task's counter, move to conque if ready (to be fetched by Comper)
			}
		}
		/* we let GC do that now
		//try to trim to capacity-limit
		size_t oversize = global_cache_size - VCACHE_LIMIT;
		if(oversize > 0) cache_table.shrink(oversize, counter);
		*/
	}

    void run()
    {
    	bool first = true;
    	thread t;
    	//------
    	while(global_end_label == false) //otherwise, thread terminates
    	{
    		int has_msg;
    		MPI_Status status;
    		MPI_Iprobe(MPI_ANY_SOURCE, RESP_CHANNEL, MPI_COMM_WORLD, &has_msg, &status);
    		if(!has_msg) usleep(WAIT_TIME_WHEN_IDLE);
    		else
    		{
    			int size;
    			MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes)
    			char * buf = new char[size]; //space for receiving this msg-batch, space will be released by obinstream in thread_func(.)
    			MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    			if(!first) t.join(); //wait for previous CPU op to finish; t can be extended to a vector of threads later if necessary
    			t = thread(&RespServerT::thread_func, this, buf, size);
    			first = false;
    		}
    	}
    	if(!first) t.join();
    }

    RespServer(CTable & cache_tab) : cache_table(cache_tab) //get cache_table from Worker
    {
    	main_thread = thread(&RespServerT::run, this);
    }

    ~RespServer()
	{
    	main_thread.join();
	}
};

#endif
