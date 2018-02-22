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

//this is the server of key-value store
#ifndef REQSERVER_H_
#define REQSERVER_H_

//it receives batches of reqs, looks up local_table for vertex objects, and responds
//this is repeated till no msg-batch is probed, in which case it sleeps for "WAIT_TIME_WHEN_IDLE" usec and probe again

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include <unistd.h> //for usleep()
#include <thread>
#include "RespQueue.h"
using namespace std;

template <class VertexT>
class ReqServer {
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;
	typedef hash_map<KeyT, VertexT*> VTable;

	VTable & local_table;
	RespQueue<VertexT> q_resp; //will create _num_workers responding threads
	thread main_thread;

	//*//unsafe version, users need to guarantee that adj-list vertex-item must exist
	//faster
	void thread_func(char * buf, int size, int src)
	{
		obinstream m(buf, size);
		KeyT vid;
		while(m.end() == false)
		{
			m >> vid;
			VertexT * v = local_table[vid];
			q_resp.add(local_table[vid], src);
		}
	}
	//*/

	/*//safe version
	void thread_func(char * buf, int size, int src)
	{
		obinstream m(buf, size);
		KeyT vid;
		while(m.end() == false)
		{
			m >> vid;
			auto it = local_table.find(vid);
			if(it == local_table.end())
			{
				cout<<_my_rank<<": [ERROR] Vertex "<<vid<<" does not exist but is requested"<<endl;
				MPI_Abort(MPI_COMM_WORLD, -1);
			}
			q_resp.add(it->second, src);
		}
	}
	//*/

    void run() //called after graph is loaded and local_table is set
    {
    	bool first = true;
    	thread t;
    	//------
    	while(global_end_label == false) //otherwise, thread terminates
    	{
    		int has_msg;
    		MPI_Status status;
    		MPI_Iprobe(MPI_ANY_SOURCE, REQ_CHANNEL, MPI_COMM_WORLD, &has_msg, &status);
    		if(!has_msg) usleep(WAIT_TIME_WHEN_IDLE);
    		else
    		{
    			int size;
    			MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes)
    			char * buf = new char[size]; //space for receiving this msg-batch, space will be released by obinstream in thread_func(.)
    			MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    			if(!first) t.join(); //wait for previous CPU op to finish; t can be extended to a vector of threads later if necessary
    			t = thread(&ReqServer<VertexT>::thread_func, this, buf, size, status.MPI_SOURCE); //insert to q_resp[status.MPI_SOURCE]
    			first = false;
    		}
    	}
    	if(!first) t.join();
    }

    ReqServer(VTable & loc_table) : local_table(loc_table) //get local_table from Worker
    {
    	main_thread = thread(&ReqServer<VertexT>::run, this);
    }

    ~ReqServer()
    {
    	main_thread.join();
    }
};

#endif
