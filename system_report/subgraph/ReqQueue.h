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

#ifndef REQQUEUE_H_
#define REQQUEUE_H_

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include "util/conque.h"
#include <atomic>
#include <thread>
#include <unistd.h> //for usleep()
using namespace std;

template <class VertexT>
class ReqQueue {
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    typedef conque<KeyT> Buffer;
    typedef vector<Buffer> Queue;

    Queue q;
    vector<thread> threads; //each thread handles one queue (to one worker), other than self

    void thread_func(int tgt_rank) //managing requests to tgt_rank
    {
    	while(global_end_label == false) //otherwise, thread terminates
    	{
    		clock_t last_tick = clock();
    		//just wake up, see whether there's any req to send
    		Buffer & buf = q[tgt_rank];
    		KeyT temp; //for fetching KeyT items
    		ibinstream m;
    		while(buf.dequeue(temp)) m << temp; //fetching till reach list-head
    		if(m.size() > 0)
    		{
    			//send reqs to tgt
    			MPI_Send(m.get_buf(), m.size(), MPI_CHAR, tgt_rank, REQ_CHANNEL, MPI_COMM_WORLD);
    		}
    		//------------------------
    		clock_t time_passed = clock() - last_tick; //the processing time above
    		clock_t gap = polling_ticks - time_passed; //remaining time before next polling
    		if(gap > 0) usleep(gap * 1000000 / CLOCKS_PER_SEC);
    	}
    }

    ReqQueue()
    {
    	q.resize(_num_workers);
    	threads.resize(_num_workers);
    	for(int tgt=0; tgt<_num_workers; tgt++)
    	{
    		if(tgt != _my_rank) threads[tgt] = thread(&ReqQueue<VertexT>::thread_func, this, tgt);
    	}
    }

    ~ReqQueue()
    {
    	for(int tgt=0; tgt<_num_workers; tgt++)
		{
			if(tgt != _my_rank) threads[tgt].join();
		}
    }

    HashT hash;

    void add(KeyT vid)
    {
    	int tgt = hash(vid);
    	Buffer & buf = q[tgt];
    	buf.enqueue(vid);
    }
};

#endif
