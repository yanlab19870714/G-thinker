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

//this is the garbage collector of vcache:
//- erase unlocked vertices from vcache when vcache overflows
#ifndef GC_H_
#define GC_H_

#include "util/global.h"
#include "adjCache.h"
#include <unistd.h> //for usleep()
#include <thread>
using namespace std;

template <class TaskT>
class GC {
public:
	typedef typename TaskT::VertexType VertexT;
	typedef typename VertexT::KeyType KeyT;
	typedef AdjCache<TaskT> CTable;

	CTable & cache_table;
	thread main_thread;
	thread_counter counter;

    void run() //called after graph is loaded and local_table is set
    {
    	while(global_end_label == false) //otherwise, thread terminates
    	{
    		int oversize = global_cache_size - VCACHE_LIMIT;
    		if(oversize > 0) cache_table.shrink(oversize, counter);
    		usleep(WAIT_TIME_WHEN_IDLE); //polling frequency
    	}
    }

    GC(CTable & cache_tab) : cache_table(cache_tab) //get cache_table from Worker
    {
    	main_thread = thread(&GC<TaskT>::run, this);
    }

    ~GC()
    {
    	main_thread.join();
    }
};

#endif
