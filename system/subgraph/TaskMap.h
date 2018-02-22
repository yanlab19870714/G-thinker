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

#ifndef TASKMAP_H_
#define TASKMAP_H_

#include <atomic>
#include "util/conque_p.h"
#include "util/global.h"
#include "cuckoo/cuckoohash_map.hh"
#include <iostream>

using namespace std;

template <class TaskT>
class TaskMap {
public:
	conque_p<TaskT> task_buf; //for keeping ready tasks
	//- pushed by Comper
	//- popped by RespServer

	typedef cuckoohash_map<long long, TaskT *> TMap; // TMap[task_id] = task
	TMap task_map; //for keeping pending tasks
	//- added by Comper
	//- removed by RespServer

	unsigned int seqno; //sequence number for tasks, go back to 0 when used up

	int thread_rank; //associated with which thread?

	atomic<int> size;
	//updated by Comper, read by Worker's status_sync
	//> ++ by add2map(.)
	//> -- by get(.)

	long long peek_next_taskID() //not take it
	{
		long long id = thread_rank;
		id = (id << 48); //first 16-bit is thread_id
		return (id + seqno);
	}

	long long get_next_taskID() //take it
	{
		long long id = thread_rank;
		id = (id << 48); //first 16-bit is thread_id
		id += seqno;
		seqno++;
		return id;
	}

	TaskMap() //need to set thread_rank right after the object creation
	{
		seqno = 0;
		size = 0;
		task_map.reserve(TASKMAP_LIMIT);
	}

	TaskMap(int thread_id)
	{
		thread_rank = thread_id;
		seqno = 0;
		size = 0;
		task_map.reserve(TASKMAP_LIMIT);
	}

	//called by Comper, need to provide counter = task.pull_all(.)
	//only called if counter > 0
	void add2map(TaskT * task)
	{
		size++;
		//add to task_map
		task_map.insert(get_next_taskID(), task);
	}//no need to add "v -> tasks" track, should've been handled by lock&get(tid) -> request(tid)

	//called by RespServer
	//- counter--
	//- if task is ready, move it from "task_map" to "task_buf"
	void update(long long task_id)
	{
		TaskT* task = NULL;
		//erase from task_map, and get the task pointer into "task"
		auto fn_erase = [&](TaskT* & mapped)
		{
			mapped->met_counter ++;
			if(mapped->met_counter == mapped->req_size())
			{
				task = mapped; //record element's task pointer
				return true; //to erase the element
			}
			else return false; //task not yet ready
		};
		bool fn_called = task_map.erase_fn(task_id, fn_erase); //compete with comper in popping the task from task_map
		//must win if mapped.counter++ is the last ++, since erase_fn locks task_id, and comper's erase_fn will be blocked; hence the assert below
		assert(fn_called); //#DEBUG# to make sure key is found, and fn is called
		if(task != NULL) //task ready
		{
			//add to conque
			task_buf.enqueue(task);
		}
	}

	TaskT* get() //get the next ready-task in "task_buf", returns NULL if no more
	{
		//note:
		//called by Comper after inserting each task to "task_map"
		//Comper will fetch as many as possible (call get() repeatedly), till NULL is returned

		TaskT* ret = task_buf.dequeue();
		if(ret != NULL) size--;
		return ret;
	}
};

#endif /* TASKMAP_H_ */
