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

#ifndef TASK_H_
#define TASK_H_

#include "Subgraph.h"
#include "adjCache.h"
#include "util/serialization.h"
#include "util/ioser.h"
#include "Comper.h"
#include "TaskMap.h"

using namespace std;

template <class VertexT, class ContextT = char>
class Task {
public:
	typedef VertexT VertexType;
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;
	typedef ContextT ContextType;

    typedef Task<VertexT, ContextT> TaskT;
	typedef Subgraph<VertexT> SubgraphT;

	typedef AdjCache<TaskT> CTable;
	typedef hash_map<KeyT, VertexT*> VTable;

	typedef TaskMap<TaskT> TaskMapT;


	SubgraphT subG;
	ContextT context;
	HashT hash;

	//internally used:
	vector<KeyT> to_pull; //vertices to be pulled for use in next round
	//- to_pull needs to be swapped to old_to_pull, before calling compute(.) that writes to_pull
	//- unlock vertices in old_to_pull
	vector<VertexT *> frontier_vertexes; //remote vertices are replaced with NULL

	atomic<int> met_counter; //how many requested vertices are at local, updated by comper/RespServer, not for use by users (system only)

	Task(){}

	Task(const Task & o)
	{
		//defined so that "met_counter" is no longer a problem when using vector<TaskT>
		subG = o.subG;
		context = o.context;
		hash = o.hash;
		to_pull = o.to_pull;
	}

	inline size_t req_size()
	{
		return frontier_vertexes.size();
	}

	/*//deprecated, now Task has no UDF, all UDFs are moved to Comper
	//will be set by Comper before calling compute(), so that add_task() can access comper's add_task()
	virtual bool compute(SubgraphT & g, ContextType & context, vector<VertexType *> & frontier) = 0;
	*/

	//to be used by users in UDF compute(.)
	void pull(KeyT id){
		to_pull.push_back(id);
	}

	//after task.compute(.) returns, process "to_pull" to:
	//set "frontier_vertexes"
	bool pull_all(thread_counter & counter, TaskMapT & taskmap) //returns whether "no need to wait for remote-pulling"
	{//called by Comper, giving its thread_counter and thread_id
		long long task_id = taskmap.peek_next_taskID();
		CTable & vcache = *(CTable *)global_vcache;
		VTable & ltable = *(VTable *)global_local_table;
		met_counter = 0;
		bool remote_detected = false; //two cases: whether add2map(.) has been called or not
		int size = to_pull.size();
		frontier_vertexes.resize(size);
		for(int i=0; i<size; i++)
		{
			KeyT key= to_pull[i];
			if(hash(key) == _my_rank) //in local-table
			{
				frontier_vertexes[i] = ltable[key];
				met_counter++;
			}
			else //remote
			{
				if(remote_detected) //no need to call add2map(.) again
				{
					frontier_vertexes[i] = vcache.lock_and_get(key, counter, task_id);
					if(frontier_vertexes[i] != NULL) met_counter++;
				}
				else
				{
					frontier_vertexes[i] = vcache.lock_and_get(key, counter, task_id,
											taskmap, this);
					if(frontier_vertexes[i] != NULL) met_counter++;
					else //add2map(.) is called
					{
						remote_detected = true;
					}
				}
			}
		}
		if(remote_detected)
		{
			//so far, all pull reqs are processed, and pending resps could've arrived (not wakening the task)
			//------
			bool popped;
			auto fn_moveItem = [&](TaskT* & mapped)
			{
				if(met_counter == req_size())
				{//ready for task move, delete
					popped = true;
				}
				else
				{//pending resps, retain
					popped = false;
				}
				return popped; //to execute whether to delete from task_map or not
			};
			//------
			bool key_found = taskmap.task_map.erase_fn(task_id, fn_moveItem); //try to compete with RespServer in popping the task from task_map
			if(key_found) //not already moved by RespServer
			{
				if(popped) //popped successfully (counter-met = counter-requested), need to move the task
				{
					taskmap.task_buf.enqueue(this);
				}
				//else, RespServer will do the move
			}
			//else, RespServer has already did the move
			return false; //either has pending resps, or all resps are received but the task is now in task_buf (to be processed, but not this time)
		}
		else return true; //all v-local, continue to run the task for another iteration
	}

	void unlock_all()
	{
		CTable & vcache = *(CTable *)global_vcache;
		for(int i=0; i<frontier_vertexes.size(); i++)
		{
			VertexT * v = frontier_vertexes[i];
			if(hash(v->id) != _my_rank) vcache.unlock(v->id);
		}
	}

	//task_map => task_buf => push_task_from_taskmap() (where it is called)
	void set_pulled() //called after vertex-pull, to replace NULL's in "frontier_vertexes"
	{
		CTable & vcache = *(CTable *)global_vcache;
		for(int i=0; i<to_pull.size(); i++)
		{
			if(frontier_vertexes[i] == NULL)
			{
				frontier_vertexes[i] = vcache.get(to_pull[i]);
			}
		}
	}

	friend ibinstream& operator<<(ibinstream& m, const Task& v)
	{
		m << v.subG;
		m << v.context;
		m << v.to_pull;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Task& v)
	{
		m >> v.subG;
		m >> v.context;
		m >> v.to_pull;
		return m;
	}

	friend ifbinstream& operator<<(ifbinstream& m, const Task& v)
	{
		m << v.subG;
		m << v.context;
		m << v.to_pull;
		return m;
	}

	friend ofbinstream& operator>>(ofbinstream& m, Task& v)
	{
		m >> v.subG;
		m >> v.context;
		m >> v.to_pull;
		return m;
	}
};

#endif /* TASK_H_ */
