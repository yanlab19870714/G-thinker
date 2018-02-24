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

#ifndef WORKER_H_
#define WORKER_H_

#include <iostream>

#include "util/global.h"
#include "util/ydhdfs.h"
#include "util/communication.h"
#include "Trimmer.h"
#include "adjCache.h"
#include "ReqServer.h"
#include "RespServer.h"
#include "Comper.h"
#include "GC.h"
#include "AggSync.h"
#include "Profiler.h"
#include <unistd.h> //sleep(sec)
#include <queue> //for std::priority_queue

using namespace std;

template <class Comper>
class Worker
{
public:
	typedef typename Comper::TaskType TaskT;
    typedef typename Comper::AggregatorType AggregatorT;
	typedef typename Comper::TaskMapT TaskMapT;

    typedef typename TaskT::VertexType VertexT;
    typedef typename TaskT::SubgraphT SubgraphT;
    typedef typename TaskT::ContextType ContextT;

    typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    typedef vector<VertexT*> VertexVec;

    typedef hash_map<KeyT, VertexT*> VTable;
    typedef typename VTable::iterator TableIter;

    typedef AdjCache<TaskT> CTable;

    typedef typename AggregatorT::PartialType PartialT;
    typedef typename AggregatorT::FinalType FinalT;

    typedef GC<TaskT> GCT;

    typedef Trimmer<VertexT> TrimmerT;

    //=======================================================
    //worker's data structures
    HashT hash;
    VTable local_table; //key-value store of my vertex portion
    CTable cache_table; //cached remote vertices, it creates ReqQueue for appeding reqs

	VertexVec vertexes;
    TaskMapT** taskmap_vec;

    bool local_idle; //indicate whether the current worker is idle
    //logic with work stealing:
    //if it tries to steal from all workers but failed to get any job, this should be set
    //after master's idle-condition sync, if job does not terminate, should steal for another round
    //ToDo："local_idle" may need to change to "one per thread", or aggregated over threads

    Comper* compers; //dynamic array of compers

    //=======================================================
    //Trimmer
    void setTrimmer(TrimmerT* trimmer)
	{
    	global_trimmer = trimmer;
	}

    //=======================================================
    //constructor & destructor

    Worker(int comper_num, string local_disk_path = "buffered_tasks", string report_path = "report")
    {
    	num_compers = comper_num;
    	TASK_DISK_BUFFER_DIR = local_disk_path;
    	_mkdir(TASK_DISK_BUFFER_DIR.c_str());
    	REPORT_DIR = report_path;
    	_mkdir(REPORT_DIR.c_str());
    	//------
    	global_end_label = false;
    	local_idle = false;
    	global_trimmer = NULL;
    	global_aggregator = NULL;
    	global_agg = NULL;
    	global_vcache = &cache_table;
    	global_local_table = &local_table;
		global_vertexes = &vertexes;
		idle_num_added = new atomic<bool>[comper_num];
    }

    void setAggregator(AggregatorT* ag)
    {
        global_aggregator = ag;
        global_agg = new FinalT;
        ag -> init();
    }

    AggregatorT* get_aggregator() //get aggregator
    //cannot use the same name as in global.h (will be understood as the local one, recursive definition)
	{
		return (AggregatorT*)global_aggregator;
	}

	virtual ~Worker()
	{
        for(int i=0;i<vertexes.size();i++){
            delete vertexes[i];
        }
		delete[] compers;
		delete[] taskmap_vec;
		delete[] global_tasknum_vec;
		delete[] idle_num_added;
		//ToDo: release aggregator
        if (global_agg != NULL)
            delete (FinalT*)global_agg;
	}

	//=======================================================
	//graph loading:

	//user-defined loading function
	virtual VertexT* toVertex(char* line) = 0;

	void load_graph(const char* inpath, VertexVec & vVec)
	{
		TrimmerT* trimmer = NULL;
		if(global_trimmer != NULL) trimmer = (TrimmerT*)global_trimmer;
		//------
		hdfsFS fs = getHdfsFS();
		hdfsFile in = getRHandle(inpath, fs);
		LineReader reader(fs, in);
		while(true)
		{
			reader.readLine();
			if (!reader.eof())
			{
				VertexT * v = toVertex(reader.getLine());
				if(trimmer) trimmer->trim(*v);
				vVec.push_back(v);
			}
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

	void sync_graph(VertexVec & vVec)
	{
		//ResetTimer(4);
		//set send buffer
		vector<VertexVec> _loaded_parts(_num_workers);
		for (int i = 0; i < vVec.size(); i++) {
			VertexT* v = vVec[i];
			_loaded_parts[hash(v->id)].push_back(v);
		}
		//exchange vertices to add
		all_to_all(_loaded_parts, GRAPH_LOAD_CHANNEL);

		vVec.clear();
		//collect vertices to add
		for (int i = 0; i < _num_workers; i++) {
			vVec.insert(vVec.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
		}
		_loaded_parts.clear();
		//StopTimer(4);
		//PrintTimer("Reduce Time",4);
	};

	void set_local_table(VertexVec & vVec)
	{
		for(int i=0; i<vVec.size(); i++)
		{
			VertexT * v = vVec[i];
			local_table[v->id] = v;
		}
	}

	//=======================================================
	void create_compers()
	{
		compers = new Comper[num_compers];
		//set global_taskmap_vec
		taskmap_vec = new TaskMapT*[num_compers];
		global_tasknum_vec = new atomic<size_t>[num_compers];
		global_taskmap_vec = taskmap_vec;
		for(int i=0; i<num_compers; i++)
		{
			taskmap_vec[i] = &(compers[i].map_task);
			global_tasknum_vec[i] = 0;
			compers[i].start(i);
		}
	}

	//called by the main worker thread, to sync computation-progress, and aggregator
	void status_sync(bool sth2steal)
	{
		bool worker_idle = (sth2steal == false) && (global_num_idle.load(memory_order_relaxed) == num_compers);
		if(_my_rank != MASTER_RANK)
		{
			send_data(worker_idle, MASTER_RANK, STATUS_CHANNEL);
			bool all_idle = recv_data<bool>(MASTER_RANK, STATUS_CHANNEL);
			if(all_idle) global_end_label = true;
		}
		else
		{
			bool all_idle = worker_idle;
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) all_idle = (recv_data<bool>(i, STATUS_CHANNEL) && all_idle);
			}
			if(all_idle) global_end_label = true;
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) send_data(all_idle, i, STATUS_CHANNEL);
			}
		}
	}

	//=======================================================
	//task stealing
	size_t get_remaining_task_num()
	//not counting number of active tasks in memory (for simplicity)
	{
		int table_remain = local_table.size() - global_vertex_pos;
		return table_remain + global_file_num * TASK_BATCH_NUM;
	}

	struct steal_plan
	{
		int src_rank;
		int dst_rank;
	};

	struct max_heap_entry
	{
		size_t num_remain;
		int rank;

		bool operator<(const max_heap_entry& o) const
		{
			return num_remain < o.num_remain;
		}
	};

	struct min_heap_entry
	{
		size_t num_remain;
		int rank;

		bool operator<(const min_heap_entry& o) const
		{
			return num_remain > o.num_remain;
		}
	};

	//UDF for stea1ing seed tasks
	virtual void task_spawn(VertexT * v, vector<TaskT> & tvec) = 0;

	/*//=== deprecated, 50 vertices may just spawn 0 task or 2 tasks (etc.), so the quota of 50 is wasted during plan generation
	//get tasks from local-table
	//returns false if local-table is exhausted
	bool locTable2vec(vector<TaskT> & tvec)
	{
		size_t begin, end; //[begin, end) are the assigned vertices (their positions in local-table)
		//note that "end" is exclusive
		int size = local_table.size();
		//======== critical section on "global_vertex_pos"
		global_vertex_pos_lock.lock();
		if(global_vertex_pos < size)
		{
			begin = global_vertex_pos; //starting element
			end = begin + TASK_BATCH_NUM;
			if(end > size) end = size;
			global_vertex_pos = end; //next position to spawn
		}
		else begin = -1; //meaning that local-table is exhausted
		global_vertex_pos_lock.unlock();
		//======== spawn tasks from local-table[begin, end)
		if(begin == -1) return false;
		else
		{
			VertexVec & gb_vertexes = *(VertexVec*) global_vertexes;
			for(int i=begin; i<end; i++)
			{//call UDF to spawn tasks
				task_spawn(gb_vertexes[i], tvec);
			}
			return true;
		}
	}
	*/

	//get tasks from local-table
	//returns false if local-table is exhausted
	bool locTable2vec(vector<TaskT> & tvec)
	{
		size_t begin, end; //[begin, end) are the assigned vertices (their positions in local-table)
		//note that "end" is exclusive
		int size = local_table.size();
		//======== critical section on "global_vertex_pos"
		while(tvec.size() < TASK_BATCH_NUM)
		{
			global_vertex_pos_lock.lock();
			if(global_vertex_pos < size)
			{
				begin = global_vertex_pos; //starting element
				end = begin + MINI_BATCH_NUM;
				if(end > size) end = size;
				global_vertex_pos = end; //next position to spawn
			}
			else begin = -1; //meaning that local-table is exhausted
			global_vertex_pos_lock.unlock();
			//======== spawn tasks from local-table[begin, end)
			if(begin == -1) return false;
			else
			{
				VertexVec & gb_vertexes = *(VertexVec*) global_vertexes;
				for(int i=begin; i<end; i++)
				{//call UDF to spawn tasks
					task_spawn(gb_vertexes[i], tvec);
				}
			}
		}
		return true;
	}

	//get tasks from disk files
	//returns false if "global_file_list" is empty
	bool file2vec(vector<TaskT> & tvec)
	{
		string file;
		bool succ = global_file_list.dequeue(file);
		if(!succ) return false; //"global_file_list" is empty
		else
		{
			global_file_num --;
			ofbinstream in(file.c_str());
			TaskT dummy;
			while(!in.eof())
			{
				TaskT task;
				tvec.push_back(dummy);
				in >> tvec.back();
			}
			in.close();
			//------
			if (remove(file.c_str()) != 0) {
				cout<<"Error removing file: "<<file<<endl;
				perror("Error printed by perror");
			}
			return true;
		}
	}

	//=== for handling task streaming on disk ===
	char fname[1000], num[20];
	long long fileSeqNo = 1;
	void set_fname() //will proceed file seq #
	{
		strcpy(fname, TASK_DISK_BUFFER_DIR.c_str());
		sprintf(num, "/%d_", _my_rank);
		strcat(fname, num);
		sprintf(num, "%d_", num_compers); //compers have rank 0, 1, ... comper_num-1; so there's no conflict
		strcat(fname, num);
		sprintf(num, "%lld", fileSeqNo);
		strcat(fname, num);
		fileSeqNo++;
	}

	bool steal_planning() //whether there's something to steal from/to others
	{
		vector<int> my_steal_list;
		//====== set my_steal_list
		if(_my_rank != MASTER_RANK)
		{
			send_data(get_remaining_task_num(), MASTER_RANK, STATUS_CHANNEL);
			recv_data<vector<int> >(MASTER_RANK, STATUS_CHANNEL, my_steal_list);
		}
		else
		{
			//collect remaining workloads
			vector<size_t> remain_vec(_num_workers);
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) remain_vec[i] = recv_data<size_t>(i, STATUS_CHANNEL);
				else remain_vec[i] = get_remaining_task_num();
			}
			//------
			priority_queue<max_heap_entry> max_heap;
			priority_queue<min_heap_entry> min_heap;
			for(int i=0; i<_num_workers; i++)
			{
				if(remain_vec[i] > MIN_TASK_NUM_BEFORE_STEALING)
				{
					max_heap_entry en;
					en.num_remain = remain_vec[i];
					en.rank = i;
					max_heap.push(en);
				}
				else if(remain_vec[i] < MIN_TASK_NUM_BEFORE_STEALING)
				{
					min_heap_entry en;
					en.num_remain = remain_vec[i];
					en.rank = i;
					min_heap.push(en);
				}
			}
			//------
			//plan generation
			vector<int> steal_num(_num_workers, 0); //each element should not exceed MAX_STEAL_TASK_NUM
			vector<steal_plan> plans;
			while(!max_heap.empty() && !min_heap.empty())
			{
				max_heap_entry max = max_heap.top();
				max_heap.pop();
				min_heap_entry min = min_heap.top();
				min_heap.pop();
				if(max.num_remain - TASK_BATCH_NUM < min.num_remain) break;
				else
				{
					max.num_remain -= TASK_BATCH_NUM;
					min.num_remain += TASK_BATCH_NUM;
					steal_num[min.rank] += TASK_BATCH_NUM;
					//---
					steal_plan plan;
					plan.src_rank = max.rank;
					plan.dst_rank = min.rank;
					plans.push_back(plan);
					//---
					if(max.num_remain > MIN_TASK_NUM_BEFORE_STEALING) max_heap.push(max);
					if(steal_num[min.rank] + TASK_BATCH_NUM <= MAX_STEAL_TASK_NUM &&
							min.num_remain < MIN_TASK_NUM_BEFORE_STEALING)
						min_heap.push(min);
				}
			}
			//------
			if(plans.size() > 0) cout<<plans.size()<<" stealing plans generated at the master"<<endl;//@@@@@@
			//calculating stealing tasks
			//a negative tag (-x-1) means receiving
			vector<vector<int> > steal_lists(_num_workers); //steal_list[i] = stealing tasks
			for(int i=0; i<plans.size(); i++)
			{
				steal_plan & plan = plans[i];
				steal_lists[plan.dst_rank].push_back(-plan.src_rank-1);
				steal_lists[plan.src_rank].push_back(plan.dst_rank);
			}
			//------
			//distribute the plans to machines
			for(int i=0; i<_num_workers; i++)
			{
				if(i == _my_rank) steal_lists[i].swap(my_steal_list);
				else
				{
					send_data(steal_lists[i], i, STATUS_CHANNEL);
				}
			}
		}
		//====== execute my_steal_list
		if(my_steal_list.size() == 0) return false;
		for(int i=0; i<my_steal_list.size(); i++)
		{
			int other = my_steal_list[i];
			if(other < 0)
			{
				vector<TaskT> tvec;
				recv_data<vector<TaskT> >(-other-1, STATUS_CHANNEL, tvec);
				if(tvec.size() > 0)
				{
					set_fname();
					ifbinstream out(fname);
					//------
					for(int i=0; i<tvec.size(); i++)
					{
						out << tvec[i];
					}
					out.close();
					num_stolen += tvec.size();
					//------
					//register with "global_file_list"
					global_file_list.enqueue(fname);
					global_file_num ++;
				}
			}
			else
			{
				vector<TaskT> tvec;
				if(get_remaining_task_num() > MIN_TASK_NUM_BEFORE_STEALING)
				//check this since time has passed, and more tasks may have been processed
				//send empty task-vec if no longer a task heavy-hitter
					if(locTable2vec(tvec) == false) file2vec(tvec);
				send_data(tvec, other, STATUS_CHANNEL); //send even if it's empty
			}
		}
		return true;
	}

	//=======================================================
	//program entry point
    void run(const WorkerParams& params)
    {
        //check path + init
        if (_my_rank == MASTER_RANK)
        {
            if (dirCheck(params.input_path.c_str()) == -1)
                return;
        }
        init_timers();

		//dispatch splits
		ResetTimer(WORKER_TIMER);
		vector<vector<string> >* arrangement;
		if (_my_rank == MASTER_RANK) {
			arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
			//reportAssignment(arrangement);//DEBUG !!!!!!!!!!
			masterScatter(*arrangement);
			vector<string>& assignedSplits = (*arrangement)[0];
			//reading assigned splits (map)
			for (vector<string>::iterator it = assignedSplits.begin();
				 it != assignedSplits.end(); it++)
				load_graph(it->c_str(), vertexes);
			delete arrangement;
		} else {
			vector<string> assignedSplits;
			slaveScatter(assignedSplits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assignedSplits.begin();
				 it != assignedSplits.end(); it++)
				load_graph(it->c_str(), vertexes);
		}

		//send vertices according to hash_id (reduce)
		sync_graph(vertexes);

		//init global_vertex_pos
		global_vertex_pos = 0;

		//use "vertexes" to set local_table
		set_local_table(vertexes);

		//barrier for data loading
		worker_barrier();
		StopTimer(WORKER_TIMER);
		PrintTimer("Load Time", WORKER_TIMER);

		//ReqQueue already set, by Worker::cache_table
		//>> by this time, ReqQueue occupies about 0.3% CPU

		//set up ReqServer (containing RespQueue), let it know local_table for responding reqs
		ReqServer<VertexT> server_req(local_table);

		//set up computing threads
		create_compers(); //side effect: set global_comper_vec

		//set up RespServer, let it know cache_table so that it can update it when getting resps
		RespServer<Comper> server_resp(cache_table); //it would read global_comper_vec

		//set up vcache GC
		GCT gc(cache_table);

		//set up AggSync
		AggSync<AggregatorT> * agg_thread; //the thread that runs agg_sync()
		if(global_aggregator != NULL) agg_thread = new AggSync<AggregatorT>;

		Profiler* profiler = new Profiler;

		//call status_sync() periodically
		while(global_end_label == false)
		{
			clock_t last_tick = clock();
			bool sth2steal = steal_planning();
            status_sync(sth2steal);
            //------
            //reset idle status of Worker, compers will add back if idle
            global_num_idle = 0;
            for(int i=0; i<num_compers; i++) idle_num_added[i] = false;
            usleep(STATUS_SYNC_TIME_GAP);
		}

		if(global_aggregator != NULL) delete agg_thread; //make sure destructor of agg_thread is called to do agg_sync() before exiting run()
		delete profiler;
    }
};

#endif
