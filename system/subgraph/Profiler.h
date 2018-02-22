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

#ifndef PROFILER_H_
#define PROFILER_H_

#include "util/global.h"
#include "util/communication.h"
#include <unistd.h> //for usleep()
#include <thread>
using namespace std;

class Profiler {
public:

	thread main_thread;
	bool first_sync;
	vector<size_t> prev;

	bool endTag_sync() //returns true if any worker has endTag = true
	{
		bool endTag = global_end_label;
		if(_my_rank != MASTER_RANK)
		{
			send_data(endTag, MASTER_RANK, PROGRESS_CHANNEL);
			bool ret = recv_data<bool>(MASTER_RANK, PROGRESS_CHANNEL);
			return ret;
		}
		else
		{
			bool all_end = endTag;
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) all_end = (recv_data<bool>(i, PROGRESS_CHANNEL) || all_end);
			}
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) send_data(all_end, i, PROGRESS_CHANNEL);
			}
			return all_end;
		}
	}

	void progress_sync()
	{
		size_t task_num = 0;
		for(int i=0; i<num_compers; i++)
			task_num += global_tasknum_vec[i];
		//======
		if (_my_rank != MASTER_RANK)
		{//send partialT to aggregator
			send_data(task_num, MASTER_RANK, PROGRESS_CHANNEL);
			//------
			size_t n_stolen = num_stolen;
			send_data(n_stolen, MASTER_RANK, PROGRESS_CHANNEL);
			num_stolen = 0; //clear
		}
		else
		{
			vector<size_t> task_num_vec(_num_workers); //collecting task# from all workers
			vector<size_t> stolen_vec(_num_workers); //collecting stolen-task# from all workers
			size_t total = 0;
			for(int i = 0; i < _num_workers; i++)
			{
				if(i != MASTER_RANK)
				{
					task_num_vec[i] = recv_data<size_t>(i, PROGRESS_CHANNEL);
					stolen_vec[i] = recv_data<size_t>(i, PROGRESS_CHANNEL);
				}
				else
				{
					task_num_vec[i] = task_num;
					stolen_vec[i] = num_stolen;
					num_stolen = 0; //clear
				}
				total += task_num_vec[i];
			}
			//======
			cout<<endl;
			cout<<"============ PROGRESS REPORT ============"<<endl;
			cout<<"Total number of tasks processed: "<<total<<endl;
			for(int i=0; i<_num_workers; i++)
			{
				if(first_sync)
				{
					cout<<"Worker "<<i<<": "<<task_num_vec[i]<<" tasks processed"<<endl;
				}
				else
				{
					size_t delta = task_num_vec[i] - prev[i];
					cout<<"Worker "<<i<<": "<<task_num_vec[i]<<" tasks processed (+ "<<delta<<")"<<endl;
				}
				prev[i] = task_num_vec[i];
				//------
				if(stolen_vec[i] > 0) cout<<"--- "<<stolen_vec[i]<<" more tasks stolen"<<endl;
			}
			cout<<endl;
		}
		first_sync = false;
	}

    void run()
    {
    	while(true) //global_end_label is checked with "any_end"
    	{
    		bool any_end = endTag_sync();
    		if(any_end)
    		{
    			while(global_end_label == false); //block till main_thread sets global_end_label = true
    			return;
    		}
    		else{
    			progress_sync();
    			usleep(PROGRESS_SYNC_TIME_GAP); //polling frequency
    		}
    	}
    }

    Profiler()
    {
    	main_thread = thread(&Profiler::run, this);
    	prev.resize(_num_workers);
    	first_sync = true;
    }

    ~Profiler()
    {
    	main_thread.join();
    	progress_sync(); //to make sure results of all tasks are aggregated
    }
};

#endif
