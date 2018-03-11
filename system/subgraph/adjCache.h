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

#ifndef ADJCACHE_H_
#define ADJCACHE_H_

/*
The cache of data objects wraps cuckoo hashmap: http://efficient.github.io/libcuckoo
Maintaining a lock_counter with each table entry
Two sub-structures:
1. zeroCache: keeps only those entrys with lock_counter == 0, for ease of eviction
2. pullCache: keeps objects to pull from remote
*/

#include <cassert>
#include <queue>
#include <atomic>
#include "ReqQueue.h" //for inserting reqs, triggered by lock_and_get(.)
#include "TaskMap.h" //an input to lock_and_get(.)
#include "util/conmap.h"
#include "util/conmap0.h"

using namespace std;

//====== global counter ======
atomic<int> global_cache_size(0); //cache size: "already-in" + "to-pull"
int COMMIT_FREQ = 10; //delta that needs to be committed from each local counter to "global_cache_size"
//parameter to fine-tune !!!

//====== thread counter ======
struct thread_counter
{
    int count;
    
    thread_counter()
    {
        count = 0;
    }
    
    void increment() //by 1, called when a vertex is requested over pull-cache for the 1st time
    {
        count++;
        if(count >= COMMIT_FREQ)
        {
            global_cache_size += count;
            count = 0; //clear local counter
        }
    }
    
    void decrement() //by 1, called when a vertex is erased by vcache
    {
        count--;
        if(count <= - COMMIT_FREQ)
        {
            global_cache_size += count;
            count = 0; //clear local counter
        }
    }
};

//====== pull-cache (to keep objects to pull from remote) ======
//tracking tasks that request for each vertex
struct TaskIDVec
{
	vector<long long> list; //for ease of erase

	TaskIDVec(){}

	TaskIDVec(long long first_tid)
	{
		list.push_back(first_tid);
	}
};

//pull cache
template <class KeyType>
class PullCache
{
public:
    typedef conmap0<KeyType, TaskIDVec> PCache; //value is just lock-counter
    //we do not need a state to indicate whether the vertex request is sent or not
    //a request will be added to the sending stream (ReqQueue) if an entry is newly inserted
    //otherwise, it may be on-the-fly, or waiting to be sent, but we only merge the reqs to avoid repeated sending
    PCache pcache;
    
    //subfunctions to be called by adjCache
    size_t erase(KeyType key, vector<long long> & tid_collector) //returns lock-counter, to be inserted along with received adj-list into vcache
    {
    	conmap0_bucket<KeyType, TaskIDVec> & bucket = pcache.get_bucket(key);
    	hash_map<KeyType, TaskIDVec> & kvmap = bucket.get_map();
    	auto it = kvmap.find(key);
    	assert(it != kvmap.end()); //#DEBUG# to make sure key is found
    	TaskIDVec & ids = it->second;
    	size_t ret = ids.list.size(); //record the lock-counter before deleting the element
		assert(ret > 0); //#DEBUG# we do not allow counter to be 0 here
		tid_collector.swap(ids.list);
		kvmap.erase(it); //to erase the element
        return ret;
    }
    
    bool request(KeyType key, thread_counter & counter, long long task_id) //returns "whether a req is newly inserted" (i.e., not just add lock-counter)
    {
    	conmap0_bucket<KeyType, TaskIDVec> & bucket = pcache.get_bucket(key);
		hash_map<KeyType, TaskIDVec> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
    	if(it != kvmap.end())
    	{
    		TaskIDVec & ids = it->second;
    		ids.list.push_back(task_id);
    		return false;
    	}
    	else
    	{
    		kvmap[key].list.push_back(task_id);
    		return true;
    	}
    }
};

//====== object cache ======

template <class TaskT>
class AdjCache
{
public:

	typedef typename TaskT::VertexType ValType;
	typedef typename ValType::KeyType KeyType;

	typedef TaskMap<TaskT> TaskMapT;

	ReqQueue<ValType> q_req;
    
    //an object wrapper, expanded with lock-counter
    struct AdjValue
    {
        ValType * value;//note that it is a pointer !!!
        int counter; //lock-counter, bounded by the number of active tasks in a machine
    };
    
    //internal conmap for cached objects
    typedef conmap<KeyType, AdjValue> ValCache;
    ValCache vcache;
    PullCache<KeyType> pcache;
    
    ~AdjCache()
    {
    	for(int i=0; i<CONMAP_BUCKET_NUM; i++)
    	{
    		conmap_bucket<KeyType, AdjValue> & bucket = vcache.pos(i);
    		bucket.lock();
    		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
    		for(auto it = kvmap.begin(); it != kvmap.end(); it++)
    		{
    			delete it->second.value; //release cached vertices
    		}
    		bucket.unlock();
    	}
    }
    
    //to be called by computing threads, returns:
    //1. NULL, if not found; but req will be posed
    //2. vcache[key].value, if found; vcache[counter] is incremented by 1
    ValType * lock_and_get(KeyType & key, thread_counter & counter, long long task_id,
    		TaskMapT & taskmap, TaskT* task) //used when vcache miss happens, for adding the task to task_map
    {
    	ValType * ret;
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
    	bucket.lock();
    	hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
    	auto it = kvmap.find(key);
    	if(it == kvmap.end())
		{
			taskmap.add2map(task);
			bool new_req = pcache.request(key, counter, task_id);
			if(new_req) q_req.add(key);
			ret = NULL;
		}
    	else
    	{
        	AdjValue & vpair = it->second;
        	if(vpair.counter == 0) bucket.zeros.erase(key); //zero-cache.remove
        	vpair.counter++;
        	ret = vpair.value;
    	}
    	bucket.unlock();
    	return ret;
    }
    
    //to be called by computing threads, returns:
	//1. NULL, if not found; but req will be posed
	//2. vcache[key].value, if found; vcache[counter] is incremented by 1
	ValType * lock_and_get(KeyType & key, thread_counter & counter, long long task_id)
	{
		ValType * ret;
		conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		if(it == kvmap.end())
		{
			bool new_req = pcache.request(key, counter, task_id);
			if(new_req) q_req.add(key);
			ret = NULL;
		}
		else
		{
			AdjValue & vpair = it->second;
			if(vpair.counter == 0) bucket.zeros.erase(key); //zero-cache.remove
			vpair.counter++;
			ret = vpair.value;
		}
		bucket.unlock();
		return ret;
	}

	//must lock, since the hash_map may be updated by other key-insertion
	ValType * get(KeyType & key) //called only if you are sure of cache hit
	{
		conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		assert(it != kvmap.end());
		ValType * val = it->second.value;
		bucket.unlock();
		return val;
	}

    //to be called by computing threads, after finishing using an object
    //will not check existence, assume previously locked and so must be in the hashmap
    void unlock(KeyType & key)
    {
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
    	bucket.lock();
    	hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		assert(it != kvmap.end());
    	it->second.counter--;
    	if(it->second.counter == 0) bucket.zeros.insert(key); //zero-cache.insert
    	bucket.unlock();
    }

    //to be called by communication threads: pass in "ValType *", "new"-ed outside
    //** obtain lock-counter from pcache
    //** these locks are transferred from pcache to the vcache
    void insert(KeyType key, ValType * value, vector<long long> & tid_collector) //tid_collector gets the task-id list
	{
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		AdjValue vpair;
		vpair.value = value;
		vpair.counter = pcache.erase(key, tid_collector);
		bool inserted = bucket.insert(key, vpair);
		assert(inserted);//#DEBUG# to make sure item is not already in vcache
		//#DEBUG# this should be the case if logic is correct:
		//#DEBUG# not_in_vcache -> pull -> insert
		bucket.unlock();
	}
    
    //note:
    //1. lock_and_get & insert operates on pull-cache
    //2. pcache.erase/request(key) should have no conflict, since they are called in vcache's lock(key) section
    
    int pos = 0; //starting position of bucket in conmap

    //try to delete "num_to_delete" elements, called only if capacity VCACHE_LIMIT is reached
    //insert(.) just moves data from pcache to vcache, does not change total cache capacity
    //calling erase followed by insert is not good, as some elements soon to be locked (by other tasks) may be erased
    //we use the strategy of "batch-insert" + "trim-to-limit" (best-effort)
    //if we fail to trim to capacity limit after checking one round of zero-cache, we just return
    size_t shrink(size_t num_to_delete, thread_counter & counter) //return value = how many elements failed to trim
    {
    	int start_pos = pos;
		//------
		while(num_to_delete > 0) //still need to delete element(s)
		{
			conmap_bucket<KeyType, AdjValue> & bucket = vcache.pos(pos);
			bucket.lock();
			auto it = bucket.zeros.begin();
			while(it != bucket.zeros.end())
			{
				KeyType key = *it;
				hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
				auto it1 = kvmap.find(key);
				assert(it1 != kvmap.end()); //#DEBUG# to make sure key is found, this is where libcuckoo's bug prompts
				AdjValue & vpair = it1->second;
				if(vpair.counter == 0) //to make sure item is not locked
				{
					counter.decrement();
					delete vpair.value;
					kvmap.erase(it1);
					it = bucket.zeros.erase(it); //update it
					num_to_delete--;
					if(num_to_delete == 0) //there's no need to look at more candidates
					{
						bucket.unlock();
						pos++; //current bucket has been checked, next time, start from next bucket
						return 0;
					}
				}
			}
			bucket.unlock();
			//------
			//move set index forward in a circular manner
			pos++;
			if(pos >= CONMAP_BUCKET_NUM) pos -= CONMAP_BUCKET_NUM;
			if(pos == start_pos) break; //finish one round of checking all zero-sets, no more checking
		}
		return num_to_delete;
    }

};

#endif
