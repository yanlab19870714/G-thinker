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

#include "cuckoo/cuckoohash_map.hh"
#include <cassert>
#include <queue>
#include <atomic>
#include "ReqQueue.h" //for inserting reqs, triggered by lock_and_get(.)
#include "TaskMap.h" //an input to lock_and_get(.)

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

//====== zero cache (to keep track of unlocked objects) ======
//since "iterator" needs to lock a map/set, we need to split zcache into many maps/sets

int ZCACHE_BIN_NUM = 100; //how many bins are there in zero cache //!!! parameter to tune !!!
int OVER_READ_FACTOR = 2; //how many times more "lock-counter = 0" items are read from zcache
//this is necessary to avoid reading all elements, since in later stage zcache could be quite full
//we over-read a bit in case some vertices in the fetched list are relocked

//default hash function
template <class KeyType>
class DefaultBinHash {
public:
    inline int operator()(KeyType key)
    {
        if (key >= 0)
            return key % ZCACHE_BIN_NUM;
        else
            return (-key) % ZCACHE_BIN_NUM;
    }
};

template <class KeyType, class HashT = DefaultBinHash<KeyType> >
class ZeroCache
{
public:
    HashT hash; //level-1 hash, hardcoded mapping
    
    //array of sets, for fine-grained locking
    typedef cuckoohash_map<KeyType, char> ZSet; //value is dummy, always 0 (cuckoo does not have hashset...)
    typedef vector<ZSet> ZSets;
    ZSets sets;
    
    ZeroCache(int bin_number)
    {
        ZCACHE_BIN_NUM = bin_number;
        sets.resize(ZCACHE_BIN_NUM);
    }
    
    ZeroCache()
    {
        sets.resize(ZCACHE_BIN_NUM);
    }
    
    void resize(int bin_number)
    {
        ZCACHE_BIN_NUM = bin_number;
        sets.resize(ZCACHE_BIN_NUM);
    }
    
    void insert(KeyType key)
    {
        ZSet & zset = sets[hash(key)];
        bool inserted = zset.insert(key, 0);
        assert(inserted); //#DEBUG# to make sure item is inserted to zcache as expected (not already there)
    }
    
    void erase(KeyType key)
    {
        ZSet & zset = sets[hash(key)];
        bool erased = zset.erase(key);
        assert(erased); //#DEBUG# to make sure item is removed from zcache as expected (should be already there)
    }
    
    ZSet& getSet(int pos)
    {
        return sets[pos];
    }
};

//====== pull-cache (to keep objects to pull from remote) ======
//tracking tasks that request for each vertex
struct TaskIDVec
{
	vector<long long> list; //for ease of erase

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
    typedef cuckoohash_map<KeyType, TaskIDVec> PCache; //value is just lock-counter
    //we do not need a state to indicate whether the vertex request is sent or not
    //a request will be added to the sending stream (ReqQueue) if an entry is newly inserted
    //otherwise, it may be on-the-fly, or waiting to be sent, but we only merge the reqs to avoid repeated sending
    PCache pcache;
    
    //subfunctions to be called by adjCache
    int erase(KeyType key, vector<long long> & tid_collector) //returns lock-counter, to be inserted along with received adj-list into vcache
    {
        int ret;
        auto fn_erase = [&](TaskIDVec & ids)
        {
        	ret = ids.list.size(); //record the lock-counter before deleting the element
            assert(ret > 0); //#DEBUG# we do not allow counter to be 0 here
            tid_collector.swap(ids.list);
            return true; //to erase the element
        };
        bool fn_called = pcache.erase_fn(key, fn_erase);
        assert(fn_called); //#DEBUG# to make sure key is found, and fn is called
        return ret;
    }
    
    bool request(KeyType key, thread_counter & counter, long long task_id) //returns "whether a req is newly inserted" (i.e., not just add lock-counter)
    {
        bool inserted = pcache.upsert(key, [&](TaskIDVec & ids) { ids.list.push_back(task_id); }, task_id);
        if(inserted) counter.increment();
        return inserted;
    }
};

//====== object cache ======

/* deprecated class def
template <class KeyType, class ValType>
class AdjCache
{
public:
*/

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
        
        AdjValue(){}
        
        AdjValue(PullCache<KeyType> & pcache, KeyType key, ValType * value, vector<long long> & tid_collector) //for use by insert(.) to atomically delete item from pull-cache
        {
            this->value = value;
            this->counter = pcache.erase(key, tid_collector); //counter is obtained from pull-cache
        }
    };
    
    //internal cuckoohash_map for cached objects
    typedef cuckoohash_map<KeyType, AdjValue> ValCache;
    ValCache vcache;
    
    //internal cuckoohash_map (used as a set) for objects that can be evicted
    ZeroCache<KeyType> zcache;
    PullCache<KeyType> pcache;
    
    int pos; //position of the set in zcache's sets, for deleting elements to make room
    
    AdjCache(){ pos = 0; }
    
    AdjCache(int bin_number)
    {
        pos = 0;
        zcache.resize(ZCACHE_BIN_NUM);
    }
    
    ~AdjCache()
    {
        auto lt = vcache.lock_table();
        for(const auto &it : lt) {
            delete it.second.value;
        }
    }
    
    void reserve(int capacity)
    {
        vcache.reserve(capacity);
    }
    
    //to be called by computing threads, returns:
    //1. NULL, if not found; but req will be posed
    //2. vcache[key].value, if found; vcache[counter] is incremented by 1
    ValType * lock_and_get(KeyType & key, thread_counter & counter, long long task_id,
    		TaskMapT & taskmap, TaskT* task) //used when vcache miss happens, for adding the task to task_map
    {
        ValType * ret;
        bool new_req;
        auto fn_lock = [&](AdjValue & vpair)
        {
            int & cnt = vpair.counter;
            if(cnt == 0)
            {//0 -> 1
                zcache.erase(key);
            }
            cnt++;
            ret = vpair.value;
        };
        auto fn_req = [&]()
        {
        	taskmap.add2map(task);
            new_req = pcache.request(key, counter, task_id);
        };
        bool found = vcache.update_fn(key, fn_lock, fn_req); //atomic to "key" here: either call fn_lock to update zcache, or call fn_req to update pcache
        if(found) return ret;
        else
        {
        	//an entry is newly inserted to pullcache's pcache:
        	//add it to ReqQueue
            if(new_req) q_req.add(key);
            return NULL;
        }
    }
    
    //to be called by computing threads, returns:
	//1. NULL, if not found; but req will be posed
	//2. vcache[key].value, if found; vcache[counter] is incremented by 1
	ValType * lock_and_get(KeyType & key, thread_counter & counter, long long task_id)
	{
		ValType * ret;
		bool new_req;
		auto fn_lock = [&](AdjValue & vpair)
		{
			int & cnt = vpair.counter;
			if(cnt == 0)
			{//0 -> 1
				zcache.erase(key);
			}
			cnt++;
			ret = vpair.value;
		};
		auto fn_req = [&]()
		{
			new_req = pcache.request(key, counter, task_id);
		};
		bool found = vcache.update_fn(key, fn_lock, fn_req); //atomic to "key" here: either call fn_lock to update zcache, or call fn_req to update pcache
		if(found) return ret;
		else
		{
			//an entry is newly inserted to pullcache's pcache:
			//add it to ReqQueue
			if(new_req) q_req.add(key);
			return NULL;
		}
	}

    ValType * get(KeyType & key) //called only if you are sure of cache hit
    {
    	return vcache.find(key).value;
    }

    //to be called by computing threads, after finishing using an object
    //will not check existence, assume previously locked and so must be in the hashmap
    void unlock(KeyType & key)
    {
        auto fn_unlock = [&](AdjValue & vpair){
            int & cnt = vpair.counter;
            cnt--;
            if(cnt == 0)
            {//1 -> 0
                zcache.insert(key);
            }
        };
        vcache.update_fn(key, fn_unlock);
    }
    
    //note:
    //1. the above two functions only insert/erase zcache, not vcache (though its lock-counter is updated)
    //2. zcache.erase/insert(key) should have no conflict, since they are called in vcache.update_fn(.) which avoids the conflict on "key"
    
    //to be called by communication threads: pass in "ValType *", "new"-ed outside
    //** obtain lock-counter from pcache
    //** these locks are transferred from pcache to the vcache
    void insert(KeyType key, ValType * value, vector<long long> & tid_collector) //tid_collector gets the task-id list
	{
		bool inserted = vcache.insert(key, pcache, key, value, tid_collector);
		//** here: constructor "AdjValue(pcache, key, value)" is called to delete "key" from pcache atomically
		assert(inserted); //#DEBUG# to make sure item is not already in vcache
		//#DEBUG# this should be the case if logic is correct:
		//#DEBUG# not_in_vcache -> pull -> insert
	}
    
    //note:
    //1. lock_and_get & insert operates on pull-cache
    //2. pcache.erase/request(key) should have no conflict, since they are called in vcache.update_fn(.) which avoids the conflict on "key"
    
    //to be called by writers: the key is to delete "ValType *"
    //** this is called to make room in vcache
    bool erase(KeyType key, thread_counter & counter) //returns true if successful
    {
        bool ret;
        auto fn_erase = [&](AdjValue & vpair)
        {
            if(vpair.counter == 0) //to make sure item is not locked
            {
                counter.decrement();
                delete vpair.value;
                ret = true; //in order to erase the entry in vcache
                zcache.erase(key);//also erase "key" from the zcache, since it's no longer in vcache
            }
            else ret = false;
            return ret;
        };
        bool fn_called = vcache.erase_fn(key, fn_erase);
        assert(fn_called); //#DEBUG# to make sure key is found, and fn is called
        return ret;
    }
    
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
			auto set = zcache.getSet(pos); //get current set
			queue<KeyType> candidates;
			int size_c = num_to_delete * OVER_READ_FACTOR;
			{//read zcache only, to write zcache[v] we need to first write vcache[v]
				auto lt = set.lock_table();
				for(const auto &it : lt) {
					candidates.push(it.first);
					size_c--;
					if(size_c == 0) break;
				}
			}//set.unlock_table
			//------
			while(!candidates.empty())
			{
				KeyType & to_del = candidates.front();
				candidates.pop();
				bool succ = erase(to_del, counter); //may fail if a computing thread locks "to_del" after "candidates" are fetched
				if(succ) //this means some element is deleted from vcache
				{
					num_to_delete--;
					if(num_to_delete == 0) break; //jump out of the inner while-loop, since there's no need to look at more candidates
				}
				//else, "candidates" is empty, need to check the next zset (by the outer while-loop)
			}
			//------
			//move set index forward in circular array of zero-set
			pos++;
			if(pos >= ZCACHE_BIN_NUM) pos -= ZCACHE_BIN_NUM;
			if(pos == start_pos) break; //finish one round of checking all zero-sets, no more checking
		}
		return num_to_delete;
    }

};

#endif
