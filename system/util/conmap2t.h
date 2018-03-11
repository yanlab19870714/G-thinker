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

#ifndef CONMAP2T_H
#define CONMAP2T_H

//concurrent map for two threads

#define CONMAP2T_BUCKET_NUM 10 //used by TaskMap, should allow a comper and the response-processing thread to minimize collision

//idea: 2-level hashing
//1. id % CONMAP_BUCKET_NUM -> bucket_index
//2. bucket[bucket_index] -> give id, get content

//now, we can dump zero-cache, since GC can directly scan buckets one by one

#include <util/global.h>
#include <vector>
using namespace std;

template <typename K, typename V> struct conmap2t_bucket
{
	typedef hash_map<K, V> KVMap;
	typedef hash_set<K> KSet;
	mutex mtx;
	KVMap bucket;

	inline void lock()
	{
		mtx.lock();
	}

	inline void unlock()
	{
		mtx.unlock();
	}

	KVMap & get_map()
	{
		return bucket;
	}

	//returns true if inserted
	//false if an entry with this key alreqdy exists
	bool insert(K key, V & val)
	{
		auto ret = bucket.insert(
			std::pair<K, V>(key, val)
		);
		return ret.second;
	}

	//returns whether deletion is successful
	bool erase(K key)
	{
		size_t num_erased = bucket.erase(key);
		return (num_erased == 1);
	}
};

template <typename K, typename V> struct conmap2t
{
public:
	typedef conmap2t_bucket<K, V> bucket;
	bucket* buckets;

	conmap2t()
	{
		buckets = new bucket[CONMAP2T_BUCKET_NUM];
	}

	bucket & get_bucket(K key)
	{
		return buckets[key % CONMAP2T_BUCKET_NUM];
	}

	bucket & pos(size_t pos)
	{
		return buckets[pos];
	}

	~conmap2t()
	{
		delete[] buckets;
	}
};

#endif
