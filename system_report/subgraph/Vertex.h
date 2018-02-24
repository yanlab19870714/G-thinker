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

#ifndef VERTEX_H_
#define VERTEX_H_

#include "util/serialization.h"
#include "util/ioser.h"

using namespace std;

//Default Hash Function =====================
template <class KeyT>
class DefaultHash {
public:
    inline int operator()(KeyT key)
    {
        if (key >= 0)
            return key % _num_workers;
        else
            return (-key) % _num_workers;
    }
};
//==========================================

template <class KeyT, class ValueT, class HashT = DefaultHash<KeyT> >
class Vertex {
public:
	KeyT id;
	ValueT value;

	typedef Vertex<KeyT, ValueT, HashT> VertexT;
	typedef KeyT KeyType;
	typedef ValueT ValueType;
	typedef HashT HashType;

	inline bool operator<(const VertexT& rhs) const
	{
		return id < rhs.id;
	}
	inline bool operator==(const VertexT& rhs) const
	{
		return id == rhs.id;
	}
	inline bool operator!=(const VertexT& rhs) const
	{
		return id != rhs.id;
	}

	friend ibinstream& operator<<(ibinstream& m, const VertexT& v)
	{
		m << v.id;
		m << v.value;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, VertexT& v)
	{
		m >> v.id;
		m >> v.value;
		return m;
	}

	friend ifbinstream& operator<<(ifbinstream& m,  const VertexT& v)
	{
		m << v.id;
		m << v.value;
		return m;
	}

	friend ofbinstream& operator>>(ofbinstream& m,  VertexT& v)
	{
		m >> v.id;
		m >> v.value;
		return m;
	}
};

#endif
