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

#ifndef SUBGRAPH_H_
#define SUBGRAPH_H_

#include "util/serialization.h"
#include "util/ioser.h"

template <class VertexT>
class Subgraph{
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;

	typedef hash_map<KeyT, int> VertexMap;
	VertexMap vmap; //vmap[vid] = index of the vertex in "vertexes" defined below
	vector<VertexT> vertexes; //store the nodes of this subgraph

	void addVertex(VertexT & vertex){
		vmap[vertex.id] = vertexes.size();
		vertexes.push_back(vertex); //deep copy
	}

	//adding edge = get neigbors' values and cope with the adj-lists in them

	bool hasVertex(KeyT vid){
		return vmap.find(vid) != vmap.end();
	}

	VertexT * getVertex(KeyT id){
		typename VertexMap::iterator it = vmap.find(id);
		if(it != vmap.end())
			return &(vertexes[it->second]);
		return NULL;
	}

	//only write "vertexes" to disk / binary-stream
	//when serialized back, "vmap" is reconstructed
	friend ibinstream& operator<<(ibinstream& m, const Subgraph& v)
	{
		m << v.vertexes;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Subgraph& v)
	{
		vector<VertexT> & vertexes = v.vertexes;
		VertexMap & vmap = v.vmap;
		m >> vertexes;
		for(int i = 0 ; i < vertexes.size(); i++)
			vmap[vertexes[i].id] = i;
		return m;
	}

	friend ifbinstream& operator<<(ifbinstream& m, const Subgraph& v)
	{
		m << v.vertexes;
		return m;
	}

	friend ofbinstream& operator>>(ofbinstream& m, Subgraph& v)
	{
		vector<VertexT> & vertexes = v.vertexes;
		VertexMap & vmap = v.vmap;
		m >> vertexes;
		for(int i = 0 ; i < vertexes.size(); i++)
			vmap[vertexes[i].id] = i;
		return m;
	}
};

#endif /* SUBGRAPH_H_ */
