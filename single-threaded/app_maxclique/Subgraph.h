#ifndef SUBGRAPH_H_
#define SUBGRAPH_H_

#include <ext/hash_map>
#include <vector>
#define hash_map __gnu_cxx::hash_map

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
};

#endif /* SUBGRAPH_H_ */
