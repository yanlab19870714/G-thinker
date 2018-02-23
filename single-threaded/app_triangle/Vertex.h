#ifndef VERTEX_H_
#define VERTEX_H_

using namespace std;

template <class KeyT, class ValueT>
class Vertex {
public:
	KeyT id;
	ValueT value;

	typedef Vertex<KeyT, ValueT> VertexT;
	typedef KeyT KeyType;
	typedef ValueT ValueType;

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
};

#endif
