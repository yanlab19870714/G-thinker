#include "Vertex.h"
#include "Subgraph.h"
#include <fstream>
#include <iostream>
#include <algorithm>
#include <set>

using namespace std;
typedef int VertexID;

typedef vector<VertexID> TriangleValue;
typedef Vertex<VertexID, TriangleValue> TriangleVertex;
typedef Subgraph<TriangleVertex> TriangleSubgraph;

void load(char* fname, TriangleSubgraph & g) {
	ifstream in(fname);
	VertexID temp, num;
	while(in >> temp)
	{
		TriangleVertex v;
		v.id = temp;
		in >> num;
        set<VertexID> nbs;
		for(int i=0; i<num; i++)
		{
			in >> temp;
			if(temp > v.id) nbs.insert(temp);
		}
        TriangleValue & list = v.value;
        for(set<VertexID>::iterator it = nbs.begin(); it != nbs.end(); it++)
        {
            list.push_back(*it);
        }
		g.addVertex(v);
	}
    in.close();
}

//input adj-list (1) must be sorted !!!
//(2) must remove all IDs less than vid !!!
size_t triangle_count(TriangleSubgraph & g)
{
	size_t count = 0;
	vector<TriangleVertex> & vertexes = g.vertexes;
	size_t V = vertexes.size();
	for(size_t i = 0; i < V; i++)
	{
		TriangleValue & vlist = vertexes[i].value; //v itself
		if(!vlist.empty())
		{
			for(int j=0; j<vlist.size(); j++)
			{
				VertexID u = vlist[j]; //u is the next smallest neighbor of v
                int m = j+1; //m is vlist's starting position to check
				TriangleValue & ulist = g.getVertex(u)->value;
				if(!ulist.empty())
				{
                    int k = 0; //k is ulist's starting position to check
                    while(k<ulist.size() && m<vlist.size())
					{
						if(ulist[k] == vlist[m])
						{
							count++;
							m++;
							k++;
						}
                        else if(ulist[k] > vlist[m]) m++;
						else k++;
					}
				}
			}
		}
    }
    return count;
}

int main(int argc, char* argv[])
{
    if(argc != 2)
    {
        cout<<"arg1 = input file name"<<endl;
        return -1;
    }
    
	TriangleSubgraph g;
	load(argv[1], g);
    cout<<"loaded"<<endl;

    size_t count = triangle_count(g);
    cout<<"# of triangles = "<<count<<endl;
    return 0;
}
