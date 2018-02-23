#include "Vertex.h"
#include "Subgraph.h"
#include <fstream>
#include <iostream>
#include <set>

using namespace std;

typedef int VertexID;
typedef char Label;

struct AdjItem
{
	VertexID id;
	Label l;
};

struct GMatchValue
{
	Label l;
	vector<AdjItem> adj;
};

typedef Vertex<VertexID, GMatchValue> GMatchVertex;
typedef Subgraph<GMatchVertex> GMatchSubgraph;

//id label \t num_nbs nb1id nb1label nb2id nb2label ...
void load(char* fname, GMatchSubgraph & g) {
	ifstream in(fname);
    VertexID temp_id;
    int num;
	AdjItem adj;
	while(in >> temp_id)
	{
		GMatchVertex v;
		v.id = temp_id;
		in >> v.value.l;
		in >> num;
		vector<AdjItem> & nbs = v.value.adj;
		for(int i=0; i<num; i++)
		{
			in >> adj.id;
			in >> adj.l;
			nbs.push_back(adj);
		}
		g.addVertex(v);
	}
    in.close();
}

void print_vec(vector<VertexID> & vertices){
	for(int i = 0; i < vertices.size(); i++)
		cout << vertices[i] << "  ";
	cout << endl;
}

//returns the count of subgraphs that match our hard-coded subgraph
size_t graph_matching(GMatchSubgraph & g)
{
	size_t count = 0;
	vector<GMatchVertex> & nodes = g.vertexes;
	vector<VertexID> GMatchQ; //record matched vertex instances
	for(int i = 0; i < nodes.size(); i++)
	{
		GMatchVertex & v_a = nodes[i];
		if(v_a.value.l == 'a')
		{
			vector<AdjItem> & a_adj = v_a.value.adj;
			GMatchQ.push_back(v_a.id);
			for(int j = 0; j < a_adj.size(); j++)
            {
				if(a_adj[j].l == 'c')
				{
					GMatchVertex * v_c = g.getVertex(a_adj[j].id);
					GMatchQ.push_back(v_c->id);
					vector<AdjItem> & c_adj = v_c->value.adj;
					//find b
					vector<VertexID> b_nodes;
					for(int k = 0; k < c_adj.size(); k++)
					{
						if(c_adj[k].l == 'b')
							b_nodes.push_back(c_adj[k].id);
					}
					if(b_nodes.size() > 1)
					{
						vector<bool> b_a(b_nodes.size(), false);//b_a[i] = whether b_nodes[i] links to v_a
						vector<vector<VertexID> > b_d;//b_d[i] = all b-d edges (d's IDs) of b_nodes[i]
						for(int m = 0; m < b_nodes.size(); m++)
						{
							GMatchVertex * v_b = g.getVertex(b_nodes[m]);
							vector<AdjItem> & b_adj = v_b->value.adj;
							vector<VertexID> vec_d;
							for(int n = 0; n < b_adj.size(); n++)
							{
								if(b_adj[n].id == v_a.id)
									b_a[m] = true;
								if(b_adj[n].l == 'd')
									vec_d.push_back(b_adj[n].id);
							}
							b_d.push_back(vec_d);
						}
						for(int m = 0; m < b_nodes.size(); m++)
						{
							if(b_a[m])
							{
								GMatchQ.push_back(b_nodes[m]);//push vertex (3) into GMatchQ
								for(int n = 0; n < b_d.size(); n++)
								{
									if(m != n) //two b's cannot be the same node
									{
										GMatchQ.push_back(b_nodes[n]);//push vertex (4) into GMatchQ
										vector<VertexID> & vec_d = b_d[n];
                                        count += vec_d.size();
										/* //version that outputs the actual match
                                        for(int cur = 0; cur < vec_d.size(); cur++)
										{
											GMatchQ.push_back(vec_d[cur]);
											print_vec(GMatchQ);
											count++;
											GMatchQ.pop_back();//d
										}
                                        */
										GMatchQ.pop_back();//b
									}
								}
								GMatchQ.pop_back();//b
							}
						}
					}
					GMatchQ.pop_back();//c
				}
			}
			GMatchQ.pop_back();//a
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
	GMatchSubgraph g;
	load(argv[1], g);
    cout<<"loaded"<<endl;
    size_t count = graph_matching(g);
    cout<<"# of matched subgraphs:"<<count<<endl;
    return 0;
}
