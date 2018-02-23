#include "Vertex.h"
#include "Subgraph.h"
#include <fstream>
#include <iostream>
#include <algorithm>
#include <chrono>
#include <set>
#include <ext/hash_set>
#define hash_set __gnu_cxx::hash_set

using namespace std;
typedef int VertexID;

using get_time = chrono::high_resolution_clock;
using ms = chrono::microseconds;
double f_elapsed=0;

typedef vector<VertexID> CliqueValue;
typedef Vertex<VertexID, CliqueValue> CliqueVertex;
typedef Subgraph<CliqueVertex> CliqueSubgraph;

void load(char* fname, CliqueSubgraph & g) {
	ifstream in(fname);
	int temp, num;
	while(in >> temp)
	{
		CliqueVertex v;
		v.id = temp;
		in >> num;
		CliqueValue nbs;
		for(int i=0; i<num; i++)
		{
			in >> temp;
			nbs.push_back(temp);
		}
		v.value.swap(nbs);
		g.addVertex(v);
	}
    in.close();
}

//define comparator for sorting
struct deg_comparator{
    bool operator()(const CliqueVertex * u, const CliqueVertex * v) const
    {
        return u->value.size() > v->value.size();
    }
}; //for sorting by degree (descending order)

void degree_sort(vector<CliqueVertex> & vertexes, vector<CliqueVertex> & vout, vector<int> & N)
{
    //in: vertexes
    //out: N
    //side effect: "vertexes" is sorted by degree
    vector<CliqueVertex *> vvec(vertexes.size());
    for(int i=0; i<vvec.size(); i++) vvec[i] = &vertexes[i];
    //--------------    
    //sort by degree
	deg_comparator comper;
    sort(vvec.begin(), vvec.end(), comper);
    //get max-degree (we require "vertexes" not empty)
    int max_deg = vvec[0]->value.size();
    int size = vertexes.size();
    //set vout
    for(int i=0; i<size; i++)
    {
        vout.push_back(*vvec[i]);
    }
    //set N-array
    N.resize(size);
    if(size > max_deg)
    {
        int i = 0;
        for(; i<max_deg; i++) N[i] = i + 1;
        for(; i<size; i++) N[i] = max_deg + 1;
    }
    else
        for(int i=0; i<size; i++) N[i] = max_deg + 1;
}

//reference paper: an efficient branch-and-bound algorithm for finding a maximum clique
//NUMBER-SORT function
void color_sort(vector<CliqueVertex> & vertexes, vector<int> & N)
{
    //{NUMBER}
    int maxno = 1;
    vector<hash_map<VertexID, CliqueVertex *> > C;
    hash_map<VertexID, int> Nmap;
    C.resize(3);
    for(int cur = 0; cur < vertexes.size(); cur++)
    {
        CliqueVertex & p = vertexes[cur];
        int k = 1;
        CliqueValue & nbs = p.value;
        //coloring
        bool stop = false;
        while(!stop)
        {
            int pos = 0; //position in neighbor list
            for(; pos < nbs.size(); pos++)
            {
                VertexID nb = nbs[pos];
                if(C[k].find(nb) != C[k].end()) break; //find a neighbor in Ck
            }
            if(pos == nbs.size()) stop = true;
            else k++;
        }
        //check k
        if(k > maxno)
        {
            maxno = k;
            C.resize(k + 2);
        }
        Nmap[p.id] = k;
        C[k][p.id] = &p;
    }
    //{SORT}
    vector<CliqueVertex> temp;
    for(int i=1; i<=maxno; i++)
    {
        hash_map<VertexID, CliqueVertex *> & set = C[i];
        for(hash_map<VertexID, CliqueVertex *>::iterator it = set.begin(); it!=set.end(); it++)
        {
            temp.push_back(*(it->second));
            N.push_back(Nmap[it->first]);
        }
    }
    temp.swap(vertexes);
}

void nbs_prune(vector<CliqueVertex *> & Rp_pts, vector<CliqueVertex> & Rp)
{
    hash_set<VertexID> Rp_set;
    for(int i = 0; i<Rp_pts.size(); i++)  Rp_set.insert(Rp_pts[i]->id);
    for(int i = 0; i<Rp_pts.size(); i++)
    {
        Rp.resize(Rp.size() + 1);
        CliqueVertex & u = Rp.back();
        u.id = Rp_pts[i]->id;
        vector<VertexID> & nb_list = Rp_pts[i]->value;
        for(int j = 0; j < nb_list.size(); j++)
        {
            if(Rp_set.find(nb_list[j]) != Rp_set.end())
                u.value.push_back(nb_list[j]);
        }
    }
}

//EXPAND
void expand(vector<CliqueVertex> & vertexes, vector<int> & N, hash_set<VertexID> & Q, hash_set<VertexID> & Qmax) //vertexes = R in the paper
{
	while(!vertexes.empty())
	{
       	CliqueVertex & p = vertexes.back(); //***
		if(Q.size() + N.back() > Qmax.size())
		{
			Q.insert(p.id);
			vector<CliqueVertex *> Rp_pts;
            //compute Rp = vertexes intersects nbs(p)
	    	CliqueValue & nbs = p.value;
            set<VertexID> nbs_set;
            auto start = get_time::now();
            for(int pos = 0; pos < nbs.size(); pos++) nbs_set.insert(nbs[pos]);
            for(int i = 0; i < vertexes.size(); i++)
            {
                CliqueVertex & v = vertexes[i];
                if(nbs_set.find(v.id) != nbs_set.end()) Rp_pts.push_back(&v);
            }
            auto end = get_time::now();
            f_elapsed = f_elapsed + chrono::duration_cast<ms>(end - start).count();
			if(!Rp_pts.empty())
			{
                vector<CliqueVertex> Rp;
				nbs_prune(Rp_pts, Rp);
                vector<int> colorN;
				color_sort(Rp, colorN);
				expand(Rp, colorN, Q, Qmax);
			}
            else if(Q.size() > Qmax.size()) Qmax = Q;
			Q.erase(p.id);
		}
        vertexes.pop_back(); //***
        N.pop_back();
	}
}

//MCQ
void MCQ(CliqueSubgraph & g)
{
    vector<CliqueVertex> vvec;
    vector<int> N;
    degree_sort(g.vertexes, vvec, N);
    hash_set<VertexID> Qmax,Q;
    expand(vvec, N, Q, Qmax);
    
    //report:
    cout<<"maximum clique:";
    for(hash_set<VertexID>::iterator it=Qmax.begin(); it!=Qmax.end(); it++) cout<<" "<<*it;
    cout<<endl<<"size = "<<Qmax.size()<<endl;
}

int main(int argc, char* argv[])
{
    if(argc != 2)
    {
        cout<<"arg1 = input file name"<<endl;
        return -1;
    }
	CliqueSubgraph g;
	load(argv[1], g);
    
    cout<<"loaded"<<endl;
    /*
	//report:
	for(int i=0; i<vertexes.size(); i++)
	{
		CliqueVertex & v = vertexes[i];
		cout<<v.id<<":";
		for(int j=0; j<v.value.size(); j++) cout<<" "<<v.value[j];
		cout<<endl;
    }*/
    
    auto Start = get_time::now();
    MCQ(g);
    auto  End = get_time::now();
    auto elapsedTime = chrono::duration_cast<ms>(End - Start);
    cout<<"time: "<<elapsedTime.count()<<endl;
    cout<<"filter time: "<<f_elapsed<<endl;
    return 0;
}
