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

#include "subg-dev.h"

typedef vector<VertexID> CliqueValue;
typedef Vertex<VertexID, CliqueValue> CliqueVertex;
typedef Subgraph<CliqueVertex> CliqueSubgraph;
typedef hash_set<VertexID> VSet;

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
    VSet Rp_set;
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
void expand(vector<CliqueVertex> & vertexes, vector<int> & N, VSet & Q, VSet & Qmax) //vertexes = R in the paper
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
	    	set<VertexID> nbs_set; //somehow, using VSet would be very slow for a sparse graph, due to hash_set setup time
            for(int pos = 0; pos < nbs.size(); pos++) nbs_set.insert(nbs[pos]);
            for(int i = 0; i < vertexes.size(); i++)
            {
                CliqueVertex & v = vertexes[i];
                if(nbs_set.find(v.id) != nbs_set.end()) Rp_pts.push_back(&v);
            }
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

void MCQ(CliqueSubgraph & g, VSet & Qmax)
{
    vector<CliqueVertex> vvec;
    vector<int> N;
    degree_sort(g.vertexes, vvec, N);
    VSet Q;
    expand(vvec, N, Q, Qmax);
    
    /*//report:
    cout<<"maximum clique:";
    for(VSet::iterator it=Qmax.begin(); it!=Qmax.end(); it++) cout<<" "<<*it;
    cout<<endl;
    //*/
}
