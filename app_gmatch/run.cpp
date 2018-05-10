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
typedef Task<GMatchVertex, char> GMatchTask; //context = step

obinstream & operator>>(obinstream & m, AdjItem & v)
{
    m >> v.id;
    m >> v.l;
    return m;
}

ibinstream & operator<<(ibinstream & m, const AdjItem & v)
{
    m << v.id;
    m << v.l;
    return m;
}

ofbinstream & operator>>(ofbinstream & m, AdjItem & v)
{
    m >> v.id;
    m >> v.l;
    return m;
}

ifbinstream & operator<<(ifbinstream & m, const AdjItem & v)
{
    m << v.id;
    m << v.l;
    return m;
}

//------------------
obinstream & operator>>(obinstream & m, GMatchValue & Val)
{
    m >> Val.l;
    m >> Val.adj;
    return m;
}

ibinstream & operator<<(ibinstream & m, const GMatchValue & Val)
{
    m << Val.l;
    m << Val.adj;
    return m;
}

ofbinstream & operator>>(ofbinstream & m, GMatchValue & Val)
{
    m >> Val.l;
    m >> Val.adj;
    return m;
}

ifbinstream & operator<<(ifbinstream & m, const GMatchValue & Val)
{
    m << Val.l;
    m << Val.adj;
    return m;
}
//-------------------
// add a node to graph: only id and label of v, not its edges
// must make sure g.hasVertex(v.id) == true !!!!!!
void addNode(GMatchSubgraph & g, GMatchVertex & v)
{
	GMatchVertex temp_v;
	temp_v.id = v.id;
	temp_v.value.l = v.value.l;
	g.addVertex(temp_v);
}

void addNode(GMatchSubgraph & g, VertexID id, Label l)
{
	GMatchVertex temp_v;
	temp_v.id = id;
	temp_v.value.l = l;
	g.addVertex(temp_v);
}

// add a edge to graph
// must make sure id1 and id2 are added first !!!!!!
void addEdge(GMatchSubgraph & g, VertexID id1, VertexID id2)
{
    GMatchVertex * v1, * v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    AdjItem temp_adj;
	temp_adj.id = v2->id;
	temp_adj.l = v2->value.l;
	v1->value.adj.push_back(temp_adj);
	temp_adj.id = v1->id;
	temp_adj.l = v1->value.l;
	v2->value.adj.push_back(temp_adj);
}

void addEdge_safe(GMatchSubgraph & g, VertexID id1, VertexID id2) //avoid redundancy
{
    GMatchVertex * v1, * v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    int i = 0;
    vector<AdjItem> & adj = v2->value.adj;
    for(; i<adj.size(); i++)
    	if(adj[i].id == id1) break;
    if(i == adj.size())
    {
    	AdjItem temp_adj;
		temp_adj.id = v2->id;
		temp_adj.l = v2->value.l;
		v1->value.adj.push_back(temp_adj);
		temp_adj.id = v1->id;
		temp_adj.l = v1->value.l;
		v2->value.adj.push_back(temp_adj);
    }
}

class GMatchAgg:public Aggregator<size_t, size_t, size_t>  //all args are counts
{
private:
	size_t count;
	size_t sum;

public:

    virtual void init()
    {
    	sum = count = 0;
    }

    virtual void init_udf(size_t & prev) {
    	sum = 0;
    }

    virtual void aggregate_udf(size_t & task_count)
    {
    	count += task_count;
    }

    virtual void stepFinal_udf(size_t & partial_count)
    {
    	sum += partial_count; //add all other machines' counts (not master's)
    }

    virtual void finishPartial_udf(size_t & collector)
    {
    	collector = count;
    }

    virtual void finishFinal_udf(size_t & collector)
    {
    	sum += count; //add master itself's count
    	if(_my_rank == MASTER_RANK) cout<<"the # of matched graph = "<<sum<<endl;
    	collector = sum;
    }
};

class GMatchTrimmer:public Trimmer<GMatchVertex>
{
    virtual void trim(GMatchVertex & v) {
    	vector<AdjItem> & val = v.value.adj;
    	vector<AdjItem> newval;
        for (int i = 0; i < val.size(); i++) {
            if (val[i].l == 'a' || val[i].l == 'b' || val[i].l == 'c' || val[i].l == 'd')
            	newval.push_back(val[i]);
        }
        val.swap(newval);
    }
};

size_t graph_matching(GMatchSubgraph & g)
{
	size_t count = 0;
	GMatchVertex & v_a = g.vertexes[0];
	vector<AdjItem> & a_adj = v_a.value.adj;
	vector<VertexID> GMatchQ; //record matched vertex instances
	//------
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
	return count;
}

class GMatchComper:public Comper<GMatchTask, GMatchAgg>
{
public:
    virtual void task_spawn(VertexT * v)
    {
    	if(v->value.l == 'a')
    	{
    		GMatchTask * t = new GMatchTask;
			addNode(t->subG, *v);
			t->context = 1; //context is step number
			vector<AdjItem> & nbs = v->value.adj;
			bool has_b = false;
			bool has_c = false;
			for(int i=0; i<nbs.size(); i++)
				if(nbs[i].l == 'b')
				{
					t->pull(nbs[i].id);
					has_b = true;
				}
				else if(nbs[i].l == 'c')
				{
					t->pull(nbs[i].id);
					has_c = true;
				}
			if(has_b && has_c) add_task(t);
            else delete t;
    	}
    }

    virtual bool compute(SubgraphT & g, ContextT & context, vector<VertexT *> & frontier)
    {
    	//match c
    	if(context == 1) //context is step number
    	{
    		VertexID rootID = g.vertexes[0].id; //root = a-matched vertex
			//cout<<rootID<<": in compute"<<endl;//@@@@@@@@@@@@@
			hash_set<VertexID> label_b; //Vb (set of IDs)
			vector<VertexT *> label_c; //Vc
			for(int i = 0; i < frontier.size(); i++) {//set Vb and Vc from frontier
				if(frontier[i]->value.l == 'b')
					label_b.insert(frontier[i]->id);
				else if(frontier[i]->value.l == 'c')
					label_c.push_back(frontier[i]);
			}
			//------
			hash_set<VertexID> bList; //vertices to pull
			for(int i = 0 ; i < label_c.size(); i++)
			{
				VertexT * node_c = label_c[i]; //get v_c
				vector<AdjItem> & c_nbs = node_c->value.adj; //get v_c's adj-list
				vector<VertexID> U1, U2;
				for(int j = 0; j < c_nbs.size(); j++) //set U1 & U2
				{
					AdjItem & nb_c = c_nbs[j];
					if(nb_c.l == 'b')
					{
						VertexID b_id = nb_c.id;
						if(label_b.find(b_id) != label_b.end())
							U1.push_back(b_id);
						else
							U2.push_back(b_id);
					}
				}
				//------
				if(U1.empty())
					continue;
				else if(U1.size() == 1)
				{
					if(U2.empty()) continue;
					else
						bList.insert(U2.begin(), U2.end());
				}
				else
				{
					bList.insert(U1.begin(), U1.end());
					bList.insert(U2.begin(), U2.end());
				}
				//------
				//add v_c and edges (v_a, v_c)
				addNode(g, *node_c);
				addEdge(g, rootID, node_c->id);
				//------
				for(int j = 0; j < U1.size(); j++)
				{
					if(!g.hasVertex(U1[j]))
					{
						addNode(g, U1[j], 'b'); //add v_b
						addEdge(g, rootID, U1[j]); //add (v_a, v_b)
					}
					addEdge(g, node_c->id, U1[j]); //add (v_c, v_b) //this is forgotten to mention in CoRR's version
				}
			}
			//pull bList
			for(auto it = bList.begin(); it != bList.end(); it++) pull(*it);
			//cout<<rootID<<": step 1 done"<<endl;//@@@@@@@@@@@@@
			//------
			context++;
			return true;
    	}
    	else //step == 2
    	{
    		hash_set<VertexID> Vc;
    		//get Vc from g
    		for(int i=0; i<g.vertexes.size(); i++)
    		{
    			VertexT & v = g.vertexes[i];
    			if(v.value.l == 'c') Vc.insert(v.id);
    		}
    		//------
    		for(int i=0; i<frontier.size(); i++)
    		{
    			VertexT* v_b = frontier[i];
    			vector<AdjItem> & adj_b = v_b->value.adj;
    			//construct Vd
    			vector<VertexID> Vd;
    			for(int j=0; j<adj_b.size(); j++)
    			{
    				if(adj_b[j].l == 'd') Vd.push_back(adj_b[j].id);
    			}
    			//------
    			if(!Vd.empty())
    			{
    				//add v_b
    				if(!g.hasVertex(v_b->id)) addNode(g, *v_b);
					//add (v_b, v_c), where v_c \in bList
					for(int j=0; j<adj_b.size(); j++)
					{
						VertexID b_id = adj_b[j].id; //v_c
						if(Vc.find(b_id) != Vc.end())
						{
							//add edge v_b, v_c
							addEdge_safe(g, b_id, v_b->id);
						}
					}
    				//add v_d and (v_b, v_d)
    				for(int j=0; j<Vd.size(); j++)
    				{
    					if(!g.hasVertex(Vd[j])) addNode(g, Vd[j], 'd');
    					addEdge(g, Vd[j], v_b->id);
    				}
    			}
    		}
    		//run single-threaded mining code
			size_t count = graph_matching(g);
			GMatchAgg* agg = get_aggregator();
			agg->aggregate(count);
			//cout<<rootID<<": step 2 done"<<endl;//@@@@@@@@@@@@@
			return false;
    	}
    }
};

class GMatchWorker:public Worker<GMatchComper>
{
public:
	GMatchWorker(int num_compers) : Worker<GMatchComper>(num_compers){}

    virtual VertexT* toVertex(char* line)
    {
        VertexT* v = new VertexT;
        char * pch;
        pch = strtok(line, " \t");
        v->id = atoi(pch);
        pch = strtok(NULL, " \t");
        v->value.l = *pch;
        vector<AdjItem> & nbs = v->value.adj;
        AdjItem temp;
        while((pch=strtok(NULL, " ")) != NULL)
        {
        	temp.id = atoi(pch);
        	pch = strtok(NULL, " ");
        	temp.l = *pch;
        	nbs.push_back(temp);
        }
        return v;
    }

    virtual void task_spawn(VertexT * v, vector<GMatchTask> & tcollector)
	{
    	if(v->value.l != 'a') return;
    	GMatchTask t;
    	addNode(t.subG, *v);
    	t.context = 1;
		vector<AdjItem> & nbs = v->value.adj;
		bool has_b = false;
		bool has_c = false;
		for(int i=0; i<nbs.size(); i++)
			if(nbs[i].l == 'b')
			{
				t.pull(nbs[i].id);
				has_b = true;
			}
			else if(nbs[i].l == 'c')
			{
				t.pull(nbs[i].id);
				has_c = true;
			}
		if(has_b && has_c) tcollector.push_back(t);
	}
};

int main(int argc, char* argv[])
{
    init_worker(&argc, &argv);
    WorkerParams param;
    param.input_path = argv[1];  //input path in HDFS
    int thread_num = atoi(argv[2]);  //number of threads per process
    param.force_write=true;
    param.native_dispatcher=false;
    //------
	GMatchTrimmer trimmer;
    GMatchAgg aggregator;
    GMatchWorker worker(thread_num);
	worker.setTrimmer(&trimmer);
    worker.setAggregator(&aggregator);
    worker.run(param);
    worker_finalize();
    return 0;
}
