#ifndef GLOBAL_H
#define GLOBAL_H

#include <mpi.h>
#include <stddef.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include <assert.h> //for ease of debug
#include <sys/stat.h>
#include <ext/hash_set>
#include <ext/hash_map>

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include <atomic>

//============================
#include <time.h>

#define POLLING_TIME 100000 //unit: usec, user-configurable, used by sender
static clock_t polling_ticks = POLLING_TIME * CLOCKS_PER_SEC / 1000000;

#define WAIT_TIME_WHEN_IDLE 100 //unit: usec, user-configurable, used by recv-er

#define TASK_BATCH_NUM 50 //minimal number of tasks processed as a unit

#define GRAPH_LOAD_CHANNEL 200
#define REQ_CHANNEL 201
#define RESP_CHANNEL 202

void* global_trimmer = NULL;

//============================

#define hash_map __gnu_cxx::hash_map
#define hash_set __gnu_cxx::hash_set

using namespace std;

atomic<bool> global_end_label(false);

//============================
///worker info
#define MASTER_RANK 0

int _my_rank;
int _num_workers;
inline int get_worker_id()
{
    return _my_rank;
}
inline int get_num_workers()
{
    return _num_workers;
}

void init_worker(int * argc, char*** argv)
{
	int provided;
	MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
	if(provided != MPI_THREAD_MULTIPLE)
	{
	    printf("MPI do not Support Multiple thread\n");
	    exit(0);
	}
	MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
	MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
}

void worker_finalize()
{
    MPI_Finalize();
}

void worker_barrier()
{
    MPI_Barrier(MPI_COMM_WORLD); //only usable before creating threads
}

//------------------------
// worker parameters

struct WorkerParams {
	string local_root;
    string input_path;
    string output_path;
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    WorkerParams()
    {
    	local_root = "tmp";
        force_write = true;
        native_dispatcher = false;
    }
};

//============================
//general types
typedef int VertexID;

void* global_aggregator = NULL;
inline void set_aggregator(void* ag)
{
    global_aggregator = ag;
}
inline void* get_aggregator()
{
    return global_aggregator;
}

void* global_agg = NULL; //for aggregator, FinalT of previous round
inline void* getAgg()
{
    return global_agg;
}

//============================
//disk operations
void _mkdir(const char *dir) {//taken from: http://nion.modprobe.de/blog/archives/357-Recursive-directory-creation.html
	char tmp[256];
	char *p = NULL;
	size_t len;

	snprintf(tmp, sizeof(tmp), "%s", dir);
	len = strlen(tmp);
	if(tmp[len - 1] == '/') tmp[len - 1] = '\0';
	for(p = tmp + 1; *p; p++)
		if(*p == '/') {
				*p = 0;
				mkdir(tmp, S_IRWXU);
				*p = '/';
		}
	mkdir(tmp, S_IRWXU);
}

void _rmdir(string path){
    DIR* dir = opendir(path.c_str());
    struct dirent * file;
    while ((file = readdir(dir)) != NULL) {
        if(strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
        	continue;
        string filename = path + "/" + file->d_name;
        remove(filename.c_str());
    }
    if (rmdir(path.c_str()) == -1) {
    	perror ("The following error occurred");
        exit(-1);
    }
}

#endif
