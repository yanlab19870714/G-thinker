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

//Acknowledgements: this code is implemented based on pregel-mpi (https://code.google.com/p/pregel-mpi/) by Chuntao Hong.

#ifndef TIME_H
#define TIME_H

#include <sys/time.h>
#include <stdio.h>

#define StartTimer(i) start_timer((i))
#define StopTimer(i) stop_timer((i))
#define ResetTimer(i) reset_timer((i))
#define PrintTimer(str, i)              \
    if (get_worker_id() == MASTER_RANK) \
        printf("%s : %f seconds\n", (str), get_timer((i)));

double get_current_time()
{
    timeval t;
    gettimeofday(&t, 0);
    return (double)t.tv_sec + (double)t.tv_usec / 1000000;
}

const int N_Timers = 10; //currently, 5 timers are available
static double _timers[N_Timers]; // timers
static double _acc_time[N_Timers]; // accumulated time

void init_timers()
{
    for (int i = 0; i < N_Timers; i++) {
        _acc_time[i] = 0;
    }
}

enum TIMERS {
    WORKER_TIMER = 0,
    SERIALIZATION_TIMER = 1,
    TRANSFER_TIMER = 2,
    COMMUNICATION_TIMER = 3
};
//currently, only 4 timers are used, others can be defined by users

void start_timer(int i)
{
    _timers[i] = get_current_time();
}

void reset_timer(int i)
{
    _timers[i] = get_current_time();
    _acc_time[i] = 0;
}

void stop_timer(int i)
{
    double t = get_current_time();
    _acc_time[i] += t - _timers[i];
}

double get_timer(int i)
{
    return _acc_time[i];
}

#endif
