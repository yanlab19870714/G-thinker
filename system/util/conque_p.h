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

#ifndef CONQUE_P_H
#define CONQUE_P_H

//algo: implemented according to http://www.parallellabs.com/2010/10/25/practical-concurrent-queue-algorithm

//version of conque where value is a pointer (to avoid copy)

#include <mutex>
#include <atomic>
#include <condition_variable>

using namespace std;

template <typename T> struct node_p
{
    T* value;
    atomic<node_p<T> *> next;
};

template <typename T> class conque_p
{
public:
    typedef node_p<T> NODE;
    NODE *head; //locked by q_h_lock
    NODE *tail;
    mutex q_h_lock;
    mutex q_t_lock;
    
    conque_p() {
    	head = tail = new NODE;
        head->next = NULL;
    }
    
    conque_p(const conque_p<T>& q) {
    	//required due to RespQueue's initialization calls resize(.)
		//the trouble is caused by mutex-elements, which cannot be copied
		//the function is not actually called, we need to define it just because resize(.) may copy
		cout << "conque's copy constructor called, should never happen !!!"<<endl;
		exit(-1);
		head = tail = new NODE;
		head->next = NULL;
	}

    void enqueue(T* value) {
    	NODE *node = new NODE;
        node->value = value;
        node->next = NULL;
        lock_guard<mutex> lck(q_t_lock);
        tail->next.store(node, memory_order_relaxed); //atomic "store"
        tail = node;
    }
    
    T* dequeue() {
    	unique_lock<mutex> lck(q_h_lock);
        NODE *node = head;
        NODE *new_head = node->next.load(memory_order_relaxed); //atomic "load"
        if(new_head == NULL) //queue is empty
        {
        	lck.unlock();
        	return NULL;
        }
        head = new_head;
        lck.unlock();
        delete node;
        return head->value;
    }
};

#endif
