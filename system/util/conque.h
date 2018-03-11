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

#ifndef CONQUE_H
#define CONQUE_H

//algo: implemented according to http://www.parallellabs.com/2010/10/25/practical-concurrent-queue-algorithm

#include <mutex>
#include <atomic>
#include <condition_variable>

#include <iostream>

using namespace std;

template <typename T> struct node_t
{
    T value;
    atomic<node_t<T> *> next;
};

template <typename T> class conque
{
public:
    typedef node_t<T> NODE;
    NODE *head; //locked by q_h_lock
    NODE *tail;
    mutex q_h_lock;
    mutex q_t_lock;
    
    conque() {
        head = tail = new NODE;
        head->next = NULL;
    }
    
    conque(const conque<T>& q) {
    	//required due to ReqQueue's initialization calls resize(.)
    	//the trouble is caused by mutex-elements, which cannot be copied
		//the function is not actually called, we need to define it just because resize(.) may copy
    	cout << "conque's copy constructor called, should never happen !!!"<<endl;
    	exit(-1);
		head = tail = new NODE;
		head->next = NULL;
	}

    void enqueue(T value) {
        NODE *node = new NODE;
        node->value = value;
        node->next = NULL;
        lock_guard<mutex> lck(q_t_lock);
        tail->next.store(node, memory_order_relaxed); //atomic "store"
        tail = node;
    }
    
    bool dequeue(T & to_get) {
        unique_lock<mutex> lck(q_h_lock);
        NODE *node = head;
        NODE *new_head = node->next.load(memory_order_relaxed); //atomic "load"
        if(new_head == NULL) //queue is empty
        {
        	lck.unlock();
        	return false;
        }
        to_get = new_head->value;
        head = new_head;
        lck.unlock();
        delete node;
        return true;
    }

    bool empty()
    {
    	unique_lock<mutex> lck(q_h_lock);
		NODE *node = head->next.load(memory_order_relaxed); //atomic "load"
		lck.unlock();
		if(node == NULL) return true;
		else return false;
    }
};

#endif
