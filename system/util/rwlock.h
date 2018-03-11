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

#ifndef RWLOCK_H
#define RWLOCK_H

#include <pthread.h>
#include <assert.h>
#include <stddef.h> //for NULL

class rwlock
{
	pthread_rwlock_t lock;

public:
    
	rwlock()
	{
		int ret = pthread_rwlock_init(&lock, NULL);
		assert(ret == 0);
	}

	~rwlock()
	{
		int ret = pthread_rwlock_destroy(&lock);
		assert(ret == 0);
	}

	void rdlock()
	{
		int ret = pthread_rwlock_rdlock(&lock);
		assert(ret == 0);
	}

	void wrlock()
	{
		int ret = pthread_rwlock_wrlock(&lock);
		assert(ret == 0);
	}

	void unlock()
	{
		int ret = pthread_rwlock_unlock(&lock);
		assert(ret == 0);
	}
};

#endif
