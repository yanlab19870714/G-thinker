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

#ifndef AGGREGATOR_H_
#define AGGREGATOR_H_

#include <stddef.h>
#include <util/rwlock.h>
using namespace std;

template <class ValueT, class PartialT, class FinalT>
class Aggregator {
public:
	rwlock lock;
    typedef PartialT PartialType;
    typedef FinalT FinalType;

    virtual void init() = 0;

    virtual void init_udf(FinalT & prev) = 0;
    //called right after agg-sync, prev is the previously sync-ed value
    void init_aggSync(FinalT & prev)
	{
		lock.wrlock();
		init_udf(prev);
		lock.unlock();
	}

    virtual void aggregate_udf(ValueT & context) = 0;
    void aggregate(ValueT & context)
    {
    	lock.wrlock();
    	aggregate_udf(context);
    	lock.unlock();
    }

    virtual void stepFinal_udf(PartialT & part) = 0;
    void stepFinal(PartialT & part)
    {
    	lock.wrlock();
    	stepFinal_udf(part);
    	lock.unlock();
    }

    virtual void finishPartial_udf(PartialT & collector) = 0;
    virtual void finishPartial(PartialT & collector)
    {
    	lock.rdlock();
    	finishPartial_udf(collector);
    	lock.unlock();
    }

    virtual void finishFinal_udf(FinalT & collector) = 0;
    virtual void finishFinal(FinalT & collector)
    {
    	lock.rdlock();
    	finishFinal_udf(collector);
    	lock.unlock();
    }
};

class DummyAgg : public Aggregator<char, char, char> {
public:
    virtual void init()
    {
    }
    virtual void init(char& prev)
	{
	}
    virtual void aggregate_udf(char & v)
    {
    }
    virtual void stepFinal_udf(char & part)
    {
    }
    virtual void finishPartial_udf(char & collector)
    {
    }
    virtual void finishFinal_udf(char & collector)
    {
    }
};

#endif /* AGGREGATOR_H_ */
