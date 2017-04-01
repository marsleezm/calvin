/*
 * logging.h
 *
 *  Created on: 24/03/2017
 *      Author: li
 */

#include <iostream>
#include <thread>

#ifndef COMMON_LOGGING_H_
#define COMMON_LOGGING_H_

struct None { };

static pthread_mutex_t stdout_mutex;

template <typename First,typename Second>
struct Pair {
  First first;
  Second second;
};

template <typename List>
struct LogData {
  List list;
};

template <typename Begin,typename Value>
LogData<Pair<Begin,const Value &>>
  operator<<(LogData<Begin> begin,const Value &value)
{
  return {{begin.list,value}};
}

template <typename Begin,size_t n>
LogData<Pair<Begin,const char *>>
  operator<<(LogData<Begin> begin,const char (&value)[n])
{
  return {{begin.list,value}};
}

inline void printList(std::ostream &os,None)
{
}


template <typename Begin,typename Last>
void printList(std::ostream &os,const Pair<Begin,Last> &data)
{
  printList(os,data.first);
  os << data.second;
}

template <typename List>
void log(const char *file,int line,const LogData<List> &data)
{
	pthread_mutex_lock(&stdout_mutex);
	//std::cout <<  std::this_thread::get_id() << "--"<< file << " (" << line << "): ";
	//std::cout << file << " (" << line << "): ";
	std::cout << line << "): ";
	printList(std::cout,data.list);
	//std::cout << "\n";
	std::cout<< std::endl;
	pthread_mutex_unlock(&stdout_mutex);
}

//#define ALLLOGGING
//#define DOASSERT

#ifdef DOASSERT
#define ASSERT(x) (assert(x))
#else
#define ASSERT(x)
#endif

#ifdef ALLLOGGING
#define LOG(x) (log(__FILE__,__LINE__,LogData<None>() << x))
#define PLOG(x) (log(__FILE__,__LINE__,LogData<None>() << x))
#define LOCKLOG(x) (log(__FILE__,__LINE__,LogData<None>() << x))

#else

#ifdef LOCKLOGING
#define LOG(x)
#define LOCKLOG(x) (log(__FILE__,__LINE__,LogData<None>() << x))
#define PLOG(x)
#else
#define LOG(x)
#define LOCKLOG(x)
#define PLOG(x)
#endif
#endif

#endif /* COMMON_LOGGING_H_ */
