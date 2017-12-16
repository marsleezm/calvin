/*
 * logging.h
 *
 *  Created on: 24/03/2017
 *      Author: li
 */

#include <iostream>
#include <fstream>
#include <thread>

#ifndef COMMON_LOGGING_H_
#define COMMON_LOGGING_H_

struct None { };

static pthread_mutex_t stdout_mutex;
static std::ofstream ofs; // ("test.txt", std::ofstream::out);

template <typename First,typename Second>
struct Pair {
  First first;
  Second second;
};

static inline void OpenFile(string num){
    std::string filename = num+"test.txt";
    ofs.open(filename.c_str());
    std::cout.rdbuf(ofs.rdbuf());
}

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

inline void printList(std::ostream &os, None)
{
}


template <typename Begin,typename Last>
void printList(std::ostream &os,const Pair<Begin,Last> &data)
{
  printList(os, data.first);
  os << data.second;
}

template <typename List>
inline void log(const char *file,int line, int64 tx_id, const LogData<List> &data)
{
    pthread_mutex_lock(&stdout_mutex);
    std::cout << std::this_thread::get_id() << "--" << line << "): "<<tx_id<<" ";
    printList(std::cout, data.list);
    std::cout<< std::endl;
    pthread_mutex_unlock(&stdout_mutex);
    /*
    pthread_mutex_lock(&stdout_mutex);
    ofs << std::this_thread::get_id() << "--" << line << "): "<<tx_id<<" ";
    printList(ofs, data.list);
    ofs << std::endl;
    pthread_mutex_unlock(&stdout_mutex);
    */
}

//#define LOCKLOGGING
//#define AGGRLOGGING
#define ALLLOGGING
#define DOASSERT

#ifdef DOASSERT
#define ASSERT(x) (assert(x))
#else
#define ASSERT(x)
#endif

#ifdef ALLLOGGING
#define LOG(txid, x) (log(__FILE__,__LINE__, txid, LogData<None>() << x))
#define PLOG(txid, x) (log(__FILE__,__LINE__, txid, LogData<None>() << x))
#define LOCKLOG(txid, x) (log(__FILE__,__LINE__, txid, LogData<None>() << x))
#define AGGRLOG(txid, x) (log(__FILE__,__LINE__, txid, LogData<None>() << x))
#else

#ifdef LOCKLOGGING
#define LOG(txid, x)
#define LOCKLOG(txid, x) (log(__FILE__,__LINE__, txid, LogData<None>() << x))
#define PLOG(x)
#define AGGRLOG(txid, x)
#else
#ifdef AGGRLOGGING
#define LOG(txid, x)
#define AGGRLOG(txid, x) (log(__FILE__,__LINE__, txid, LogData<None>() << x))
#define PLOG(x)
#define LOCKLOG(txid, x)
#else
#define LOG(txid, x)
#define LOCKLOG(txid, x)
#define PLOG(x)
#define AGGRLOG(txid, x)
#endif
#endif
#endif

#endif /* COMMON_LOGGING_H_ */
