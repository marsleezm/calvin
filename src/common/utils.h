// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// TODO(alex): UNIT TESTING!

#ifndef _DB_COMMON_UTILS_H_
#define _DB_COMMON_UTILS_H_

#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <cmath>
#include <vector>
#include <tr1/unordered_map>
#include <atomic>
#include <climits>
#include <iostream>
#include <sstream>

#include "common/types.h"
#include "common/logging.h"

//using std::string;
//using std::vector;
using namespace std;
using tr1::unordered_map;


#define DEPENDENT_MASK 8

#define NO_LOCK INT_MAX
#define GC_THRESHOLD 100

#define NUM_THREADS 5

#define ASSERTS_ON true

#define DCHECK(ARG) do { if (ASSERTS_ON) assert(ARG); } while (0)
#define BUFFER_TXNS_NUM 20

#define NORMAL_QUEUE 0
#define FROM_SELF 1
#define FROM_SEQ_SINGLE 2
#define FROM_SEQ_DIST 3

#define NORMAL 0
#define SKIP 1
#define SPECIAL 2

// Tranasction execution return
#define FINISHED 3
#define WAIT_AND_SENT 4
#define WAIT_NOT_SENT 5
#define TX_ABORTED 6
#define SUSPENDED 7

#define IS_COPY 2
#define NOT_COPY 4
#define WRITE 8

#define NEW_MASK 1

#define NEW_COPY 3
#define NEW_NOT_COPY 5
#define NEW_WRITE 9

#define LOCKED 1
#define LOCK_FAILED 2
#define NO_NEED 3


#define FULL_READ(storage, key, object, read_state, val, new_object) \
read_state = NORMAL; \
val = storage->ReadValue(key, read_state, new_object); \
if (read_state == SPECIAL) return reinterpret_cast<int64>(val); \
else assert(object.ParseFromString(*val));

// It happens after a transaction resumes from suspension; we know that
// the value of the key will not be needed anymore, so no need do parse it
/*#define PART_READ(storage, key, object, read_state, val) \
read_state = NORMAL; \
val = storage->ReadValue(key, read_state); \
if (read_state == SPECIAL) return reinterpret_cast<int64>(val); \
else if(read_state == NORMAL) assert(object.ParseFromString(*val));
*/

#define PART_READ(storage, type, key, read_state, val) \
if(storage->ShouldRead()){	\
	val = storage->ReadValue(key, read_state, false);	\
	if (read_state == SPECIAL) return reinterpret_cast<int64>(val);	\
	else {	\
		type obj;	\
		assert(obj.ParseFromString(*val));	}}\


// Status code for return values.
struct Status {
  // Represents overall status state.
  enum Code {
    ERROR = 0,
    OKAY = 1,
    DONE = 2,
  };
  Code code;

  // Optional explanation.
  string message;

  // Constructors.
  explicit Status(Code c) : code(c) {}
  Status(Code c, const string& s) : code(c), message(s) {}
  static Status Error() { return Status(ERROR); }
  static Status Error(const string& s) { return Status(ERROR, s); }
  static Status Okay() { return Status(OKAY); }
  static Status Done() { return Status(DONE); }

  // Pretty printing.
  string ToString() {
    string out;
    if (code == ERROR) out.append("Error");
    if (code == OKAY) out.append("Okay");
    if (code == DONE) out.append("Done");
    if (message.size()) {
      out.append(": ");
      out.append(message);
    }
    return out;
  }
};

// Returns the number of seconds since midnight according to local system time,
// to the nearest microsecond.
static inline double GetTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec/1e6;
}

// Returns the number of seconds since midnight according to local system time,
// to the nearest microsecond.
static inline int64 GetUTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec*1e6 + tv.tv_usec;
}

// Busy-wait for 'duration' seconds.
static inline void Spin(double duration) {
  usleep(1000000 * duration);
//  double start = GetTime();
//  while (GetTime() < start + duration) {}
}

static inline string to_str(vector<string> s){
	string str = s[0];
	for(uint i = 1; i < s.size(); ++i){
		str += "," + s[i];
	}
	return str;
}

//static inline string ps_to_str(::google::protobuf::RepeatedPtrField<string> s){
//	string str = s[0];
//	for(uint i = 1; i < s.size(); ++i){
//		str += "," + s[i];
//	}
//	return str;
//}

// Busy-wait until GetTime() >= time.
static inline void SpinUntil(double time) {
  while (GetTime() >= time) {}
}

// Produces a random alphabet string of the specified length
static inline string RandomString(int length) {
  string random_string;
  for (int i = 0; i < length; i++)
    random_string += rand() % 26 + 'A';

  return random_string;
}

// Returns a human-readable string representation of an int.
static inline string IntToString(int n) {
  char s[64];
  snprintf(s, sizeof(s), "%d", n);
  return string(s);
}

// Converts a human-readable numeric string to an int.
static inline int StringToInt(const string& s) {
  return atoi(s.c_str());
}

static inline string DoubleToString(double n) {
  char s[64];
  snprintf(s, sizeof(s), "%lf", n);
  return string(s);
}

static inline double StringToDouble(const string& s) {
  return atof(s.c_str());
}

static inline double RandomDoubleBetween(double fMin, double fMax) {
  double f = (double)rand()/RAND_MAX;
  return fMin + f*(fMax - fMin);
}

// Converts a human-readable numeric sub-string (starting at the 'n'th position
// of 's') to an int.
static inline int OffsetStringToInt(const string& s, int n) {
  return atoi(s.c_str() + n);
}

// Function for deleting a heap-allocated string after it has been sent on a
// zmq socket connection. E.g., if you want to send a heap-allocated
// string '*s' on a socket 'sock':
//
//  zmq::message_t msg((void*) s->data(), s->size(), DeleteString, (void*) s);
//  sock.send(msg);
//
static inline void DeleteString(void* data, void* hint) {
  delete reinterpret_cast<string*>(hint);
}
static inline void Noop(void* data, void* hint) {}


////////////////////////////////
template<typename T1, typename T2, typename T3>
class MyTuple{
public:
	T1 first;
	T2 second;
	T3 third;

	MyTuple(T1 t1, T2 t2, T3 t3){
		first = t1;
		second = t2;
		third = t3;
	}

	MyTuple(){}

	//MyTuple(const MyTuple& a) :
	//	first(a.first), first(a.second), first(a.third)
	//{
//
//	}
};

template<typename T1, typename T2, typename T3, typename T4>
class MyFour{
public:
	T1 first;
	T2 second;
	T3 third;
	T4 fourth;

	MyFour(T1 t1, T2 t2, T3 t3, T4 t4){
		first = t1;
		second = t2;
		third = t3;
		fourth = t4;
	}

	MyFour(){}
};




class Mutex {
 public:
  // Mutexes come into the world unlocked.
  Mutex() {
    pthread_mutex_init(&mutex_, NULL);
  }

 private:
  friend class Lock;
  // Actual pthread mutex wrapped by Mutex class.
  pthread_mutex_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  Mutex(const Mutex&);
  Mutex& operator=(const Mutex&);
};

class Lock {
 public:
  explicit Lock(Mutex* mutex) : mutex_(mutex) {
    pthread_mutex_lock(&mutex_->mutex_);
  }
  ~Lock() {
    pthread_mutex_unlock(&mutex_->mutex_);
  }

 private:
  Mutex* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  Lock();

  // DISALLOW_COPY_AND_ASSIGN
  Lock(const Lock&);
  Lock& operator=(const Lock&);
};


class Rand {
	int64_t x;
	int64_t y;
public:
	void seed(int64_t seed){
		x = seed;
	}

	int64_t next(){
	    x ^= x >> 12; // a
	    x ^= x << 25; // b
	    x ^= x >> 27; // c
	    x = x * 0x2545F4914F6CDD1D;
	    if (y == x){
	    	x += 1;
	    	y = x;
	    	return abs(x);
	    }
	    else{
	    	y = x;
	    	return abs(x);
	    }
	}
};


template<typename T>
class FixedList {
 public:
  // Mutexes come into the world unlocked.
  FixedList(int size) {
	  data_ = new T[size];
	  current_ = 0;
	  end_ = size-1;
  }

  ~FixedList() { delete data_;}

  T* GetFirst(){
	  if(current_ <= end_)
		  return data_[current_++];
	  else
		  return NULL;
  }

  void Clear(){
	  current_=0;
  }

  void Add(T* t){
	  data_[current_++] = t;
  }

 private:
  T* data_;
  int current_;
  int end_;
};

template<typename T>
class AtomicQueue {
 public:
  AtomicQueue() {
    queue_.resize(256);
    size_ = 256;
    front_ = 0;
    back_ = 0;
  }

  // Returns the number of elements currently in the queue.
  inline size_t Size() {
    Lock l(&size_mutex_);
    return (back_ + size_ - front_) % size_;
  }

  // Returns true iff the queue is empty.
  inline bool Empty() {
    return front_ == back_;
  }

  // Atomically pushes 'item' onto the queue.
  inline void Push(const T& item) {
    Lock l(&back_mutex_);
    // Check if the buffer has filled up. Acquire all locks and resize if so.
    if (front_ == (back_+1) % size_) {
      Lock m(&front_mutex_);
      Lock n(&size_mutex_);
      uint32 count = (back_ + size_ - front_) % size_;
      queue_.resize(size_ * 2);
      for (uint32 i = 0; i < count; i++) {
        queue_[size_+i] = queue_[(front_ + i) % size_];
      }
      front_ = size_;
      back_ = size_ + count;
      size_ *= 2;
    }
    // Push item to back of queue.
    queue_[back_] = item;
    back_ = (back_ + 1) % size_;
  }

  // If the queue is non-empty, (atomically) sets '*result' equal to the front
  // element, pops the front element from the queue, and returns true,
  // otherwise returns false.
  inline bool Pop(T* result) {
    Lock l(&front_mutex_);
    if (front_ != back_) {
      *result = queue_[front_];
      front_ = (front_ + 1) % size_;
      return true;
    }
    return false;
  }

  // Sets *result equal to the front element and returns true, unless the
  // queue is empty, in which case does nothing and returns false.
  inline bool Front(T* result) {
    Lock l(&front_mutex_);
    if (front_ != back_) {
      *result = queue_[front_];
      return true;
    }
    return false;
  }

  T Get(int i) {
	  Lock l(&size_mutex_);
	  return queue_[i];
  }

 private:
  vector<T> queue_;  // Circular buffer containing elements.
  uint32 size_;      // Allocated size of queue_, not number of elements.
  uint32 front_;     // Offset of first (oldest) element.
  uint32 back_;      // First offset following all elements.

  // Mutexes for synchronization.
  Mutex front_mutex_;
  Mutex back_mutex_;
  Mutex size_mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicQueue(const AtomicQueue<T>&);
  AtomicQueue& operator=(const AtomicQueue<T>&);
};



class MutexRW {
 public:
  // Mutexes come into the world unlocked.
  MutexRW() {
    pthread_rwlock_init(&mutex_, NULL);
  }

 private:
  friend class ReadLock;
  friend class WriteLock;
  // Actual pthread rwlock wrapped by MutexRW class.
  pthread_rwlock_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  MutexRW(const MutexRW&);
  MutexRW& operator=(const MutexRW&);
};

class ReadLock {
 public:
  explicit ReadLock(MutexRW* mutex) : mutex_(mutex) {
    pthread_rwlock_rdlock(&mutex_->mutex_);
  }
  ~ReadLock() {
    pthread_rwlock_unlock(&mutex_->mutex_);
  }

 private:
  MutexRW* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  ReadLock();

  // DISALLOW_COPY_AND_ASSIGN
  ReadLock(const ReadLock&);
  ReadLock& operator=(const ReadLock&);
};

class WriteLock {
 public:
  explicit WriteLock(MutexRW* mutex) : mutex_(mutex) {
    pthread_rwlock_wrlock(&mutex_->mutex_);
  }
  ~WriteLock() {
    pthread_rwlock_unlock(&mutex_->mutex_);
  }

 private:
  MutexRW* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  WriteLock();

  // DISALLOW_COPY_AND_ASSIGN
  WriteLock(const WriteLock&);
  WriteLock& operator=(const WriteLock&);
};


//template<class T>
//class slist{
//  class node
//  {
//	  node* next_;
//	  const T* data_;
//
//	public:
//
//	  node(const node* n, const const T d):next_(n),data_(d){}
//
//	  ~node(){
//		  delete next;
//		  delete data_;
//	  }
//
//	  node* next(){
//		  return next_;
//	  }
//
//	  const T* data(){
//		  return data_;
//	  }
//  };
//
//	node* head_;
//
//
//public:
//
//	slist() : head_(nullptr){}
//	bool empty(){ return (head_ == nullptr); }
//
//
//	void push_front(const T* h){
//		node* new_head = new node(head, h);
//		replace_head(new_head);
//	}
//
//
//
//	node* head(){
//		return std::atomic_load(head_);
//	}
//
//	void  replace_head(const node* h){
//		std::atomic_store(head_, h);
//	}
//
//	node* begin(){
//		return head;
//	}
//
//	// Non-copyable non-movable
//private:
//	slist(const slist&);
//	slist& operator=(const slist&) ;
//	slist(const slist&&);
//	slist& operator=(const slist&&) ;
//};


template <class T>
bool VectorEqual(std::vector<T> a, std::vector<T> b) {
	int size1 = a.size(), size2 = b.size();
	if (size1 != size2){
		LOG(-1, "Size different!"<<size1<<":"<<size2);
		return false;
	}
	else
	{
		for(int i = 0; i<size1; ++i){
			if (a[i] == b[i])
				continue;
			else{
				LOG(-1, "Element different!"<<a[i]<<":"<<b[i]);
				return false;
			}
		}
		return true;
	}
};

template<typename K, typename V>
class AtomicMap {
 public:
  AtomicMap() {}
  ~AtomicMap() {}

  inline bool Lookup(const K& k, V* v) {
    ReadLock l(&mutex_);
    typename unordered_map<K, V>::const_iterator lookup = map_.find(k);
    if (lookup == map_.end()) {
      return false;
    }
    *v = lookup->second;
    return true;
  }

  inline void Put(const K& k, const V& v) {
    WriteLock l(&mutex_);
    map_.insert(std::make_pair(k, v));
  }

  inline void Erase(const K& k) {
    WriteLock l(&mutex_);
    map_.erase(k);
  }

  // Puts (k, v) if there is no record for k. Returns the value of v that is
  // associated with k afterwards (either the inserted value or the one that
  // was there already).
  inline V PutNoClobber(const K& k, const V& v) {
    WriteLock l(&mutex_);
    typename unordered_map<K, V>::const_iterator lookup = map_.find(k);
    if (lookup != map_.end()) {
      return lookup->second;
    }
    map_.insert(std::make_pair(k, v));
    return v;
  }

  inline uint32 Size() {
    ReadLock l(&mutex_);
    return map_.size();
  }

  inline void DeleteVAndClear() {
    WriteLock l(&mutex_);
    for (typename unordered_map<K, V>::iterator it = map_.begin();
       it != map_.end(); ++it) {
      delete it->second;
    }
    map_.clear();
  }

 private:
  unordered_map<K, V> map_;
  MutexRW mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicMap(const AtomicMap<K, V>&);
  AtomicMap& operator=(const AtomicMap<K, V>&);
};


struct ValuePair{
	int first;
	Value* second = NULL;

	ValuePair(){
		first = 0;
		second = NULL;
	}

	ValuePair(bool new_obj){
		first = new_obj;
		second = NULL;
	}

	ValuePair(int in_first, Value* in_second){
		first = in_first;
		second = in_second;
	}
	void inline assign_first(int value){
		ASSERT(first == 0 || first == 1 || (first & WRITE));
		first = first | value;
	}

	bool inline assign(int value, Value* in_second){
		if(first == 0 || first == 1 || (first & WRITE)){
			first = first | value;
			second = in_second;
			return true;
		}
		else{
			//ASSERT(1==2);
			return false;
		}
	}
};

class ReadFromEntry{
public:
	int64_t my_tx_id_;
	int64_t read_from_id_;
	atomic<int>* abort_bit_;
	int num_aborted_;
	AtomicQueue<pair<int64_t, int>>* abort_queue_;

	ReadFromEntry(int64_t my_tx_id, int64_t read_from_id, atomic<int>* abort_bit,
			int num_aborted, AtomicQueue<pair<int64_t, int>>* abort_queue){
		my_tx_id_ = my_tx_id;
		read_from_id_ = read_from_id;
		abort_bit_ = abort_bit;
		num_aborted_ = num_aborted;
		abort_queue_ = abort_queue;
	}

	bool operator==  (ReadFromEntry another) const
	{
		return (my_tx_id_ == another.my_tx_id_
				&& read_from_id_ == another.read_from_id_
				&& abort_bit_ == another.abort_bit_
				&& num_aborted_ == another.num_aborted_
				&& abort_queue_ == another.abort_queue_);
	}

	friend inline std::ostream &operator<<(std::ostream &os, ReadFromEntry const &m) {
		if(m.abort_bit_)
			return os << "ReadFromEntry: ["<< m.my_tx_id_ << ", "<< m.read_from_id_ <<", "<< *m.abort_bit_<<
					", "<<m.num_aborted_<<", abort_queue_]";
		else
			return os << "ReadFromEntry: ["<< m.my_tx_id_ << ", "<< m.read_from_id_ <<", "<< m.abort_bit_<<
							", "<<m.num_aborted_<<", abort_queue_]";
	}
};

class PendingReadEntry{
public:
	int64_t my_tx_id_;
	atomic<int>* abort_bit_;
	int num_aborted_;
	bool request_lock_;
	AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue_;
	AtomicQueue<pair<int64_t, int>>* abort_queue_;

	PendingReadEntry(int64_t my_tx_id, atomic<int>* abort_bit, int num_aborted,
			AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, AtomicQueue<pair<int64_t, int>>* abort_queue, bool request_lock){
		my_tx_id_ = my_tx_id;
		abort_bit_ = abort_bit;
		num_aborted_ = num_aborted;
		pend_queue_ = pend_queue;
		abort_queue_ = abort_queue;
		request_lock_ = request_lock;
	}

	bool operator== (PendingReadEntry another) const
	{
		return (my_tx_id_ == another.my_tx_id_
				&& abort_bit_ == another.abort_bit_
				&& num_aborted_ == another.num_aborted_
				&& pend_queue_ == another.pend_queue_
				&& abort_queue_ == another.abort_queue_);
	}

	friend inline std::ostream &operator<<(std::ostream &os, PendingReadEntry const &m) {
		if(m.abort_bit_)
			return os << "ReadFromEntry: ["<< m.my_tx_id_ << ", "<<*m.abort_bit_<<
				m.num_aborted_<<", pend_queue, abort_queue]";
		else
			return os << "ReadFromEntry: ["<< m.my_tx_id_ << ", "<<m.abort_bit_<<
							m.num_aborted_<<", , pend_queue, abort_queue]";
	}
};

class LockEntry{
public:
	int64_t tx_id_;
	atomic<int>* abort_bit_;
	int num_aborted_;
	AtomicQueue<pair<int64_t, int>>* abort_queue_;

	LockEntry(){
		tx_id_ = NO_LOCK;
		abort_bit_ = NULL;
		num_aborted_ = 0;
		abort_queue_ = NULL;
	}

	LockEntry(int64_t tx_id, std::atomic<int>* abort_bit, int num_aborted, AtomicQueue<pair<int64_t, int>>* abort_queue){
		tx_id_ = tx_id;
		abort_bit_ = abort_bit;
		num_aborted_ = num_aborted;
		abort_queue_ = abort_queue;
	}

	bool operator== (LockEntry another) const
	{
		return (tx_id_ == another.tx_id_ && num_aborted_ == another.num_aborted_ &&
				abort_bit_ == another.abort_bit_ && abort_queue_ == another.abort_queue_);
	}


	friend inline std::ostream &operator<<(std::ostream &os, LockEntry const &m) {
		if(m.abort_bit_)
			return os << "LockEntry: ["<< m.tx_id_ << ", "<< *m.abort_bit_ <<", "<<m.num_aborted_<<
					", queue]";
		else
			return os << "LockEntry: ["<< m.tx_id_ << ", "<< m.abort_bit_ <<", "<<m.num_aborted_<<
							", queue]";
	}
};

class CompareReadFrom
{
public:
    bool operator() (ReadFromEntry left, ReadFromEntry right)
    {
    	return (left.my_tx_id_ > right.my_tx_id_);
    }
};

class ComparePendingRead
{
public:
    bool operator() (PendingReadEntry left, PendingReadEntry right)
    {
    	return (left.my_tx_id_ > right.my_tx_id_);
    }
};

class ComparePair
{
public:
    bool operator() (std::pair<int64_t, bool> left, std::pair<int64_t, bool> right)
    {
    	return (left.first > right.first);
    }
};

class CompareTuple
{
public:
    bool operator() (MyTuple<int64_t, int64_t, bool> left, MyTuple<int64_t, int64_t, bool> right)
    {
    	return (left.first > right.first);
    }
};

class CompareFour
{
public:
    bool operator() (MyFour<int64_t, int64_t, int, bool> left, MyFour<int64_t, int64_t, int, bool> right)
    {
    	return (left.second > right.second);
    }
};

class DataNode {
	public:
	int64_t txn_id;
	Value* value = NULL;
	DataNode* next = NULL;
	bool operator== (DataNode another)
	{
		return (txn_id == another.txn_id
				&& *value == *another.value
				&& next == another.next);
	}

	friend inline std::ostream &operator<<(std::ostream &os, DataNode const &m) {
		return os << "DataNode: ["<< m.txn_id << ", "<< *m.value <<"]";
	}

	~DataNode(){
		delete value;
	}

};

class KeyEntry {
	public:
		pthread_mutex_t mutex_;
		LockEntry lock;
		std::vector<ReadFromEntry>* read_from_list;
		std::vector<PendingReadEntry>* pend_list;
		DataNode* head;

		KeyEntry(){
			read_from_list = new vector<ReadFromEntry>();
			pend_list = new vector<PendingReadEntry>();
			head = NULL;
			pthread_mutex_init(&mutex_, NULL);
		}

		bool operator == (KeyEntry another) const {
			if (lock == another.lock){
				if(VectorEqual<ReadFromEntry>(*read_from_list, *another.read_from_list) == false){
					cout << "Read from entry not equal!" << endl;
					return false;
				}
				else{
					if(VectorEqual<PendingReadEntry>(*pend_list, *another.pend_list) == false){
						cout << "Pending read not equal" << endl;
						return false;
					}
					else{
						DataNode* mynode = head, *anothernode = another.head;
						while(mynode && anothernode){
							if (mynode == anothernode){
								mynode = mynode->next;
								anothernode = anothernode->next;
							}
							else
								return false;
						}
						if (mynode == anothernode)
							return true;
						else
							return false;
					}
				}
			}
			else{
				cout << "Lock not equal!" << endl;
				return false;
			}
		}

//		friend inline std::ostream &operator<<(std::ostream &os, KeyEntry const &m) {
//			  std::ostringstream oss1, oss2, oss3;
//
////			  if (m.read_from_list && !m.read_from_list->empty())
////			  {
////				// Convert all but the last element to avoid a trailing ","
////				std::copy(m.read_from_list->begin(), m.read_from_list->end()-1,
////					std::ostream_iterator<ReadFromEntry>(oss1, ","));
////
////				// Now add the last element with no delimiter
////				oss1 << m.read_from_list->back();
////			  }
////
////			  if (m.pend_list && !m.pend_list->empty())
////			  {
////				// Convert all but the last element to avoid a trailing ","
////				std::copy(m.pend_list->begin(), m.pend_list->end()-1,
////					std::ostream_iterator<int>(oss2, ","));
////
////				// Now add the last element with no delimiter
////				oss2 << m.pend_list[m.pend_list->size()-1];
////			  }
//
//			  DataNode* node = m.head;
//			  if(node != NULL){
//				  oss3 << node;
//				  node = node->next;
//				  if (!node)
//				  {
//					// Now add the last element with no delimiter
//					oss3 <<  "," << node;
//				  }
//			  }
//
//			//return os << "KeyEntry: [Mutex is not displayed!, "<<m.lock <<","<<oss1 << "," << oss2 <<"," << oss3 << "]";
//			  //return os << "KeyEntry: [Mutex is not displayed!, " << ","<<oss1 << "," << oss2 <<"," << oss3 << "]";
//			  //return os << "KeyEntry: [Mutex is not displayed!, " << m.lock << "]";
//			  return os << "WTF?";
//		}
};



#endif  // _DB_COMMON_UTILS_H_

