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
#include <fstream>
#include <iostream>
#include <iomanip>
#include "proto/message.pb.h"
#include <sstream>
//#include <unordered_map>

#include "common/logging.h"
#include "common/types.h"

using namespace std;
using std::string;
using std::vector;
using std::tr1::unordered_map;
//using std::unordered_map;

// Queue modes
#define NORMAL_QUEUE 1
#define SELF_QUEUE 2
#define DIRECT_QUEUE 3

#define NORMAL 1

// Transactin states
#define  SUCCESS 0
#define  FAILURE  1
#define  REDO  2
#define  RECON_SUCCESS 3
#define  SUSPENDED  4

// Loading parameters
//#define MAX_BATCH_SIZE 100
//#define RECON_BATCH_SIZE 50
//#define ABORT_MSG_SIZE 50
//#define DEPENDENT_PERCENT 0

#define DEPENDENT_MASK 8
#define RECON_MASK 16

#define ASSERTS_ON true

#define DCHECK(ARG) do { if (ASSERTS_ON) assert(ARG); } while (0)


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

const int base = 1000000000;
const int base_digits = 9;
struct bigint {
    vector<int> a;
    int sign;
    /*<arpa>*/
    int size(){
	if(a.empty())return 0;
	int ans=(a.size()-1)*base_digits;
	int ca=a.back();
	while(ca)
	    ans++,ca/=10;
	return ans;
    }
    bigint operator ^(const bigint &v){
	bigint ans=1,a=*this,b=v;
	while(!b.isZero()){
	    if(b%2)
		ans*=a;
	    a*=a,b/=2;
	}
	return ans;
    }
    string to_string(){
	stringstream ss;
	ss << *this;
	string s;
	ss >> s;
	return s;
    }
    int sumof(){
	string s = to_string();
	int ans = 0;
	for(auto c : s)  ans += c - '0';
	return ans;
    }
    /*</arpa>*/
    bigint() :
	sign(1) {
    }

    bigint(long long v) {
	*this = v;
    }

    bigint(const string &s) {
	read(s);
    }

    void operator=(const bigint &v) {
	sign = v.sign;
	a = v.a;
    }

    void operator=(long long v) {
	sign = 1;
	a.clear();
	if (v < 0)
	    sign = -1, v = -v;
	for (; v > 0; v = v / base)
	    a.push_back(v % base);
    }

    bigint operator+(const bigint &v) const {
	if (sign == v.sign) {
	    bigint res = v;

	    for (int i = 0, carry = 0; i < (int) max(a.size(), v.a.size()) || carry; ++i) {
		if (i == (int) res.a.size())
		    res.a.push_back(0);
		res.a[i] += carry + (i < (int) a.size() ? a[i] : 0);
		carry = res.a[i] >= base;
		if (carry)
		    res.a[i] -= base;
	    }
	    return res;
	}
	return *this - (-v);
    }

    bigint operator-(const bigint &v) const {
	if (sign == v.sign) {
	    if (abs() >= v.abs()) {
		bigint res = *this;
		for (int i = 0, carry = 0; i < (int) v.a.size() || carry; ++i) {
		    res.a[i] -= carry + (i < (int) v.a.size() ? v.a[i] : 0);
		    carry = res.a[i] < 0;
		    if (carry)
			res.a[i] += base;
		}
		res.trim();
		return res;
	    }
	    return -(v - *this);
	}
	return *this + (-v);
    }

    void operator*=(int v) {
	if (v < 0)
	    sign = -sign, v = -v;
	for (int i = 0, carry = 0; i < (int) a.size() || carry; ++i) {
	    if (i == (int) a.size())
		a.push_back(0);
	    long long cur = a[i] * (long long) v + carry;
	    carry = (int) (cur / base);
	    a[i] = (int) (cur % base);
	    //asm("divl %%ecx" : "=a"(carry), "=d"(a[i]) : "A"(cur), "c"(base));
	}
	trim();
    }

    bigint operator*(int v) const {
	bigint res = *this;
	res *= v;
	return res;
    }

    void operator*=(long long v) {
	if (v < 0)
	    sign = -sign, v = -v;
	for (int i = 0, carry = 0; i < (int) a.size() || carry; ++i) {
	    if (i == (int) a.size())
		a.push_back(0);
	    long long cur = a[i] * (long long) v + carry;
	    carry = (int) (cur / base);
	    a[i] = (int) (cur % base);
	    //asm("divl %%ecx" : "=a"(carry), "=d"(a[i]) : "A"(cur), "c"(base));
	}
	trim();
    }

    bigint operator*(long long v) const {
	bigint res = *this;
	res *= v;
	return res;
    }

    friend pair<bigint, bigint> divmod(const bigint &a1, const bigint &b1) {
	int norm = base / (b1.a.back() + 1);
	bigint a = a1.abs() * norm;
	bigint b = b1.abs() * norm;
	bigint q, r;
	q.a.resize(a.a.size());

	for (int i = a.a.size() - 1; i >= 0; i--) {
	    r *= base;
	    r += a.a[i];
	    int s1 = r.a.size() <= b.a.size() ? 0 : r.a[b.a.size()];
	    int s2 = r.a.size() <= b.a.size() - 1 ? 0 : r.a[b.a.size() - 1];
	    int d = ((long long) base * s1 + s2) / b.a.back();
	    r -= b * d;
	    while (r < 0)
		r += b, --d;
	    q.a[i] = d;
	}

	q.sign = a1.sign * b1.sign;
	r.sign = a1.sign;
	q.trim();
	r.trim();
	return make_pair(q, r / norm);
    }

    bigint operator/(const bigint &v) const {
	return divmod(*this, v).first;
    }

    bigint operator%(const bigint &v) const {
	return divmod(*this, v).second;
    }

    void operator/=(int v) {
	if (v < 0)
	    sign = -sign, v = -v;
	for (int i = (int) a.size() - 1, rem = 0; i >= 0; --i) {
	    long long cur = a[i] + rem * (long long) base;
	    a[i] = (int) (cur / v);
	    rem = (int) (cur % v);
	}
	trim();
    }

    bigint operator/(int v) const {
	bigint res = *this;
	res /= v;
	return res;
    }

    int operator%(int v) const {
	if (v < 0)
	    v = -v;
	int m = 0;
	for (int i = a.size() - 1; i >= 0; --i)
	    m = (a[i] + m * (long long) base) % v;
	return m * sign;
    }

    void operator+=(const bigint &v) {
	*this = *this + v;
    }
    void operator-=(const bigint &v) {
	*this = *this - v;
    }
    void operator*=(const bigint &v) {
	*this = *this * v;
    }
    void operator/=(const bigint &v) {
	*this = *this / v;
    }

    bool operator<(const bigint &v) const {
	if (sign != v.sign)
	    return sign < v.sign;
	if (a.size() != v.a.size())
	    return a.size() * sign < v.a.size() * v.sign;
	for (int i = a.size() - 1; i >= 0; i--)
	    if (a[i] != v.a[i])
		return a[i] * sign < v.a[i] * sign;
	return false;
    }

    bool operator>(const bigint &v) const {
	return v < *this;
    }
    bool operator<=(const bigint &v) const {
	return !(v < *this);
    }
    bool operator>=(const bigint &v) const {
	return !(*this < v);
    }
    bool operator==(const bigint &v) const {
	return !(*this < v) && !(v < *this);
    }
    bool operator!=(const bigint &v) const {
	return *this < v || v < *this;
    }

    void trim() {
	while (!a.empty() && !a.back())
	    a.pop_back();
	if (a.empty())
	    sign = 1;
    }

    bool isZero() const {
	return a.empty() || (a.size() == 1 && !a[0]);
    }

    bigint operator-() const {
	bigint res = *this;
	res.sign = -sign;
	return res;
    }

    bigint abs() const {
	bigint res = *this;
	res.sign *= res.sign;
	return res;
    }

    long long longValue() const {
	long long res = 0;
	for (int i = a.size() - 1; i >= 0; i--)
	    res = res * base + a[i];
	return res * sign;
    }

    friend bigint gcd(const bigint &a, const bigint &b) {
	return b.isZero() ? a : gcd(b, a % b);
    }
    friend bigint lcm(const bigint &a, const bigint &b) {
	return a / gcd(a, b) * b;
    }

    void read(const string &s) {
	sign = 1;
	a.clear();
	int pos = 0;
	while (pos < (int) s.size() && (s[pos] == '-' || s[pos] == '+')) {
	    if (s[pos] == '-')
		sign = -sign;
	    ++pos;
	}
	for (int i = s.size() - 1; i >= pos; i -= base_digits) {
	    int x = 0;
	    for (int j = max(pos, i - base_digits + 1); j <= i; j++)
		x = x * 10 + s[j] - '0';
	    a.push_back(x);
	}
	trim();
    }

    friend istream& operator>>(istream &stream, bigint &v) {
	string s;
	stream >> s;
	v.read(s);
	return stream;
    }

    friend ostream& operator<<(ostream &stream, const bigint &v) {
	if (v.sign == -1)
	    stream << '-';
	stream << (v.a.empty() ? 0 : v.a.back());
	for (int i = (int) v.a.size() - 2; i >= 0; --i)
	    stream << std::setw(base_digits) << std::setfill('0') << v.a[i];
	return stream;
    }

    static vector<int> convert_base(const vector<int> &a, int old_digits, int new_digits) {
	vector<long long> p(max(old_digits, new_digits) + 1);
	p[0] = 1;
	for (int i = 1; i < (int) p.size(); i++)
	    p[i] = p[i - 1] * 10;
	vector<int> res;
	long long cur = 0;
	int cur_digits = 0;
	for (int i = 0; i < (int) a.size(); i++) {
	    cur += a[i] * p[cur_digits];
	    cur_digits += old_digits;
	    while (cur_digits >= new_digits) {
		res.push_back(int(cur % p[new_digits]));
		cur /= p[new_digits];
		cur_digits -= new_digits;
	    }
	}
	res.push_back((int) cur);
	while (!res.empty() && !res.back())
	    res.pop_back();
	return res;
    }

    typedef vector<long long> vll;

    static vll karatsubaMultiply(const vll &a, const vll &b) {
	int n = a.size();
	vll res(n + n);
	if (n <= 32) {
	    for (int i = 0; i < n; i++)
		for (int j = 0; j < n; j++)
		    res[i + j] += a[i] * b[j];
	    return res;
	}

	int k = n >> 1;
	vll a1(a.begin(), a.begin() + k);
	vll a2(a.begin() + k, a.end());
	vll b1(b.begin(), b.begin() + k);
	vll b2(b.begin() + k, b.end());

	vll a1b1 = karatsubaMultiply(a1, b1);
	vll a2b2 = karatsubaMultiply(a2, b2);

	for (int i = 0; i < k; i++)
	    a2[i] += a1[i];
	for (int i = 0; i < k; i++)
	    b2[i] += b1[i];

	vll r = karatsubaMultiply(a2, b2);
	for (int i = 0; i < (int) a1b1.size(); i++)
	    r[i] -= a1b1[i];
	for (int i = 0; i < (int) a2b2.size(); i++)
	    r[i] -= a2b2[i];

	for (int i = 0; i < (int) r.size(); i++)
	    res[i + k] += r[i];
	for (int i = 0; i < (int) a1b1.size(); i++)
	    res[i] += a1b1[i];
	for (int i = 0; i < (int) a2b2.size(); i++)
	    res[i + n] += a2b2[i];
	return res;
    }

    bigint operator*(const bigint &v) const {
	vector<int> a6 = convert_base(this->a, base_digits, 6);
	vector<int> b6 = convert_base(v.a, base_digits, 6);
	vll a(a6.begin(), a6.end());
	vll b(b6.begin(), b6.end());
	while (a.size() < b.size())
	    a.push_back(0);
	while (b.size() < a.size())
	    b.push_back(0);
	while (a.size() & (a.size() - 1))
	    a.push_back(0), b.push_back(0);
	vll c = karatsubaMultiply(a, b);
	bigint res;
	res.sign = sign * v.sign;
	for (int i = 0, carry = 0; i < (int) c.size(); i++) {
	    long long cur = c[i] + carry;
	    res.a.push_back((int) (cur % 1000000));
	    carry = (int) (cur / 1000000);
	}
	res.a = convert_base(res.a, 6, base_digits);
	res.trim();
	return res;
    }
};



// Returns the number of seconds since midnight according to local system time,
// to the nearest microsecond.
static inline double GetTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec/1e6;
}

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


class LatencyUtils {
  	private:
		int small_lat[1000];
		vector<int> large_lat;
		int total_latency;
		int total_count;
	public:
		inline void add_latency(int latency){
			if (latency >= 1000)
				large_lat.push_back(latency);
			else
				small_lat[latency] += 1;
		}

		int average_latency(){
			if(total_latency == 0)
				calculate_total();
			return total_latency/total_count; 	
		}	
		int medium_latency(){
			if(total_latency == 0)
				calculate_total();
			return get_percent_latency(0.5);
		}
		int the95_latency(){
			if(total_latency == 0)
				calculate_total();
			return get_percent_latency(0.95);
		}
		int the99_latency(){
			if(total_latency == 0)
				calculate_total();
			return get_percent_latency(0.99);
		}
		int the999_latency(){
			if(total_latency == 0)
				calculate_total();
			return get_percent_latency(0.999);
		}
	private:
		void calculate_total() {
			for(uint i = 0; i< 1000; ++i){
				total_latency += small_lat[i]*i;
				total_count += small_lat[i];
			}
			for(uint i = 0; i< large_lat.size(); ++i){	
				total_latency += large_lat[i];
				total_count += 1;
			}
		}
		int get_percent_latency(double percent){
			int medium_cnt = total_count*percent, cnt = 0;	
			for(uint j = 0; j < 1000; ++j){
				if(cnt+small_lat[j] >= medium_cnt)
					return j;
				cnt += small_lat[j];
			}
			for(uint j = 0; j< large_lat.size(); ++j){	
				if(cnt+1 >= medium_cnt)
					return large_lat[j];
				cnt += 1;
			}
			return large_lat[large_lat.size()-1];
		}

};

////////////////////////////////
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

////////////////////////////////////////////////////////////////

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
};

class CompareMsg
{
public:
    bool operator() (MessageProto* left, MessageProto* right)
    {
    	return (left->msg_id() > right->msg_id());
    }
};

template<typename K, typename V>
class MyAtomicMap {
 public:
	MyAtomicMap() {
		pthread_mutex_init(&mutex_, NULL);
	}
  ~MyAtomicMap() {}

  inline V Lookup(const K& k) {
	  V v;
	  pthread_mutex_lock(&mutex_);
	  v = map_[k];
	  pthread_mutex_unlock(&mutex_);
	  return v;
  }

  inline void Put(const K& k, const V& v) {
	pthread_mutex_lock(&mutex_);
    map_[k] = v;
    pthread_mutex_unlock(&mutex_);
  }

  inline void Erase(const K& k) {
	pthread_mutex_lock(&mutex_);
	map_.erase(k);
	pthread_mutex_unlock(&mutex_);
  }

 private:
  unordered_map<K, V> map_;
  pthread_mutex_t mutex_;

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

	MyFour() {}

	MyFour(const MyFour& mf){
		first = mf.first;
		second = mf.second;
		third = mf.third;
		fourth = mf.fourth;
	}
};

struct Node {
  // Globally unique node identifier.
  int node_id;
  int replica_id;
  int partition_id;

  // IP address of this node's machine.
  string host;

  // Port on which to listen for messages from other nodes.
  int port;

  // Total number of cores available for use by this node.
  // Note: Is this needed?
  int cores;
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

#endif  // _DB_COMMON_UTILS_H_

