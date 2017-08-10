// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//
// TODO(scw): replace iostream with cstdio

#include "sequencer/sequencer.h"

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <utility>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include <fstream>

#define PAXOS

#ifdef PAXOS
# include "paxos/paxos.h"
#endif

using std::map;
using std::multimap;
using std::set;
using std::queue;

extern LatencyUtils latency_util;

#ifdef LATENCY_TEST
double sequencer_recv[SAMPLES];
double sequencer_send[SAMPLES];
double prefetch_cold[SAMPLES];
double scheduler_lock[SAMPLES];
double worker_begin[SAMPLES];
double worker_end[SAMPLES];
double scheduler_unlock[SAMPLES];
#endif

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}


Sequencer::Sequencer(Configuration* conf, ConnectionMultiplexer* multiplexer,
                     Client* client, Storage* storage, int queue_mode)
    : epoch_duration_(0.01), configuration_(conf), multiplexer_(multiplexer),
      client_(client), storage_(storage), deconstructor_invoked_(false), queue_mode_(queue_mode), fetched_txn_num_(0) {
	pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.

  	cpu_set_t cpuset;

	message_queues = new AtomicQueue<MessageProto>();

	connection_ = multiplexer->NewConnection("sequencer", &message_queues);
	skeen_connection_ = multiplexer->NewConnection("skeen");

	pthread_attr_t attr_writer;
	pthread_attr_init(&attr_writer);
	//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	CPU_ZERO(&cpuset);
	CPU_SET(1, &cpuset);
	pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
	std::cout << "Sequencer writer starts at core 1"<<std::endl;

	pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
		 reinterpret_cast<void*>(this));

	CPU_ZERO(&cpuset);
	CPU_SET(2, &cpuset);
	pthread_attr_t attr_reader;
	pthread_attr_init(&attr_reader);
	pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);
	std::cout << "Sequencer reader starts at core 2"<<std::endl;
	pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		  reinterpret_cast<void*>(this));

	#ifdef PAXOS
		std::cout<<"Using Paxos replication!"<<std::endl;
		batch_prop_limit = conf->num_partitions;
		Connection* paxos_connection = multiplexer->NewConnection("paxos");
		paxos = new Paxos(conf->this_group, conf->this_node, paxos_connection, conf->this_node_partition, conf->num_partitions);
	#else
		batch_prop_limit = conf->all_nodes.size();
	#endif
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  if (queue_mode_ == DIRECT_QUEUE)
	  delete txns_queue_;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
  pthread_join(skeen_thread_, NULL);
  delete connection_;
  std::cout<<"Sequencer done"<<std::endl;
}

void Sequencer::FindParticipatingNodes(const TxnProto& txn, set<int>* nodes) {
  nodes->clear();
  for (int i = 0; i < txn.read_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
  for (int i = 0; i < txn.write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
  for (int i = 0; i < txn.read_write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
}

#ifdef PREFETCHING
double PrefetchAll(Storage* storage, TxnProto* txn) {
  double max_wait_time = 0;
  double wait_time = 0;
  for (int i = 0; i < txn->read_set_size(); i++) {
    storage->Prefetch(txn->read_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
  for (int i = 0; i < txn->read_write_set_size(); i++) {
    storage->Prefetch(txn->read_write_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
  for (int i = 0; i < txn->write_set_size(); i++) {
    storage->Prefetch(txn->write_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
#ifdef LATENCY_TEST
  if (txn->txn_id() % SAMPLE_RATE == 0)
    prefetch_cold[txn->txn_id() / SAMPLE_RATE] = max_wait_time;
#endif
  return max_wait_time;
}
#endif



void Sequencer::RunWriter() {
  Spin(1);

#ifdef PREFETCHING
  multimap<double, TxnProto*> fetching_txns;
#endif

  // Synchronization loadgen start with other sequencers.
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("sequencer");
  for (uint32 i = 0; i < configuration_->all_nodes.size(); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint32>(configuration_->this_node_id))
      connection_->Send(synchronization_message);
  }
  uint32 synchronization_counter = 1;
  while (synchronization_counter < configuration_->all_nodes.size()) {
    synchronization_message.Clear();
    if (connection_->GetMessage(&synchronization_message)) {
      assert(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }

  started = true;

  MessageProto batch;
  batch.set_destination_channel("paxos");
  batch.set_destination_node(configuration_->this_node_id);
  batch.set_source_node(configuration_->this_node_id);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);

  //double time = GetTime();
  int all_parts = configuration_->num_partitions;

  for (int batch_number = configuration_->this_node_partition;
       !deconstructor_invoked_;
       batch_number += all_parts) {
    // Begin epoch.
    double epoch_start = GetTime();
    batch.set_batch_number(batch_number);
    batch.clear_data();

    // Collect txn requests for this epoch.
    int txn_id_offset = 0;
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if (batch.data_size() < max_batch_size) {
        TxnProto* txn;
        string txn_string;
        client_->GetTxn(&txn, batch_number * max_batch_size + txn_id_offset);
        if(txn->txn_id() == -1) {
          delete txn;
          continue;
        }

        txn->SerializeToString(&txn_string);
        batch.add_data(txn_string);
        txn_id_offset++;
        delete txn;
      }
    }

    batch.SerializeToString(&batch_string);
    pthread_mutex_lock(&mutex_);
    batch_queue_.push(batch_string);
    pthread_mutex_unlock(&mutex_);
  }

  Spin(1);
}


void Sequencer::HandleSkeen() {

  	MessageProto recv_msg;
    if(skeen_connection_->GetMessage(&recv_msg)){
        int msg_type = recv_msg.type();
        if (msg_type == MessageProto::SKEEN_REQ){
            int64 to_propose_batch = max(max_batch, proposed_batch+1);
            // Increase random_batch with 50% probability, to avoid the case that messages keep being aggregated in this batch
            if(max_batch == to_propose_batch){
                if (proposed_for_batch+1 == batch_prop_limit){
                    proposed_for_batch = 0;
                    max_batch = max_batch + 1;
                }
                else
                    ++proposed_for_batch;
            }
            else{
                proposed_for_batch = 1;
                max_batch = to_propose_batch;
            }

            num_pending[to_propose_batch] += 1;
            recv_msg.set_propose_batch(to_propose_batch);
            pending_received_skeen[recv_msg.msg_id()] = new MessageProto(recv_msg);
            SEQLOG(-1, " replying skeen request: "<<recv_msg.msg_id()<<", proposing "<<to_propose_batch);

            MessageProto reply;
            reply.set_destination_channel("skeen");
            reply.set_destination_node(recv_msg.source_node());
            reply.set_type(MessageProto::SKEEN_PROPOSE);
            reply.set_batch_number(recv_msg.batch_number());
            reply.set_propose_batch(to_propose_batch);
            reply.set_msg_id(recv_msg.msg_id());

            // TODO: Replicate locally if needed before replying
            skeen_connection_->Send(reply);
        }
        else if (msg_type == MessageProto::SKEEN_PROPOSE){
            int64 msg_id = recv_msg.msg_id();
            int64 index = recv_msg.batch_number();
            MyFour<int64, int64, vector<int>, MessageProto*> entry = pending_sent_skeen[index];
            SEQLOG(-1, " Got skeen propose: "<<recv_msg.msg_id()<<", he proposed "<<recv_msg.propose_batch()<<", remaining is "<<entry.first);
            entry.second = max(recv_msg.propose_batch(), entry.second);
            //pending_skeen_msg[msg_id].third->set_batch_number(new_batch);
            if (entry.first == 1)
            {
                //Reply to allstd::cout<<"Got batch"
                int64 final_batch = max(max_batch, max(proposed_batch+1, entry.second));
                // Increase random_batch with 50% probability, to avoid the case that messages keep being aggregated in this batch
                if(max_batch != final_batch){
                    max_batch = final_batch;
                    proposed_for_batch = 0;
                }

                //lzing skeen request: "<<msg->msg_id()<<", proposing "<<final_batch);

                MessageProto reply_msg;
                reply_msg.set_type(MessageProto::SKEEN_REPLY);
                reply_msg.set_destination_channel("skeen");
                reply_msg.set_msg_id(msg_id);
                reply_msg.set_batch_number(final_batch);
                vector<int> involved_nodes = entry.third;
                for(uint i = 0; i<involved_nodes.size(); ++i){
                    reply_msg.set_destination_node(involved_nodes[i]);
                    skeen_connection_->Send(reply_msg);
                }

                //Put it to the batch
                multi_part_txns[final_batch].push(entry.fourth);
                pending_sent_skeen.erase(index);
                SEQLOG(-1, " Got skeen propose: "<<recv_msg.msg_id()<<", pushed "<<entry.fourth->msg_id()<<" to"<<final_batch<<" and size is "<<multi_part_txns[final_batch].size());
                SEQLOG(-1, " For "<<recv_msg.msg_id()<<", num pending is "<<num_pending[final_batch]<<", proposed_batch is"<<proposed_batch);

                if( num_pending[final_batch] == 0 && proposed_batch+1 == final_batch)
                    propose_global(proposed_batch, num_pending, pending_paxos_props, multi_part_txns);
            }
            else{
                entry.first -= 1;
                pending_sent_skeen[index] = entry;
            }
        }
        else if (msg_type == MessageProto::SKEEN_REPLY){
            int64 new_batch = recv_msg.batch_number(),
                    blocked_batch = pending_received_skeen[recv_msg.msg_id()]->propose_batch();

            //Put it to the batch
            multi_part_txns[new_batch].push(pending_received_skeen[recv_msg.msg_id()]);
            SEQLOG(-1, " got skeen final: "<<recv_msg.msg_id()<<", batch number is "<<new_batch<<", pushed "<<reinterpret_cast<int64>(pending_received_skeen[recv_msg.msg_id()])<<", size is"<<multi_part_txns[new_batch].size());
            num_pending[blocked_batch] -= 1;

            if(num_pending[blocked_batch] == 0 && blocked_batch == proposed_batch+1){
                pending_received_skeen.erase(recv_msg.msg_id());
                propose_global(proposed_batch, num_pending, pending_paxos_props, multi_part_txns);
            }
        }
    }
}

void Sequencer::propose_global(int64& proposed_batch, map<int64, int>& num_pending, queue<MessageProto*>& pending_paxos_props, unordered_map<int64, priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg>>& multi_part_txns){
    while(true){
        int64 next_batch = proposed_batch+1;
        if (num_pending[next_batch] == 0 && pending_paxos_props.size()
                && pending_paxos_props.front()->batch_number() == next_batch){
            MessageProto* propose_msg = pending_paxos_props.front();
            SEQLOG(-1, " Proposing to global "<<next_batch<<", proposed batch is "<<proposed_batch);
            if (multi_part_txns.count(next_batch) != 0){
                priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg> msgs = multi_part_txns[next_batch];
                SEQLOG(-1, " Proposing to global "<<next_batch<<", msg size is "<<msgs.size());
                while(msgs.size()){
                    MessageProto* msg = msgs.top();
                    msgs.pop();
                    SEQLOG(-1, " Proposing to global "<<next_batch<<", adding message "<<msg->msg_id());
                    for(int j = 0; j < msg->data_size(); ++j)
                        propose_msg->add_data(msg->data(j));
                    delete msg;
                }
            }
			paxos->SubmitBatch(propose_msg);
            multi_part_txns.erase(next_batch);
            pending_paxos_props.pop();
            num_pending.erase(next_batch);
            ++proposed_batch;
        }
        else
            break;
    }
}


void Sequencer::RunReader() {
  Spin(1);
  int node_id = configuration_->this_node_id;
  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  vector<Node*> dc = configuration_->this_dc;
  for (uint i = 0; i < dc.size(); ++i) {
    batches[dc[i]->node_id].set_destination_channel("skeen");
    batches[dc[i]->node_id].set_destination_node(dc[i]->node_id);
    batches[dc[i]->node_id].set_source_node(node_id);
    batches[dc[i]->node_id].set_type(MessageProto::SKEEN_REQ);
  }

  int batch_number = 0;

  while (!deconstructor_invoked_) {

    if (batch_queue_.size()) {
    	string batch_string;
        pthread_mutex_lock(&mutex_);
        batch_string = batch_queue_.front();
        batch_queue_.pop();
        pthread_mutex_unlock(&mutex_);

		MessageProto batch_message;
		MessageProto* multi_part_msg = new MessageProto(),
					 *single_part_msg = new MessageProto();

		single_part_msg->set_batch_number(batch_number);
		single_part_msg->set_destination_channel("paxos");
		single_part_msg->set_source_node(configuration_->this_node_id);

		batch_message.ParseFromString(batch_string);
		set<int> involved_parts;
		for (int i = 0; i < batch_message.data_size(); i++) {
			TxnProto txn;
			txn.ParseFromString(batch_message.data(i));

			// Compute readers & writers; store in txn proto.
			set<int> to_send;
			google::protobuf::RepeatedField<int>::const_iterator  it;

			for (it = txn.readers().begin(); it != txn.readers().end(); ++it)
				to_send.insert(configuration_->PartLocalNode(*it));
			for (it = txn.writers().begin(); it != txn.writers().end(); ++it)
				to_send.insert(configuration_->PartLocalNode(*it));

			// Insert txn into appropriate batches.
			if(to_send.size() == 1 && *to_send.begin() == node_id){
				//SEQLOG(txn.txn_id(), " added to my single "<<batch_number);
				single_part_msg->add_data(batch_message.data(i));
			}
			else{
				for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it){
					//LOG(txn.txn_id(), "is added to "<<*it);
					if(*it == node_id)
						multi_part_msg->add_data(batch_message.data(i));
					else{
						batches[*it].add_data(batch_message.data(i));
						involved_parts.insert(*it);
					}
				}
			}
		}

		// Deal with single part msg
		int64 to_propose_batch = single_part_msg->batch_number();
        SEQLOG(-1, " got single part msg for batch "<<to_propose_batch<<", proposed batch is "<<proposed_batch);
        if (num_pending[to_propose_batch] == 0 && to_propose_batch == proposed_batch+1){
            SEQLOG(-1, " proposing to global "<<to_propose_batch<<", proposed batch is "<<proposed_batch);
            if (multi_part_txns.count(to_propose_batch) != 0){
                priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg> msgs = multi_part_txns[to_propose_batch];
                SEQLOG(-1, " Proposing to global "<<to_propose_batch<<", msg size is "<<msgs.size());
                while(msgs.size()){
                    MessageProto* msg = msgs.top();
                    SEQLOG(-1, " Proposing to global "<<to_propose_batch<<", adding message "<<msg->msg_id());
                    msgs.pop();
                    for(int j = 0; j < msg->data_size(); ++j)
                        single_part_msg->add_data(msg->data(j));
                    delete msg;
                }
            }
            paxos->SubmitBatch(single_part_msg);
            multi_part_txns.erase(to_propose_batch);
            ++proposed_batch;
        }
        else{
            SEQLOG(-1, " not ready to proceed "<<to_propose_batch<<", num pending is "<<num_pending[to_propose_batch]);
            pending_paxos_props.push(single_part_msg);
        }


		int64 msg_id = batch_number | ((uint64)node_id) <<40;
		//SEQLOG(-1, " finished loading for "<<batch_number);
		if(involved_parts.size()){
			std::vector<int> output(involved_parts.size());
			std::copy(involved_parts.begin(), involved_parts.end(), output.begin());
			SEQLOG(-1, "multi-part txn's size is "<<involved_parts.size());
			multi_part_msg->set_msg_id(msg_id);
			pending_sent_skeen[batch_number] = MyFour<int64, int64, vector<int>, MessageProto*>(involved_parts.size(), 0, output, multi_part_msg);
		}
		else
			delete multi_part_msg;

		for(set<int>::iterator it = involved_parts.begin(); it != involved_parts.end(); ++it){
			batches[*it].set_batch_number(batch_number);
			batches[*it].set_msg_id(msg_id);
			connection_->Send(batches[*it]);
			batches[*it].clear_data();
			SEQLOG(-1, " Sending skeen request "<<msg_id<<" to "<<*it);
		}
    	batch_number += 1;
    }

	HandleSkeen();
	Spin(0.0005);
  }
  Spin(1);
}


void Sequencer::output(DeterministicScheduler* scheduler){
    ofstream myfile;
    myfile.open (IntToString(configuration_->this_node_id)+"output.txt");
    int count =0;
    double abort = 0;
    myfile << "THROUGHPUT" << '\n';
    while((abort = scheduler->abort[count]) != -1 && count < THROUGHPUT_SIZE){
        myfile << scheduler->throughput[count] << ", "<< abort << '\n';
        ++count;
    }
    myfile << "LATENCY" << '\n';
	myfile << latency_util.average_latency()<<", "<<latency_util.medium_latency()<<", "<< latency_util.the95_latency() <<", "<<latency_util.the99_latency()<<", "<<latency_util.the999_latency()<< '\n';
    myfile.close();
}
