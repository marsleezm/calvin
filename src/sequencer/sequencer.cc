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

//#define PAXOS

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

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}


Sequencer::Sequencer(Configuration* conf, ConnectionMultiplexer* multiplexer,
                     Client* client, Storage* storage, int queue_mode)
    : epoch_duration_(0.01), batch_count_(0), configuration_(conf), multiplexer_(multiplexer),
      client_(client), storage_(storage), deconstructor_invoked_(false), queue_mode_(queue_mode), fetched_txn_num_(0)  {
	pthread_mutex_init(&mutex_, NULL);
	paxos = NULL;
	message_queues = new AtomicQueue<MessageProto>();

	connection_ = multiplexer->NewConnection("sequencer", &message_queues);
	skeen_connection_ = multiplexer->NewConnection("skeen");

	pthread_attr_t attr_reader;
	pthread_attr_init(&attr_reader);

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
  if (queue_mode_ == DIRECT_QUEUE)
	  delete txns_queue_;
  delete connection_;
  delete skeen_connection_;
  delete paxos;
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


void Sequencer::Synchronize(){
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
}



void Sequencer::HandleSkeen() {
  	MessageProto recv_msg;
    while(skeen_connection_->GetMessage(&recv_msg)){
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
            #ifdef PAXOS
			    paxos->SubmitBatch(propose_msg);
            #else
                propose_msg->set_destination_channel("scheduler_");
                propose_msg->set_type(MessageProto::TXN_BATCH);
                propose_msg->set_destination_node(configuration_->this_node_id);
                connection_->Send(*propose_msg);
                delete propose_msg;
            #endif
            multi_part_txns.erase(next_batch);
            pending_paxos_props.pop();
            num_pending.erase(next_batch);
            ++proposed_batch;
        }
        else
            break;
    }
}

void Sequencer::GenerateLoad(double now, map<int, MessageProto>& batches){
	if ( now > epoch_start_ + batch_count_*epoch_duration_ ){ 
		int txn_id_offset = 0, node_id = configuration_->this_node_id;
		TxnProto* txn;
        int txn_base = batch_count_*configuration_->all_nodes.size()+node_id;
		set<int> involved_parts;
		MessageProto* multi_part_msg = new MessageProto(),
					 *single_part_msg = new MessageProto();

		single_part_msg->set_batch_number(batch_count_);
		single_part_msg->set_destination_channel("paxos");
		single_part_msg->set_source_node(configuration_->this_node_id);

		while (!deconstructor_invoked_ &&
     		now < epoch_start_ + (batch_count_+1)*epoch_duration_ && txn_id_offset < max_batch_size){
    		client_->GetTxn(&txn, max_batch_size*txn_base+txn_id_offset);
			LOG(txn->txn_id(), " adding txn");
			set<int> to_send;
			google::protobuf::RepeatedField<int>::const_iterator  it;

			for (it = txn->readers().begin(); it != txn->readers().end(); ++it)
				to_send.insert(configuration_->PartLocalNode(*it));
			for (it = txn->writers().begin(); it != txn->writers().end(); ++it)
				to_send.insert(configuration_->PartLocalNode(*it));

			// Insert txn into appropriate batches.
			if(to_send.size() == 1 && *to_send.begin() == node_id){
				single_part_msg->add_data(txn->SerializeAsString());
			}
			else{
				for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it){
					//LOG(txn.txn_id(), "is added to "<<*it);
					if(*it == node_id)
						multi_part_msg->add_data(txn->SerializeAsString());
					else{
						batches[*it].add_data(txn->SerializeAsString());
						involved_parts.insert(*it);
					}
				}
            }
			delete txn;
			txn_id_offset++;
		}

		// Deal with single part msg
        SEQLOG(-1, " got single part msg for batch "<<batch_count_<<", proposed batch is "<<proposed_batch);
        if (num_pending[batch_count_] == 0 && batch_count_ == proposed_batch+1){
            SEQLOG(-1, " proposing to global "<<batch_count_<<", proposed batch is "<<proposed_batch);
            if (multi_part_txns.count(batch_count_) != 0){
                priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg> msgs = multi_part_txns[batch_count_];
                SEQLOG(-1, " Proposing to global "<<batch_count_<<", msg size is "<<msgs.size());
                while(msgs.size()){
                    MessageProto* msg = msgs.top();
                    SEQLOG(-1, " Proposing to global "<<batch_count_<<", adding message "<<msg->msg_id());
                    msgs.pop();
                    for(int j = 0; j < msg->data_size(); ++j)
                        single_part_msg->add_data(msg->data(j));
                    delete msg;
                }
            }
            #ifdef PAXOS
                paxos->SubmitBatch(single_part_msg);
            #else
                single_part_msg->set_destination_channel("scheduler_");
                single_part_msg->set_type(MessageProto::TXN_BATCH);
                single_part_msg->set_destination_node(configuration_->this_node_id);
                connection_->Send(*single_part_msg);
                delete single_part_msg;
            #endif
            multi_part_txns.erase(batch_count_);
            ++proposed_batch;
        }
        else{
            SEQLOG(-1, " not ready to proceed "<<batch_count_<<", num pending is "<<num_pending[batch_count_]);
            pending_paxos_props.push(single_part_msg);
        }

		int64 msg_id = batch_count_ | ((uint64)node_id) <<40;
		//SEQLOG(-1, " finished loading for "<<batch_number);
		if(involved_parts.size()){
			std::vector<int> output(involved_parts.size());
			std::copy(involved_parts.begin(), involved_parts.end(), output.begin());
			SEQLOG(-1, "multi-part txn's size is "<<involved_parts.size());
			multi_part_msg->set_msg_id(msg_id);
			pending_sent_skeen[batch_count_] = MyFour<int64, int64, vector<int>, MessageProto*>(involved_parts.size(), 0, output, multi_part_msg);
		}
		else
			delete multi_part_msg;

		for(set<int>::iterator it = involved_parts.begin(); it != involved_parts.end(); ++it){
			batches[*it].set_batch_number(batch_count_);
			batches[*it].set_msg_id(msg_id);
			connection_->Send(batches[*it]);
			batches[*it].clear_data();
			SEQLOG(-1, " Sending skeen request "<<msg_id<<" to "<<*it);
		}
		batch_count_++;
	}
}

void Sequencer::RunReader() {
  Spin(1);
  int node_id = configuration_->this_node_id;

  Synchronize();
  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  vector<Node*> dc = configuration_->this_dc;
  for (uint i = 0; i < dc.size(); ++i) {
    batches[dc[i]->node_id].set_destination_channel("skeen");
    batches[dc[i]->node_id].set_destination_node(dc[i]->node_id);
    batches[dc[i]->node_id].set_source_node(node_id);
    batches[dc[i]->node_id].set_type(MessageProto::SKEEN_REQ);
  }

  epoch_start_ = GetTime();

  while (!deconstructor_invoked_) {
    GenerateLoad(GetTime(), batches);
	HandleSkeen();
	Spin(0.001);
  }
  Spin(1);
}


void Sequencer::output(DeterministicScheduler* scheduler){
    deconstructor_invoked_ = true;
    pthread_join(reader_thread_, NULL);

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
	if(configuration_->this_node_id == 0){
		//Wait for nodes from the same DC to send him data
		int to_receive_msg = configuration_->this_dc.size()-1;
		MessageProto message;
		while(to_receive_msg != 0){
			if(connection_->GetMessage(&message)){
				if(message.type() == MessageProto::LATENCY){
					std::cout<<"Got message from "<<message.source_node()<<std::endl;
					for(int i = 0; i< message.latency_size(); ++i){
						for(int j = 0; j < message.count(i); ++j)
							latency_util.add_latency(message.latency(i));
					}
					to_receive_msg--;
				}
			}
		}
		latency_util.reset_total();
    	myfile << "SUMMARY LATENCY" << '\n';
		myfile << latency_util.average_latency()<<", "<<latency_util.medium_latency()<<", "<< latency_util.the95_latency() <<", "<<latency_util.the99_latency()<<", "<<latency_util.the999_latency()<< '\n';
	}
	else if (configuration_->all_nodes[configuration_->this_node_id]->replica_id == 0){
		// Pack up my data		
		MessageProto message;
		message.set_destination_channel("sequencer");	
		message.set_source_node(configuration_->this_node_id);
		std::cout<<"Sent message to "<<0<<std::endl;
		message.set_destination_node(0);	
		message.set_type(MessageProto::LATENCY);	
		for(int i = 0; i < 1000; ++i){
			if (latency_util.small_lat[i]!=0)
			{
				message.add_latency(i);
				message.add_count(latency_util.small_lat[i]);
			}
		}	
		for(uint i = 0; i < latency_util.large_lat.size(); ++i){
			message.add_latency(latency_util.large_lat[i]);
			message.add_count(1);
		}	
		connection_->Send(message);
		Spin(1);
	}

    myfile.close();
}
