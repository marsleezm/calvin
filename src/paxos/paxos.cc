// Author: Kun Ren (kun.ren@yale.edu)
//         Alexander Thomson (thomson@cs.yale.edu)
// The Paxos object allows batches to be registered with a running zookeeper
// instance, inserting them into a globally consistent batch order.

#include "paxos/paxos.h"

#include <fstream>
#include <utility>
#include <vector>

using std::ifstream;
using std::pair;
using std::vector;


Paxos::Paxos(vector<Node*>& my_group, Node* myself_n, Connection* paxos_connection, int p_id, int num_p): group(my_group), myself(myself_n), num_partitions(num_p), partition_id(p_id), connection(paxos_connection) {
	//Set CPU affinity	
    pthread_mutex_init(&mutex_, NULL);
	leader = group[0];
	group_size = group.size();
   	cpu_set_t cpuset;

    pthread_attr_t attr_thread;
    pthread_attr_init(&attr_thread);
    //pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    CPU_SET(0, &cpuset);
    pthread_attr_setaffinity_np(&attr_thread, sizeof(cpu_set_t), &cpuset);
    std::cout << "Paxos starts at core 0"<<std::endl;

    pthread_create(&paxos_thread, &attr_thread, InitRunPaxos,
          reinterpret_cast<void*>(this));
}

Paxos::~Paxos() {
	deconstructor_invoked_ = true;
	pthread_join(paxos_thread, NULL);
	delete connection;
}

void* Paxos::InitRunPaxos(void *arg) {
  reinterpret_cast<Paxos*>(arg)->RunPaxos();
  return NULL;
}

void Paxos::HandleClientProposal(MessageProto& message, int& batch_to_prop){
	int batch_num = message.batch_number();
	if(client_prop_map.count(batch_num) == 0){
		//std::cout<<"Got client proposal for "<<batch_num<<", cnt is 1 from "<<message.source_node()<<std::endl;
		MessageProto* msg_list = new MessageProto[group_size];
		msg_list[message.source_node()] = message;
		client_prop_map[batch_num] = make_pair(1, msg_list);
	}
	else{
		pair<int, MessageProto*> msgs = client_prop_map[batch_num];
		//std::cout<<"Got client proposal for "<<batch_num<<", cnt is "<<msgs.first+1<<" from "<<message.source_node()<<std::endl;
		msgs.first = msgs.first+1;
		msgs.second[message.source_node()] = message;
		client_prop_map[batch_num] = msgs;
	}
	while(client_prop_map.count(batch_to_prop) != 0 && client_prop_map[batch_to_prop].first == group_size){
		pair<int, MessageProto*> msgs = client_prop_map[batch_to_prop];
		MessageProto decision_msg;
		decision_msg.set_batch_number(batch_to_prop);
		decision_msg.set_destination_channel("paxos");
		decision_msg.set_type(MessageProto::LEADER_PROPOSAL);
		for( int i = 0; i < group_size; ++i) {
			for( int j = 0; j <msgs.second[i].data_size(); ++j)
				decision_msg.add_data(msgs.second[i].data(j));	
		}
		//std::cout<<"Sending decision for client proposal "<<batch_to_prop<<std::endl;
		delete[] msgs.second;
		client_prop_map.erase(batch_to_prop);
		SendMsgToAll(decision_msg);
		batch_to_prop += 1;
	}
}

void Paxos::RunPaxos() {
	MessageProto message;
	int batch_to_prop = partition_id;
	int batch_to_accept = partition_id;
	int quorum_size = group_size/2+1;
  	while (!deconstructor_invoked_) {
		// If has received enough proposal message, propose it!
		//if(message_queue->Pop(&message)){
		if(connection->GetMessage(&message)){
			if(message.type() == MessageProto::CLIENT_PROPOSAL){
				assert(leader == myself);
				HandleClientProposal(message, batch_to_prop);
			}	
			else if(message.type() == MessageProto::LEADER_PROPOSAL){
				//std::cout<<"Sending accept for leader proposal for "<<message.batch_number()<<std::endl;
				if(leader_prop_map.count(message.batch_number()) == 0){
					leader_prop_map[message.batch_number()] = make_pair(0, new MessageProto(message));	
				}
				else{
					pair<int, MessageProto*> proposal_msg = leader_prop_map[message.batch_number()];
					assert(proposal_msg.second == NULL);
					leader_prop_map[message.batch_number()].second = new MessageProto(message);
				}
				MessageProto msg;
				msg.set_type(MessageProto::LEARNER_ACCEPT);
				msg.set_destination_channel("paxos");
				msg.set_batch_number(message.batch_number());
				SendMsgToAll(msg);
			}
			else{
				assert(message.type() == MessageProto::LEARNER_ACCEPT);
				if(message.batch_number() >= batch_to_accept){
					if(leader_prop_map.count(message.batch_number()) == 0) {
						//std::cout<<"Got new learner accept "<<message.batch_number()<<std::endl;
						leader_prop_map[message.batch_number()] = pair<int, MessageProto*>(1, NULL);	
					}
					else{
						pair<int, MessageProto*> proposal_msg = leader_prop_map[message.batch_number()];
						//std::cout<<"Got learner accept for "<<message.batch_number()<<", count is "<<proposal_msg.first<<std::endl;
						proposal_msg.first += 1;
						leader_prop_map[message.batch_number()] = proposal_msg;	
					}
					while(leader_prop_map.count(batch_to_accept) != 0 && leader_prop_map[batch_to_accept].first == quorum_size && leader_prop_map[batch_to_accept].second != NULL) {
						//std::cout<<"Accepting batch "<<batch_to_accept<<", his batch is "<<leader_prop_map[batch_to_accept].second->batch_number()<<std::endl;
						leader_prop_map[batch_to_accept].second->set_destination_channel("scheduler_");
						leader_prop_map[batch_to_accept].second->set_type(MessageProto::TXN_BATCH);
						leader_prop_map[batch_to_accept].second->set_destination_node(myself->node_id);
						pthread_mutex_lock(&mutex_);
						connection->Send(*leader_prop_map[batch_to_accept].second);
						pthread_mutex_unlock(&mutex_);
						delete leader_prop_map[batch_to_accept].second;
						leader_prop_map.erase(batch_to_accept);
						batch_to_accept += 1;
					}
				}
			}
		}
		Spin(0.0005);
 	}
}

void Paxos::SendMsgToAll(MessageProto& msg){
	for(int i = 0; i < group_size; ++i){
		msg.set_destination_node(group[i]->node_id);
		pthread_mutex_lock(&mutex_);
		connection->Send(msg);
		pthread_mutex_unlock(&mutex_);
	}	
}

void Paxos::SendMsgToAllOthers(MessageProto& msg){
	for(int i = 0; i < group_size; ++i){
		if(group[i]->node_id != myself->node_id){
			msg.set_destination_node(group[i]->node_id);
			pthread_mutex_lock(&mutex_);
			connection->Send(msg);
			pthread_mutex_unlock(&mutex_);
		}
	}	
}

void Paxos::SubmitBatch(MessageProto& batch_msg) {
	// Send batch to leader
    batch_msg.set_destination_node(leader->node_id);
	batch_msg.set_type(MessageProto::CLIENT_PROPOSAL);
	pthread_mutex_lock(&mutex_);
	connection->Send(batch_msg);
	pthread_mutex_unlock(&mutex_);
}

void Paxos::SubmitBatch(MessageProto* batch_msg) {
	// Send batch to leader
	//std::cout<<"Sending msg of batch "<<batch_msg->batch_number()<<" to paxos" <<std::endl;
    batch_msg->set_destination_node(leader->node_id);
	batch_msg->set_type(MessageProto::CLIENT_PROPOSAL);
	pthread_mutex_lock(&mutex_);
	connection->Send(*batch_msg);
	pthread_mutex_unlock(&mutex_);
	delete batch_msg;
}
