use std::sync::mpsc::{Sender, Receiver, channel};
use raft::ServerConfig;
use raft::network::{ClientChannel, NetworkChannel};
use raft::common::{AppendEntriesResponse, RequestVoteRequest, AppendEntriesRequest, RequestVoteResponse};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};


struct ClientSender<SReq,SResp> {
    server_request_sender: Sender<SReq>,
    server_response_receiver: Receiver<SResp>,
}

impl <SReq,SResp> ClientSender<SReq, SResp> {

    pub fn new(server_request_sender: Sender<SReq>, server_response_receiver: Receiver<SResp>) -> ClientSender<SReq, SResp> {
        ClientSender {
            server_request_sender,
            server_response_receiver,
        }
    }

    pub fn send_request_to_server(&self, message: SReq)  {
        self.server_request_sender.send(message).unwrap();
    }

    pub fn receive_response_from_server(&self) -> SResp {
        self.server_response_receiver.recv().unwrap()
    }
}

struct ServerReceiver<SReq,SResp> {
    server_request_receiver: Receiver<SReq>,
    server_response_sender: Sender<SResp>,
}

impl <SReq,SResp> ServerReceiver<SReq, SResp> {

    pub fn new(server_request_receiver: Receiver<SReq>, server_response_sender: Sender<SResp>) -> ServerReceiver<SReq, SResp> {
        ServerReceiver {
            server_request_receiver,
            server_response_sender,
        }
    }

    pub fn send_response_to_client(&self, message: SResp)  {
        self.server_request_sender.send(message).unwrap();
    }

    pub fn receive_request_from_client(&self) -> SReq {
        self.server_request_receiver.recv().unwrap()
    }
}


//#[derive(Clone)]
struct TestClientChannel {
    request_vote_channel:ClientSender<RequestVoteRequest, RequestVoteResponse>,
    append_entries_channel:ClientSender<AppendEntriesRequest, AppendEntriesResponse>,
}

impl TestClientChannel {
    pub fn new(request_vote_channel: ClientSender<RequestVoteRequest, RequestVoteResponse>,
               append_entries_channel:ClientSender<AppendEntriesRequest, AppendEntriesResponse>) -> Self {
        TestClientChannel { request_vote_channel,  append_entries_channel }
    }
}

impl ClientChannel for TestClientChannel {

    fn send_request_vote(&self, request_vote_request: RequestVoteRequest) -> Result<RequestVoteResponse, ()> {
        self.request_vote_channel.send_request_to_server(request_vote_request);
        Ok(self.request_vote_channel.receive_response_from_server())
    }

    fn send_append_entries(&self, append_entries_request: AppendEntriesRequest) -> Result<AppendEntriesResponse, ()> {
        self.append_entries_channel.send_request_to_server(append_entries_request);
        Ok(self.append_entries_channel.receive_response_from_server())
    }
}

struct RaftTestServerImpl {

}

impl RaftTestServerImpl {
    pub fn start(server_config: ServerConfig) {

    }
}

/*
TODO: verificare se non esiste un metodo senza mutex per fare tutto questo
Senza complicare troppo l'algoritmo e mantenendo semplice il codice
 */
struct RaftTestNetworkChannelFactory {
    client_map: Mutex<HashMap<String, TestClientChannel>>,
    server_map_for_append_entries: Mutex<HashMap<String, ServerReceiver<AppendEntriesRequest, AppendEntriesResponse>>>,
    server_map_for_send_request_vote: Mutex<HashMap<String, ServerReceiver<RequestVoteRequest, RequestVoteResponse>>>
}

impl RaftTestNetworkChannelFactory {
    pub fn new(server_address_list: Vec<String>) -> RaftTestNetworkChannelFactory {
        let local_client_map_mutex=Mutex:new(Default::new());
        let local_server_map_for_append_entries_mutex=Mutex:new(Default::new());
        let local_server_map_for_send_request_vote_mutex=Mutex:new(Default::new());
        let local_client_map=local_client_map_mutex.lock();
        let local_server_map_for_append_entries=local_server_map_for_append_entries_mutex.lock();
        let local_server_map_for_send_request_vote=local_server_map_for_send_request_vote_mutex.lock();
        for server_address_from in server_address_list {
            for server_address_to in server_address_list {
                if server_address_from!=server_address_to {
                    let (sender_for_request_vote_req, receiver_for_request_vote_req) = channel();
                    let (sender_for_request_vote_resp, receiver_for_request_vote_resp) = channel();
                    let (sender_for_append_log_entries_req, receiver_for_append_log_entries_req) = channel();
                    let (sender_for_append_log_entries_resp, receiver_for_append_log_entries_resp) = channel();
                    let client_sender_for_request_vote=ClientSender::new(sender_for_request_vote_req,receiver_for_request_vote_resp);
                    let server_receiver_for_request_vote =ServerReceiver:new(receiver_for_request_vote_req,sender_for_request_vote_resp);
                    let client_sender_for_append_log_entries=ClientSender::new(sender_for_append_log_entries_req,receiver_for_append_log_entries_resp);
                    let server_receiver_for_append_log_entries =ServerReceiver:new(receiver_for_append_log_entries_req,sender_for_append_log_entries_resp);
                    let client_channel= TestClientChannel::new(client_sender_for_request_vote,client_sender_for_append_log_entries);
                    local_client_map.insert(server_address_from.clone(),client_channel);
                    local_server_map_for_append_entries.insert(server_address_from.clone(), server_receiver_for_append_log_entries);
                    local_server_map_for_send_request_vote.insert(server_address_from.clone(), server_receiver_for_request_vote);
                }
            }
        }

        RaftTestNetworkChannelFactory {
            client_map: local_client_map_mutex,
            server_map_for_append_entries: local_server_map_for_append_entries_mutex,
            server_map_for_send_request_vote: local_server_map_for_send_request_vote_mutex
        }
    }
}

struct RaftTestNetworkChannel {
    client_map_mutex: Mutex<HashMap<String, TestClientChannel>>
}

impl RaftTestNetworkChannel {
    pub fn new(client_map: HashMap<String, TestClientChannel>) -> RaftTestNetworkChannel {
        RaftTestNetworkChannel{
            client_map_mutex: Mutex::new(client_map)
        }
    }
}

impl NetworkChannel for RaftTestNetworkChannel {
    type Client = TestClientChannel;

    fn client_channel(&self, remote_address: String) -> Self::Client {
        let client_map=self.client_map_mutex.lock();
        if let Some(client) = client_map.remove(&*remote_address) {
            return client;
        } else {
            panic!("client alread taken for {}",remote_address);
        }
    }
}


/*
questo si può mettere anche dentro la impl di una struct
 */
#[test]
fn testThreeServers() {

}