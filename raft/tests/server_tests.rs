use std::sync::mpsc::{Sender, Receiver, channel};
use raft::ServerConfig;
use raft::network::{ClientChannel, NetworkChannel};
use raft::common::{AppendEntriesResponse, RequestVoteRequest, AppendEntriesRequest, RequestVoteResponse};
use std::collections::HashMap;
use std::sync::Arc;


struct ClientServerChannel<SReq,SResp> {
    server_request_sender: Sender<SReq>,
    server_request_receiver: Receiver<SReq>,
    server_response_sender: Sender<SResp>,
    server_response_receiver: Receiver<SResp>,
}

impl <SReq,SResp> ClientServerChannel<SReq, SResp> {

    pub fn new() -> ClientServerChannel<SReq, SResp> {
        let(c_req_sender,c_req_receiver)=channel::<SReq>();
        let(c_resp_sender,c_resp_receiver)=channel::<SResp>();
        ClientServerChannel {
            server_request_sender: c_req_sender,
            server_request_receiver: c_req_receiver,
            server_response_sender: c_resp_sender,
            server_response_receiver: c_resp_receiver,
        }
    }

    pub fn send_request_to_server(&self, message: SReq)  {
        self.server_request_sender.send(message).unwrap();
    }

    pub fn receive_request_from_client(&self) -> SReq {
        self.server_request_receiver.recv().unwrap()
    }

    pub fn send_response_to_client(&self, message: SResp)  {
        self.server_response_sender.send(message).unwrap();
    }

    pub fn receive_response_from_server(&self) -> SResp {
        self.server_response_receiver.recv().unwrap()
    }
}

//#[derive(Clone)]
struct TestClientChannel {
    request_vote_channel:ClientServerChannel<RequestVoteRequest, RequestVoteResponse>,
    append_entries_channel:ClientServerChannel<AppendEntriesRequest, AppendEntriesResponse>,
}

impl TestClientChannel {
    pub fn new() -> Self {
        TestClientChannel { request_vote_channel: ClientServerChannel::new(),  append_entries_channel: ClientServerChannel::new() }
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

struct RaftTestNetworkChannel {
    client_map: HashMap<String, Arc<TestClientChannel>>
}

impl RaftTestNetworkChannel {
    pub fn new() -> RaftTestNetworkChannel {
        RaftTestNetworkChannel{
            client_map: Default::default()
        }
    }
}

impl NetworkChannel for RaftTestNetworkChannel {
    type Client = TestClientChannel;

    fn client_channel(&self, remote_address: String) -> Self::Client {
        let client_ref=self.client_map.get(&*remote_address).unwrap();
        //client_ref.clone().d
    }
}


/*
questo si può mettere anche dentro la impl di una struct
 */
#[test]
fn testThreeServers() {

}