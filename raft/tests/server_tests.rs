use std::sync::mpsc::{Sender, Receiver, channel};
use raft::{ServerConfig, RaftServer};
use raft::network::{ClientChannel, NetworkChannel};
use raft::common::{AppendEntriesResponse, RequestVoteRequest, AppendEntriesRequest, RequestVoteResponse};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;


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
        self.server_response_sender.send(message).unwrap();
    }

    pub fn receive_request_from_client(&self) -> SReq {
        self.server_request_receiver.recv().unwrap()
    }
}


struct TestInnerClientChannel {
    request_vote_channel:ClientSender<RequestVoteRequest, RequestVoteResponse>,
    append_entries_channel:ClientSender<AppendEntriesRequest, AppendEntriesResponse>,
}

//#[derive(Clone)]
struct TestClientChannel {
    inner_client_mutex: Mutex<TestInnerClientChannel>,
}

impl TestClientChannel {
    pub fn new(request_vote_channel: ClientSender<RequestVoteRequest, RequestVoteResponse>,
               append_entries_channel:ClientSender<AppendEntriesRequest, AppendEntriesResponse>) -> Self {
        TestClientChannel { inner_client_mutex: Mutex::new(TestInnerClientChannel {request_vote_channel,  append_entries_channel}) }
    }
}

impl ClientChannel for TestClientChannel {

    fn send_request_vote(&self, request_vote_request: RequestVoteRequest) -> Result<RequestVoteResponse, ()> {
        let inner_client=self.inner_client_mutex.lock().unwrap();
        inner_client.request_vote_channel.send_request_to_server(request_vote_request);
        Ok(inner_client.request_vote_channel.receive_response_from_server())
    }

    fn send_append_entries(&self, append_entries_request: AppendEntriesRequest) -> Result<AppendEntriesResponse, ()> {
        let inner_client=self.inner_client_mutex.lock().unwrap();
        inner_client.append_entries_channel.send_request_to_server(append_entries_request);
        Ok(inner_client.append_entries_channel.receive_response_from_server())
    }
}

struct RaftTestServerImpl {
    raft_server: Arc<RaftServer<TestClientChannel>>,
}

impl RaftTestServerImpl {

    pub fn start(server_name: String, server_config: ServerConfig,channel_factory: RaftTestNetworkChannelFactory) {
        let raft_test_server_impl=RaftTestServerImpl {
            raft_server: Arc::new(RaftServer::new(server_config, channel_factory.get_network_channel(server_name.clone()))),
        };
        let raft_server_server_state= raft_test_server_impl.get_raft_server();
        thread::spawn(move || {
            raft_server_server_state.manage_server_state();
        });

        let mut server_list_iterator= channel_factory.get_server_channels_for_request_vote(&server_name).into_iter();
        while let Some(server_receiver)= server_list_iterator.next() {
            let raft_server_server_for_thread= raft_test_server_impl.get_raft_server();
            thread::spawn(move || {
                let request_in=server_receiver.receive_request_from_client();
                let response=raft_server_server_for_thread.on_request_vote(request_in);
                server_receiver.send_response_to_client(response);
            });
        }

        let mut server_list_iterator= channel_factory.get_server_channels_for_append_log_entries(&server_name).into_iter();
        while let Some(server_receiver)= server_list_iterator.next() {
            let raft_server_server_for_thread= raft_test_server_impl.get_raft_server();
            thread::spawn(move || {
                let request_in=server_receiver.receive_request_from_client();
                let response=raft_server_server_for_thread.on_append_entries(request_in);
                server_receiver.send_response_to_client(response);
            });
        }


    }

    fn get_raft_server(&self) -> Arc<RaftServer<TestClientChannel>>{
        self.raft_server.clone()
    }
}

/*
TODO: verificare se non esiste un metodo senza mutex per fare tutto questo
Senza complicare troppo l'algoritmo e mantenendo semplice il codice
 */
struct RaftTestNetworkChannelFactory {
    client_map_mutex: Mutex<HashMap<String, HashMap<String, TestClientChannel>>>,
    server_map_for_append_entries: Mutex<HashMap<String, Vec<ServerReceiver<AppendEntriesRequest, AppendEntriesResponse>>>>,
    server_map_for_send_request_vote: Mutex<HashMap<String, Vec<ServerReceiver<RequestVoteRequest, RequestVoteResponse>>>>
}

impl RaftTestNetworkChannelFactory {
    pub fn new(server_address_list: Vec<String>) -> RaftTestNetworkChannelFactory {
        let local_client_map_mutex=Mutex::new(HashMap::new());
        let local_server_map_for_append_entries_mutex=Mutex::new(HashMap::new());
        let local_server_map_for_send_request_vote_mutex=Mutex::new(HashMap::new());
        /*
TODO spiegare perchè qui va qui { --> prendo il lock e devo rilasciarlo prima di restituire il valore
se non metto niente viene rilasciato a fine metodo
 */
        {

        /*
        TODO spiegare perchè qui va qui unwrap al posto delle righe 142 e successive
         */
            let mut local_client_map = local_client_map_mutex.lock().unwrap();
            let mut local_server_map_for_append_entries = local_server_map_for_append_entries_mutex.lock().unwrap();
            let mut local_server_map_for_send_request_vote = local_server_map_for_send_request_vote_mutex.lock().unwrap();
            /*
        TODO spiegare perchè qui va &server_address_list al posto di server_address_list
         */
            for server_address_from in &server_address_list {
                let mut local_client_map_for_server = HashMap::new();
                let mut server_receiver_for_append_log_entries_list = vec![];
                let mut server_receiver_for_request_vote_list = vec![];
                for server_address_to in &server_address_list {
                    if server_address_from != server_address_to {
                        let (sender_for_request_vote_req, receiver_for_request_vote_req) = channel();
                        let (sender_for_request_vote_resp, receiver_for_request_vote_resp) = channel();
                        let (sender_for_append_log_entries_req, receiver_for_append_log_entries_req) = channel();
                        let (sender_for_append_log_entries_resp, receiver_for_append_log_entries_resp) = channel();
                        let client_sender_for_request_vote = ClientSender::new(sender_for_request_vote_req, receiver_for_request_vote_resp);
                        let server_receiver_for_request_vote = ServerReceiver::new(receiver_for_request_vote_req, sender_for_request_vote_resp);
                        let client_sender_for_append_log_entries = ClientSender::new(sender_for_append_log_entries_req, receiver_for_append_log_entries_resp);
                        let server_receiver_for_append_log_entries = ServerReceiver::new(receiver_for_append_log_entries_req, sender_for_append_log_entries_resp);
                        let client_channel = TestClientChannel::new(client_sender_for_request_vote, client_sender_for_append_log_entries);
                        local_client_map_for_server.insert(server_address_to.clone(), client_channel);
                        server_receiver_for_append_log_entries_list.push(server_receiver_for_append_log_entries);
                        server_receiver_for_request_vote_list.push(server_receiver_for_request_vote);
                    }
                }
                local_client_map.insert(server_address_from.clone(), local_client_map_for_server);
                local_server_map_for_append_entries.insert(server_address_from.clone(), server_receiver_for_append_log_entries_list);
                local_server_map_for_send_request_vote.insert(server_address_from.clone(), server_receiver_for_request_vote_list);
            }
        }
        RaftTestNetworkChannelFactory {
            client_map_mutex: local_client_map_mutex,
            server_map_for_append_entries: local_server_map_for_append_entries_mutex,
            server_map_for_send_request_vote: local_server_map_for_send_request_vote_mutex
        }
    }

    pub fn get_network_channel(&self, server_address: String) -> RaftTestNetworkChannel {
        let mut local_client_map=self.client_map_mutex.lock().unwrap();
        RaftTestNetworkChannel::new(local_client_map.remove(&server_address).unwrap())
    }

    pub fn get_server_channels_for_request_vote(&self, server_address: &String) -> Vec<ServerReceiver<RequestVoteRequest, RequestVoteResponse>> {
        let mut local_server_map_for_send_request_vote=self.server_map_for_send_request_vote.lock().unwrap();
        local_server_map_for_send_request_vote.remove(server_address).unwrap()
    }

    pub fn get_server_channels_for_append_log_entries(&self, server_address: &String) -> Vec<ServerReceiver<AppendEntriesRequest, AppendEntriesResponse>> {
        let mut local_server_map_for_append_entries=self.server_map_for_append_entries.lock().unwrap();
        local_server_map_for_append_entries.remove(server_address).unwrap()
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
        let mut client_map=self.client_map_mutex.lock().unwrap();
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