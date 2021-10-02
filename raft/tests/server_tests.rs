use std::sync::mpsc::{Sender, Receiver, channel, RecvTimeoutError};
use raft::{ServerConfig, RaftServer, RaftServerState};
use raft::network::{ClientChannel, NetworkChannel, RaftClient};
use raft::common::{AppendEntriesResponse, RequestVoteRequest, AppendEntriesRequest, RequestVoteResponse, ApplyCommandRequest, ApplyCommandResponse, StateMachineCommand};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{thread, time, env};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::Sleep;
use std::time::Duration;
use rand::Rng;
use std::fs::create_dir;


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

    pub fn receive_request_from_client(&self) -> Result<SReq,RecvTimeoutError> {
        let timeout = Duration::from_millis(50);
        self.server_request_receiver.recv_timeout(timeout)
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
    server_name: String,
    channel_factory: Arc<RaftTestNetworkChannelFactory>,
    shutdown: Arc<AtomicBool>,
    raft_client: Mutex<Option<TestRaftClient>>,
    apply_command_server_receiver: Mutex<Option<ServerReceiver<ApplyCommandRequest, ApplyCommandResponse>>>

}

impl RaftTestServerImpl {

    pub fn new(server_name: String, server_config: ServerConfig,channel_factory: Arc<RaftTestNetworkChannelFactory>) -> RaftTestServerImpl {
        let (sender_for_apply_command_req, receiver_for_apply_command_req) = channel();
        let (sender_for_apply_command_resp, receiver_for_apply_command_resp) = channel();
        RaftTestServerImpl {
            raft_server: Arc::new(RaftServer::new(server_config, channel_factory.get_network_channel(server_name.clone()))),
            server_name,
            channel_factory,
            shutdown:Arc::new(AtomicBool::new(false)),
            raft_client: Mutex::new(Some(TestRaftClient::new(ClientSender::new(sender_for_apply_command_req, receiver_for_apply_command_resp)))),
            apply_command_server_receiver: Mutex::new(Some(ServerReceiver::new(receiver_for_apply_command_req, sender_for_apply_command_resp)))
        }
    }
    pub fn stop(&self) {
        let cloned=self.shutdown.clone();
        self.shutdown.store(true, Ordering::SeqCst);
        println!("stop shutdown value {}", cloned.load(Ordering::SeqCst));
    }

    pub fn start(&self) {

        let raft_server_server_state= self.get_raft_server();
        println!("server:{} - before manage_server_state",self.server_name);
        let shutdown_server_state_thread=self.shutdown.clone();
        thread::spawn(move || {
            while !shutdown_server_state_thread.load(Ordering::SeqCst) {
                raft_server_server_state.manage_server_state();
            }
        });

        let mut children = vec![];

        let mut server_list_iterator= self.channel_factory.get_server_channels_for_request_vote(&self.server_name).into_iter();
        while let Some(server_receiver)= server_list_iterator.next() {
            let raft_server_server_for_thread= self.get_raft_server();
            let shutdown_thread=self.shutdown.clone();
            children.push(thread::spawn(move || {
                while !shutdown_thread.load(Ordering::SeqCst) {
                    let result_request_in = server_receiver.receive_request_from_client();
                    if result_request_in.is_ok() {
                        let request_in=result_request_in.unwrap();
                        let response = raft_server_server_for_thread.on_request_vote(request_in);
                        server_receiver.send_response_to_client(response);
                    }
                }
                println!("exit from on_request_vote thread");
            }));
        }

        let mut server_list_iterator= self.channel_factory.get_server_channels_for_append_log_entries(&self.server_name).into_iter();
        while let Some(server_receiver)= server_list_iterator.next() {
            let raft_server_server_for_thread= self.get_raft_server();
            let shutdown_thread=self.shutdown.clone();
            children.push(thread::spawn(move || {
                while !shutdown_thread.load(Ordering::SeqCst) {
                    let result_request_in = server_receiver.receive_request_from_client();

                    if result_request_in.is_ok() {
                        let request_in = result_request_in.unwrap();
                        let response = raft_server_server_for_thread.on_append_entries(request_in);
                        server_receiver.send_response_to_client(response);
                    }
                }
                println!("exit from append_entries thread");
            }));
        }
        let shutdown_thread=self.shutdown.clone();
        let raft_server_server_for_thread= self.get_raft_server();
        let mut server_receiver_guard=self.apply_command_server_receiver.lock().unwrap();
        let server_receiver = server_receiver_guard.take().unwrap();
        thread::spawn(move || {
            while !shutdown_thread.load(Ordering::SeqCst) {

                let result_request_in = server_receiver.receive_request_from_client();
                println!("got apply command");
                if result_request_in.is_ok() {
                    let request_in = result_request_in.unwrap();
                    let response = raft_server_server_for_thread.on_apply_command(request_in);
                    server_receiver.send_response_to_client(response);
                }
            }
            println!("exit from apply_command thread");
        });

        println!("server:{} - before join",&self.server_name);
        for child in children {
            // Wait for the thread to finish. Returns a result.
            let _ = child.join();
        }


    }

    fn get_raft_server(&self) -> Arc<RaftServer<TestClientChannel>>{
        self.raft_server.clone()
    }

    pub fn raft_server_state(&self) -> RaftServerState {
        self.raft_server.server_state()
    }

    fn get_raft_client(&self) -> TestRaftClient {
        self.raft_client.lock().unwrap().take().unwrap()}
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
        let local_client_map_mutex: Mutex<HashMap<String, HashMap<String, TestClientChannel>>>=Mutex::new(HashMap::new());
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
                //let mut local_client_map_for_server = HashMap::new();
                let mut server_receiver_for_append_log_entries_list = vec![];
                let mut server_receiver_for_request_vote_list = vec![];
                for server_address_to in &server_address_list {
                    if server_address_from != server_address_to {
                        println!("creating channel for {} -> {}", server_address_from, server_address_to);
                        let (sender_for_request_vote_req, receiver_for_request_vote_req) = channel();
                        let (sender_for_request_vote_resp, receiver_for_request_vote_resp) = channel();
                        let (sender_for_append_log_entries_req, receiver_for_append_log_entries_req) = channel();
                        let (sender_for_append_log_entries_resp, receiver_for_append_log_entries_resp) = channel();
                        let client_sender_for_request_vote = ClientSender::new(sender_for_request_vote_req, receiver_for_request_vote_resp);
                        let server_receiver_for_request_vote = ServerReceiver::new(receiver_for_request_vote_req, sender_for_request_vote_resp);
                        let client_sender_for_append_log_entries = ClientSender::new(sender_for_append_log_entries_req, receiver_for_append_log_entries_resp);
                        let server_receiver_for_append_log_entries = ServerReceiver::new(receiver_for_append_log_entries_req, sender_for_append_log_entries_resp);
                        let client_channel = TestClientChannel::new(client_sender_for_request_vote, client_sender_for_append_log_entries);
                        if local_client_map.contains_key(&server_address_to.clone()) {
                            let mut local_client_map_for_server = local_client_map.get_mut(&server_address_to.clone()).unwrap();
                            local_client_map_for_server.insert(server_address_from.clone(), client_channel);
                        } else {
                            let mut local_client_map_for_server = HashMap::new();
                            local_client_map_for_server.insert(server_address_from.clone(), client_channel);
                            local_client_map.insert(server_address_to.clone(), local_client_map_for_server);
                        }
                        server_receiver_for_append_log_entries_list.push(server_receiver_for_append_log_entries);
                        server_receiver_for_request_vote_list.push(server_receiver_for_request_vote);
                    }
                }
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
        println!("get_server_channels_for_request_vote for {}", server_address);
        let mut local_server_map_for_send_request_vote=self.server_map_for_send_request_vote.lock().unwrap();
        local_server_map_for_send_request_vote.remove(server_address).unwrap()
    }

    pub fn get_server_channels_for_append_log_entries(&self, server_address: &String) -> Vec<ServerReceiver<AppendEntriesRequest, AppendEntriesResponse>> {
        println!("get_server_channels_for_append_log_entries for {}", server_address);
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

struct TestInnerRaftClient {
    apply_command_channel: ClientSender<ApplyCommandRequest, ApplyCommandResponse>,
}

struct TestRaftClient {
    inner_client_mutex: Mutex<TestInnerRaftClient>,
}

impl TestRaftClient {
    pub fn new(apply_command_channel: ClientSender<ApplyCommandRequest, ApplyCommandResponse>) -> Self {
        TestRaftClient { inner_client_mutex: Mutex::new(TestInnerRaftClient {apply_command_channel}) }
    }
}

impl RaftClient for TestRaftClient {

    fn apply_command(&self, apply_command_request: ApplyCommandRequest) -> Result<ApplyCommandResponse, ()> {
        let inner_client=self.inner_client_mutex.lock().unwrap();
        inner_client.apply_command_channel.send_request_to_server(apply_command_request);
        Ok(inner_client.apply_command_channel.receive_response_from_server())
    }
}

/*
questo si può mettere anche dentro la impl di una struct
 */



fn create_test_dir() -> String {
    let dir = env::temp_dir();
    let mut rng = rand::thread_rng();
    let dir = format!("{}{}",dir.display(), rng.gen::<u32>());
    println!("dir={}",dir);
    create_dir(&dir).unwrap();
    dir
}

#[test]
fn testThreeServers() {
    let server_config_1=ServerConfig::new(1,65,100, 9090,vec![String::from("server2"),String::from("server3")],create_test_dir(),create_test_dir());
    let server_config_2=ServerConfig::new(2,65,100, 9091,vec![String::from("server1"),String::from("server3")],create_test_dir(), create_test_dir());
    let server_config_3=ServerConfig::new(3,65,100, 9092,vec![String::from("server1"),String::from("server2")],create_test_dir(), create_test_dir());

    let mut children = vec![];

    /*
Se non raccolgo gli handle il programma finisce subito
 */
    let channel_factory=Arc::new(RaftTestNetworkChannelFactory::new(vec![String::from("server1"),String::from("server2"),String::from("server3")]));
    println!("before starting servers");
    let server1=Arc::new(RaftTestServerImpl::new(String::from("server1"),server_config_1,channel_factory.clone()));
    let server1_thread=server1.clone();
    children.push(thread::spawn(move || {
        println!("inside starting server 1");
        server1_thread.start();
    }));
    let server2=Arc::new(RaftTestServerImpl::new(String::from("server2"),server_config_2,channel_factory.clone()));
    let server2_thread=server2.clone();
    children.push(thread::spawn(move || {
        server2_thread.start();
    }));
    let server3=Arc::new(RaftTestServerImpl::new(String::from("server3"),server_config_3,channel_factory.clone()));
    let server3_thread=server3.clone();
    children.push(thread::spawn(move || {
        server3_thread.start();
    }));

    let sleep_time = time::Duration::from_millis(3000);
    thread::sleep(sleep_time);
    server1.stop();
    server2.stop();
    server3.stop();
    for child in children {
        // Wait for the thread to finish. Returns a result.
        let _ = child.join();
    }
    let server_state_list=vec![server1.raft_server_state(),server2.raft_server_state(), server3.raft_server_state()];
    assert_eq!(server_state_list.iter().filter(|server_state| **server_state==RaftServerState::Follower).count(),2);
    assert_eq!(server_state_list.iter().filter(|server_state| **server_state==RaftServerState::Leader).count(),1);
    assert_eq!(server_state_list.iter().filter(|server_state| **server_state==RaftServerState::Candidate).count(),0);
}

#[test]
fn testApplyCommandThreeServers() {
    println!("testApplyCommandThreeServers start");
    let server_config_1=ServerConfig::new(1,65,100, 9090,vec![String::from("server2"),String::from("server3")],create_test_dir(),create_test_dir());
    let server_config_2=ServerConfig::new(2,65,100, 9091,vec![String::from("server1"),String::from("server3")],create_test_dir(), create_test_dir());
    let server_config_3=ServerConfig::new(3,65,100, 9092,vec![String::from("server1"),String::from("server2")],create_test_dir(), create_test_dir());

    let mut children = vec![];

    /*
Se non raccolgo gli handle il programma finisce subito
 */
    let channel_factory=Arc::new(RaftTestNetworkChannelFactory::new(vec![String::from("server1"),String::from("server2"),String::from("server3")]));
    println!("before starting servers");
    let mut server1=Arc::new(RaftTestServerImpl::new(String::from("server1"),server_config_1,channel_factory.clone()));
    let server1_thread=server1.clone();
    children.push(thread::spawn(move || {
        println!("inside starting server 1");
        server1_thread.start();
    }));
    let mut server2=Arc::new(RaftTestServerImpl::new(String::from("server2"),server_config_2,channel_factory.clone()));
    let server2_thread=server2.clone();
    children.push(thread::spawn(move || {
        server2_thread.start();
    }));
    let mut server3=Arc::new(RaftTestServerImpl::new(String::from("server3"),server_config_3,channel_factory.clone()));
    let server3_thread=server3.clone();
    children.push(thread::spawn(move || {
        server3_thread.start();
    }));

    let sleep_time = time::Duration::from_millis(3000);
    thread::sleep(sleep_time);
    let mut client=None;
    if server1.raft_server_state()==RaftServerState::Leader {
        let c=server1.get_raft_client();
        client=Some(c);
    } else if server2.raft_server_state()==RaftServerState::Leader {
        let c=server2.get_raft_client();
        client=Some(c);
    } else if server3.raft_server_state()==RaftServerState::Leader {
        let c=server3.get_raft_client();
        client=Some(c);
    }
    if client.is_some() {
        let apply_command_request=ApplyCommandRequest::new(StateMachineCommand::Put { key: "key".to_string(), value: "value".to_string() });
        client.take().unwrap().apply_command(apply_command_request);
    }
    server1.stop();
    server2.stop();
    server3.stop();
    for child in children {
        // Wait for the thread to finish. Returns a result.
        let _ = child.join();
    }
    let server_state_list=vec![server1.raft_server_state(),server2.raft_server_state(), server3.raft_server_state()];
    assert_eq!(server_state_list.iter().filter(|server_state| **server_state==RaftServerState::Follower).count(),2);
    assert_eq!(server_state_list.iter().filter(|server_state| **server_state==RaftServerState::Leader).count(),1);
    assert_eq!(server_state_list.iter().filter(|server_state| **server_state==RaftServerState::Candidate).count(),0);
}

#[test]
fn test_start_one_server() {
    let server_config_1=ServerConfig::new(1,65,100, 9090,vec![String::from("server2"),String::from("server3")],String::from("d:\\temp\\10\\wal"),String::from("d:\\temp\\10\\sm"));

    let mut children = vec![];

    /*
Se non raccolgo gli handle il programma finisce subito
 */
    let channel_factory=Arc::new(RaftTestNetworkChannelFactory::new(vec![String::from("server1"),String::from("server2"),String::from("server3")]));
    println!("before starting servers");
    let server1=Arc::new(RaftTestServerImpl::new(String::from("server1"),server_config_1,channel_factory.clone()));
    let server1_thread=server1.clone();
    children.push(thread::spawn(move || {
        println!("inside starting server 1");
        server1_thread.start();
    }));


    let sleep_time = time::Duration::from_millis(3000);
    thread::sleep(sleep_time);
    server1.stop();
    for child in children {
        // Wait for the thread to finish. Returns a result.
        let _ = child.join();
    }
}