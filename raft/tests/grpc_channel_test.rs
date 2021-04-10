use tonic::{transport::Server, Request, Response, Status, Code};
use raft_rpc::raft_rpc_client::{RaftRpcClient};
use raft_rpc::raft_rpc_server::{RaftRpc,RaftRpcServer};

use raft_rpc::{RequestVoteRpcReply, RequestVoteRpcRequest,AppendEntriesRpcRequest,AppendEntriesRpcReply};
use raft_rpc::append_entries_rpc_request::{LogEntryRpc};
use raft::network::{ClientChannel, NetworkChannel};
use raft::common::{RequestVoteRequest, RequestVoteResponse, CandidateIdType, AppendEntriesResponse, AppendEntriesRequest, CommandType, LogEntry, StateMachineCommand};
use raft::{RaftServer, ServerConfig};
use tokio::runtime::Runtime;
use std::sync::{Arc, Mutex};
use std::{thread, time};
use std::ops::Deref;
use chrono::Utc;
use tonic::transport::{Endpoint, Channel};
use std::time::Duration;

pub mod raft_rpc {
    tonic::include_proto!("raft_rpc"); // The string specified here must match the proto package name
}

pub struct RaftRPCServerImplTest {
}

impl RaftRPCServerImplTest {



    pub fn start(server_config: ServerConfig) {
        let addr_str=format!("127.0.0.1:{}",server_config.server_port());
        println!("addr_str={}",addr_str);
        let addr = addr_str.parse().unwrap();
        let raft_rpc_server_impl = RaftRPCServerImplTest { } ;
        //è da mettere qui perchè questa RaftRpcServer::new(raft_rpc_server_impl) fa il borrow

        println!(" after raft_server start");
        let rt = Runtime::new().expect("failed to obtain a new RunTime object");
        let server_future = Server::builder()
            .add_service(RaftRpcServer::new(raft_rpc_server_impl))
            .serve(addr);
        rt.block_on(server_future).expect("failed to successfully run the future on RunTime");

    }
}

#[tonic::async_trait]
impl RaftRpc for RaftRPCServerImplTest {
    async fn request_vote_rpc(
        &self,
        request: Request<RequestVoteRpcRequest>,
    ) -> Result<Response<RequestVoteRpcReply>, Status> {
        //println!("Got a request: {:?}", request);

        let request_obj=request.into_inner();
        let request_in=RequestVoteRequest::new(
            request_obj.term,
            request_obj.candidate_id as CandidateIdType,
            request_obj.last_log_index,
            request_obj.last_log_term,
        );

        let reply = raft_rpc::RequestVoteRpcReply {
            term: 1,
            vote_granted:true,
        };
        Ok(Response::new(reply))
    }

    async fn append_entries_rpc(
        &self,
        request: Request<AppendEntriesRpcRequest>,
    ) -> Result<Response<AppendEntriesRpcReply>, Status> {
        println!("async fn append_entries_rpc at {}",Utc::now().timestamp_millis());

        let reply = raft_rpc::AppendEntriesRpcReply {
            term: 1,
            success: true,
        };
        Ok(Response::new(reply))
    }
}

pub struct RaftRPCClientImplTest {
    address: String,
    opt_channel: Mutex<Option<Channel>>,
}


impl RaftRPCClientImplTest {
    pub fn new(address: String) -> Self{
        RaftRPCClientImplTest {
            address,
            opt_channel: Mutex::new(None),
        }
    }

    async fn connect(endpoint: Endpoint) ->Result<Channel, Box<dyn std::error::Error>>  {
        let channel =endpoint.connect().await?;
        Ok(channel)
    }

    async fn getRPCChannel(&self) -> Channel {
        let local_opt_channel_res=self.opt_channel.lock();
        let mut local_opt_channel=local_opt_channel_res.unwrap();
        if local_opt_channel.is_none() {
            let endpoint= Endpoint::from_shared(self.address.clone()).unwrap()
                .timeout(Duration::from_secs(5))
                .concurrency_limit(2);
            //let rt = tokio::runtime::Runtime::new().unwrap();
            let channel_res=RaftRPCClientImplTest::connect(endpoint).await;
            if channel_res.is_ok() {
                let channel=channel_res.unwrap();
                let clone=channel.clone();
                local_opt_channel.replace(channel);
                return clone;
            } else {
                panic!("Errore di connessione")
            }
        } else {
            return local_opt_channel.as_ref().unwrap().clone();
        }

    }



    async fn send_request_vote_async(&self, request_vote_request: RequestVoteRequest) ->Result<RequestVoteResponse, Box<dyn std::error::Error>>  {
        let channel=self.getRPCChannel().await;
        let mut client = RaftRpcClient::new(channel);
        //let mut client = RaftRpcClient::connect(String::from(&self.address)).await?;

        let request = tonic::Request::new(RequestVoteRpcRequest {
            term: request_vote_request.term(),
            candidate_id: request_vote_request.candidate_id() as u32,
            last_log_index: request_vote_request.last_log_index(),
            last_log_term: request_vote_request.last_log_term(),
        });
        let response_rpc=client.request_vote_rpc(request).await?;
        let response=response_rpc.into_inner();
        Ok(RequestVoteResponse::new(response.term, response.vote_granted))
    }

    async fn send_append_entries_async(&self, append_entries_request: AppendEntriesRequest) ->Result<AppendEntriesResponse, Box<dyn std::error::Error>>  {
        println!("send_append_entries grpc client before connect at {}", Utc::now().timestamp_millis());
        let channel=self.getRPCChannel().await;
        let mut client = RaftRpcClient::new(channel);
        //let mut client = RaftRpcClient::connect(String::from(&self.address)).await?;
        println!("send_append_entries grpc client after connect at {}", Utc::now().timestamp_millis());
        let log_entries_rpc=append_entries_request.entries().iter().map(|entry| LogEntryRpc {
            index: entry.index(),
            term: entry.term(),
            command_type: match entry.state_machine_command().command_type() {
                CommandType::Put => {0}
                CommandType::Delete => {1}
            },
            /*
            TODO capire se si può evitare il clone e come fare con le stringhe
             */
            key: entry.state_machine_command().key().clone(),
            value: entry.state_machine_command().value().clone(),
        }).collect();
        let request = tonic::Request::new(AppendEntriesRpcRequest {
            term: append_entries_request.term(),
            leader_id: append_entries_request.leader_id() as u32,
            prev_log_index: append_entries_request.prev_log_index(),
            prev_log_term: append_entries_request.prev_log_term(),
            entries: log_entries_rpc,
            leader_commit_term: append_entries_request.leader_commit(),
        });
        println!("send_append_entries grpc client sending at {}", Utc::now().timestamp_millis());
        let response_rpc=client.append_entries_rpc(request).await?;
        let response=response_rpc.into_inner();
        Ok(AppendEntriesResponse::new(response.term, response.success))
    }
}

impl ClientChannel for RaftRPCClientImplTest {

    fn send_request_vote(&self, request_vote_request: RequestVoteRequest) -> Result<RequestVoteResponse,()> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let response_result=rt.block_on(self.send_request_vote_async(request_vote_request));
        //questo potrebbe non rispondere, gestire con un result
        return if response_result.is_ok() {
            Ok(response_result.ok().unwrap())
        } else {
            println!("ko response: {:?}", response_result.err().unwrap().deref());
            Err(())
        }
    }

    fn send_append_entries(&self, append_entries_request: AppendEntriesRequest) -> Result<AppendEntriesResponse, ()> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        println!("send_append_entries grpc sending at {}", Utc::now().timestamp_millis());
        let response_result=rt.block_on(
            self.send_append_entries_async(append_entries_request));
        //questo potrebbe non rispondere, gestire con un result
        println!("send_append_entries grpc response at {}", Utc::now().timestamp_millis());
        return if response_result.is_ok() {
            Ok(response_result.ok().unwrap())
        } else {
            println!("ko response: {:?}", response_result.err().unwrap().deref());
            Err(())
        }
    }
}

pub struct RaftRpcNetworkChannelTest {
}


impl NetworkChannel for RaftRpcNetworkChannelTest {
    type Client = RaftRPCClientImplTest;

    fn client_channel(&self, remote_address: String) -> Self::Client {
        RaftRPCClientImplTest::new(remote_address)
    }
}

#[test]
fn testOneServer() {
    let server_config_1=ServerConfig::new(1,65,100, 9090,vec![String::from("http://localhost:9091"),String::from("http://localhost:9092")]);
    thread::spawn(|| {
        RaftRPCServerImplTest::start(server_config_1);
    });
    let sleep_time = time::Duration::from_millis(2000);
    thread::sleep(sleep_time);
    let network_channel=RaftRpcNetworkChannelTest {};
    println!("Before SendRequest_vote");
    let client_channel=network_channel.client_channel(String::from("http://localhost:9090"));
    let request_vote_request=RequestVoteRequest::new(1,1,1,1);
    let request_vote_response=client_channel.send_request_vote(request_vote_request);
    if request_vote_response.is_ok() {
        println!("Request vote ok");
    }
    println!("Before Append Entries");
    let append_entries_request=AppendEntriesRequest::new(1,1,1,1,vec![],1);
    let append_entries_response= client_channel.send_append_entries(append_entries_request);
    if append_entries_response.is_ok() {
        println!("Append entries ok");
    }
    let append_entries_request2=AppendEntriesRequest::new(1,1,1,1,vec![],1);
    let append_entries_response2= client_channel.send_append_entries(append_entries_request2);
    if append_entries_response2.is_ok() {
        println!("Append entries ok2");
    }

}

