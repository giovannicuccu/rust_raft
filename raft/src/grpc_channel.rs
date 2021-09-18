use tonic::{transport::Server, Request, Response, Status, Code};
use raft_rpc::raft_rpc_client::{RaftRpcClient};
use raft_rpc::raft_rpc_server::{RaftRpc,RaftRpcServer};

use raft_rpc::{RequestVoteRpcReply, RequestVoteRpcRequest,AppendEntriesRpcRequest,AppendEntriesRpcReply,ApplyCommandRpcRequest,ApplyCommandRpcReply};
use raft_rpc::append_entries_rpc_request::{LogEntryRpc};
use raft_rpc::append_entries_rpc_request::log_entry_rpc::{Command};
use raft_rpc::{PutCommand,DeleteCommand};
use raft_rpc::apply_command_rpc_reply::{OkKo};
use raft::network::{ClientChannel, NetworkChannel};
use raft::common::{RequestVoteRequest, RequestVoteResponse, CandidateIdType, AppendEntriesResponse, AppendEntriesRequest, LogEntry, StateMachineCommand, ApplyCommandRequest, ApplyCommandStatus};
use raft::{RaftServer, ServerConfig};
use tokio::runtime::Runtime;
use std::sync::{Arc, Mutex};
use std::thread;
use std::ops::Deref;
use chrono::Utc;
use tonic::transport::{Endpoint, Channel};
use std::time::Duration;
use raft::common::ApplyCommandStatus::{Pending, Redirect};

pub mod raft_rpc {
    tonic::include_proto!("raft_rpc"); // The string specified here must match the proto package name
}

pub struct RaftRPCServerImpl {
    raft_server: Arc<RaftServer<RaftRPCClientImpl>>,
}

impl RaftRPCServerImpl {

    pub fn new (server_config: ServerConfig) -> RaftRPCServerImpl {
        RaftRPCServerImpl {
            raft_server: Arc::new(RaftServer::new(server_config, RaftRpcNetworkChannel {})),
        }
    }

    fn get_raft_server(&self) -> Arc<RaftServer<RaftRPCClientImpl>>{
        self.raft_server.clone()
    }

    pub fn start(server_config: ServerConfig) {
        let addr_str=format!("127.0.0.1:{}",server_config.server_port());
        println!("addr_str={}",addr_str);
        let addr = addr_str.parse().unwrap();
        let raft_rpc_server_impl = RaftRPCServerImpl::new(server_config);
        //è da mettere qui perchè questa RaftRpcServer::new(raft_rpc_server_impl) fa il borrow
        let raft_server_server_state= raft_rpc_server_impl.get_raft_server();
        thread::spawn(move || {
            loop {
                raft_server_server_state.manage_server_state();
            }
        });
        println!(" before raft_server start_senders");
        let raft_server_server_state= raft_rpc_server_impl.get_raft_server();
        raft_server_server_state.start_senders();
        println!(" after raft_server start_senders");
        let rt = Runtime::new().expect("failed to obtain a new RunTime object");
        let server_future = Server::builder()
            .add_service(RaftRpcServer::new(raft_rpc_server_impl))
            .serve(addr);
        rt.block_on(server_future).expect("failed to successfully run the future on RunTime");
        //Questo sembra che non sia mai eseguito ovvero che si blocchi prima
        //println!(" server with addr={} started",addr_str);

    }
}

#[tonic::async_trait]
impl RaftRpc for RaftRPCServerImpl {
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
        /*
        Questo continua a funzionare anche con Arc perchè evidentemente il compilatore è smart
         */
        let response=self.raft_server.on_request_vote(request_in);
        let reply = raft_rpc::RequestVoteRpcReply {
            term: response.term(),
            vote_granted: response.vote_granted(),
        };
        Ok(Response::new(reply))
    }

    async fn append_entries_rpc(
        &self,
        request: Request<AppendEntriesRpcRequest>,
    ) -> Result<Response<AppendEntriesRpcReply>, Status> {
        println!("async fn append_entries_rpc at {}",Utc::now().timestamp_millis());
        let request_obj=request.into_inner();
        let log_entries_res:Vec<Result<LogEntry,i32>>=request_obj.entries.iter().map(|entry| {

          let command_res=      match &entry.command {
              //capire meglio il giro di queste stringhe
                    Some(command) => {
                        match command {
                            Command::Put(putCommand) => { Ok(StateMachineCommand::Put {key:String::from(&putCommand.key), value:String::from(&putCommand.value)}) }
                            Command::Delete(deleteCommand) => { Ok(StateMachineCommand::Delete {key:String::from(&deleteCommand.key)}) }
                        }
                    }
                    None => {
                        println!("NONE in Command");
                        Err(-1)
                    }
                };
            //Ok(StateMachineCommand::Delete {key:String::from("delete")})
            match command_res {
                Ok(command) => {
                    Ok(LogEntry::new(
                        entry.term,
                        entry.index,
                        command)
                    )
                }
                Err(x)=> {
                    Err(x)
                }
            }
        }).collect();
        let err_opt=log_entries_res.iter().find(|enty_res| enty_res.is_err());
        if err_opt.is_some() {
            return Err(Status::new(Code::InvalidArgument,"invalid command"));
        }
        // qui serve into_iter perchè devo fare la move degli elementi del vettore
        let log_entries=log_entries_res.into_iter().map(|entry| entry.unwrap()).collect();
        let request_in=AppendEntriesRequest::new(
            request_obj.term,
            request_obj.leader_id as CandidateIdType,
            request_obj.prev_log_index,
            request_obj.prev_log_term,
            log_entries,
            request_obj.leader_commit_term,
        );
        let response=self.raft_server.on_append_entries(request_in);
        let reply = raft_rpc::AppendEntriesRpcReply {
            term: response.term(),
            success: response.success(),
        };
        Ok(Response::new(reply))
    }

    async fn apply_command_rpc(
        &self,
        request: Request<ApplyCommandRpcRequest>,
    ) -> Result<Response<ApplyCommandRpcReply>, Status> {
        let request_obj=request.into_inner();
        let state_machine_command=match request_obj.command.unwrap() {
            raft_rpc::apply_command_rpc_request::Command::Put(putCommand) => {
                StateMachineCommand::Put {
                    key:String::from(&putCommand.key), value:String::from(&putCommand.value)
                }
            }
            raft_rpc::apply_command_rpc_request::Command::Delete(deleteCommand) => {
                StateMachineCommand::Delete {
                    key:String::from(&deleteCommand.key)
                }
            }

        };
        let response=self.raft_server.on_apply_command(ApplyCommandRequest::new(state_machine_command));
        let response_status=match response.status() {
            ApplyCommandStatus::Ok => {
                Some(raft_rpc::apply_command_rpc_reply::Status::Okko(0))
            }
            ApplyCommandStatus::Ko=> {
                Some(raft_rpc::apply_command_rpc_reply::Status::Okko(1))
            }
            ApplyCommandStatus::Pending{token} => {
                Some(raft_rpc::apply_command_rpc_reply::Status::Pending(raft_rpc::apply_command_rpc_reply::PendingStatus {token: *token}))
            }
            ApplyCommandStatus::Redirect{leader} => {
                Some(raft_rpc::apply_command_rpc_reply::Status::Redirect(raft_rpc::apply_command_rpc_reply::RedirectStatus {leader: leader.to_string()}))
            }
        };
        let reply = raft_rpc::ApplyCommandRpcReply {
            status: response_status
        };
        Ok(Response::new(reply))
    }
}
/*
RaftRPC è un trait Send+Sync questo significa che
Send => può essere passato come valore (move in Rust) fra thread
Sync => può essere passato come non mut reference fra thread
Se è Sync anche chi implementa il trait deve essere Sync e quindi devono essere sync anche le property
della struct nel nostro caso handler che è un puntatore a funzione
 */


/*impl ServerChannel for RaftRPCServerImpl {
    fn handle_request_vote(&mut self, handler_fn: impl Fn(RequestVoteRequest) -> RequestVoteResponse + Send + Sync) {
        self.handler=handler_fn;
    }
}*/

pub struct RaftRPCClientImpl {
    address: String,
    opt_channel: Mutex<Option<Channel>>,
    opt_channel_req_vote: Mutex<Option<Channel>>,
}


impl RaftRPCClientImpl {
    pub fn new(address: String) -> RaftRPCClientImpl{
        RaftRPCClientImpl {
            address,
            opt_channel: Mutex::new(None),
            opt_channel_req_vote: Mutex::new(None),
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
            let channel_res=RaftRPCClientImpl::connect(endpoint).await;
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

    async fn getRPCChannelReqVote(&self) -> Channel {
        let local_opt_channel_res=self.opt_channel_req_vote.lock();
        let mut local_opt_channel=local_opt_channel_res.unwrap();
        if local_opt_channel.is_none() {
            let endpoint= Endpoint::from_shared(self.address.clone()).unwrap()
                .timeout(Duration::from_secs(5))
                .concurrency_limit(2);
            //let rt = tokio::runtime::Runtime::new().unwrap();
            let channel_res=RaftRPCClientImpl::connect(endpoint).await;
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
        //let mut client = RaftRpcClient::connect(String::from(&self.address)).await?;
        let channel=self.getRPCChannelReqVote().await;
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
            command: match entry.state_machine_command() {
                StateMachineCommand::Put {key,value} => {
                    Some(Command::Put(PutCommand {key:String::from(key), value:String::from(value)}))
                }
                StateMachineCommand::Delete {key} => {
                    Some(Command::Delete(DeleteCommand {key:String::from(key)}))
                }
            },
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

impl ClientChannel for RaftRPCClientImpl {

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

pub struct RaftRpcNetworkChannel {
}


/*
Questo sotto non è corretto
capire bene la differenza rispetto a quella corretta
 */



/*impl NetworkChannel<RaftRPCClientImpl> for RaftRpcNetworkChannel {
    fn client_channel(&self, remote_address: String) -> RaftRPCClientImpl {
        RaftRPCClientImpl::new(remote_address)
    }
}*/

impl NetworkChannel for RaftRpcNetworkChannel {
    type Client = RaftRPCClientImpl;

    fn client_channel(&self, remote_address: String) -> Self::Client {
        RaftRPCClientImpl::new(remote_address)
    }
}

