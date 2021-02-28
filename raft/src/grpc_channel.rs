use tonic::{transport::Server, Request, Response, Status};
use futures::{join, future};
use raft_rpc::raft_rpc_client::{RaftRpcClient};
use raft_rpc::raft_rpc_server::{RaftRpc,RaftRpcServer};

use raft_rpc::{RequestVoteRpcReply, RequestVoteRpcRequest,AppendEntriesRpcRequest,AppendEntriesRpcReply};
use raft_rpc::append_entries_rpc_request::{LogEntryRpc};
use raft::network::{ServerChannel, ClientChannel, NetworkChannel};
use raft::common::{RequestVoteRequest, RequestVoteResponse, CandidateIdType, AppendEntriesResponse, AppendEntriesRequest, CommandType};
use raft::{RaftServer, ServerConfig};
use tokio::runtime::Runtime;
use std::sync::Arc;
use std::thread;
use std::ops::Deref;

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
            raft_server_server_state.manage_server_state();
        });
        println!(" after raft_server start");
        let mut rt = Runtime::new().expect("failed to obtain a new RunTime object");
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
        let request_obj=request.into_inner();
        let request_in=AppendEntriesRequest::new(
            request_obj.term,
            request_obj.leader_id as CandidateIdType,
            request_obj.prev_log_index,
            request_obj.prev_log_term,
            vec![],
            request_obj.leader_commit_term,
        );
        let response=self.raft_server.on_append_entries(request_in);
        let reply = raft_rpc::AppendEntriesRpcReply {
            term: 1,
            success: true,
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
}


impl RaftRPCClientImpl {
    pub fn new(address: String) -> RaftRPCClientImpl{
        RaftRPCClientImpl {
            address
        }
    }

    async fn send_request_vote_async(&self, request_vote_request: RequestVoteRequest) ->Result<RequestVoteResponse, Box<dyn std::error::Error>>  {
        let mut client = RaftRpcClient::connect(String::from(&self.address)).await?;

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
        let mut client = RaftRpcClient::connect(String::from(&self.address)).await?;

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
        let response_rpc=client.append_entries_rpc(request).await?;
        let response=response_rpc.into_inner();
        Ok(AppendEntriesResponse::new(response.term, response.success))
    }
}

impl ClientChannel for RaftRPCClientImpl {

    fn send_request_vote(&self, request_vote_request: RequestVoteRequest) -> Result<RequestVoteResponse,()> {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
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
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let response_result=rt.block_on(
            self.send_append_entries_async(append_entries_request));
        //questo potrebbe non rispondere, gestire con un result
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

