use tonic::{transport::Server, Request, Response, Status};
use futures::{join, future};
use raft_rpc::raft_rpc_client::{RaftRpcClient};
use raft_rpc::raft_rpc_server::{RaftRpc,RaftRpcServer};
use raft_rpc::{RequestVoteRpcReply, RequestVoteRpcRequest};
use raft::network::{ServerChannel, ClientChannel, NetworkChannel};
use raft::common::{RequestVoteRequest, RequestVoteResponse, CandidateIdType};
use raft::{RaftServer, ServerConfig};
use tokio::runtime::Runtime;

pub mod raft_rpc {
    tonic::include_proto!("raft_rpc"); // The string specified here must match the proto package name
}

pub struct RaftRPCServerImpl {
    raft_server: RaftServer<RaftRPCClientImpl>,
}

impl RaftRPCServerImpl {

    pub fn new (server_config: ServerConfig) -> RaftRPCServerImpl {
        RaftRPCServerImpl {
            raft_server: RaftServer::new(server_config, RaftRpcNetworkChannel {}),
        }
    }

    pub fn start(server_config: ServerConfig) {
        let addr_str=format!("127.0.0.1:{}",server_config.server_port());
        println!("addr_str={}",addr_str);
        let addr = addr_str.parse().unwrap();
        let raft_rpc_server_impl = RaftRPCServerImpl::new(server_config);
        //è da mettere qui perchè questa RaftRpcServer::new(raft_rpc_server_impl) fa il borrow
        raft_rpc_server_impl.raft_server.start();
        println!(" after raft_server start");
        let mut rt = Runtime::new().expect("failed to obtain a new RunTime object");
        let server_future = Server::builder()
            .add_service(RaftRpcServer::new(raft_rpc_server_impl))
            .serve(addr);
        rt.block_on(server_future).expect("failed to successfully run the future on RunTime");
        println!(" server with addr={} started",addr_str);

    }
}

#[tonic::async_trait]
impl RaftRpc for RaftRPCServerImpl {
    async fn request_vote_rpc(
        &self,
        request: Request<RequestVoteRpcRequest>,
    ) -> Result<Response<RequestVoteRpcReply>, Status> {
        println!("Got a request: {:?}", request);

        let request_obj=request.into_inner();
        let request_in=RequestVoteRequest::new(
            request_obj.term,
            request_obj.candidate_id as CandidateIdType,
            request_obj.last_log_index,
            request_obj.last_log_term,
        );
        let response=self.raft_server.on_request_vote(request_in);
        let reply = raft_rpc::RequestVoteRpcReply {
            term: response.term(),
            vote_granted: response.vote_granted(),
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
}

impl ClientChannel for RaftRPCClientImpl {

    fn send_request_vote(&self, request_vote_request: RequestVoteRequest) -> RequestVoteResponse {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let response_result=rt.block_on(self.send_request_vote_async(request_vote_request));
        //questo potrebbe non rispondere, gestire con un result
        response_result.ok().unwrap()
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

