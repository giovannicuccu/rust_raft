use crate::common::{RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, ApplyCommandRequest, ApplyCommandResponse};

/*
cerco qualcosa che sia chiaro e che leghi insieme i concetti di client e
server insieme a livello di programmazione
 */
/*
approfondire perchè server impl Fn()
 */
pub trait ServerChannel {

    fn handle_request_vote(&mut self, handler: impl Fn(RequestVoteRequest) -> RequestVoteResponse + Send + Sync);
}

pub trait ClientChannel {
    fn send_request_vote(&self, request_vote_request: RequestVoteRequest)-> Result<RequestVoteResponse,()>;
    fn send_append_entries(&self, append_entries_request: AppendEntriesRequest)-> Result<AppendEntriesResponse,()>;
}
/*
questo sotto non è proprio corretto capire cosa fa veramente
per capire meglio
https://www.reddit.com/r/rust/comments/dtt0oz/associated_types_vs_generic_types/
https://blog.thomasheartman.com/posts/on-generics-and-associated-types

provare anche a realizzare la parte con il trait generico
pub trait NetworkChannel<C:ClientChannel>{
    fn client_channel(&self,remote_address: String)->C;
}

e sotto grpc
impl NetworkChannel<RaftRPCClientImpl> for RaftRpcNetworkChannel {
}
 */


/*
pub trait NetworkChannel<C:ClientChannel>{
    fn client_channel(&self,remote_address: String)->C;
}
*/

pub trait NetworkChannel{
    type Client : ClientChannel;
    fn client_channel(&self,remote_address: String)->Self::Client;
}

pub trait RaftClient {
    fn apply_command(&self, append_entries_request: ApplyCommandRequest)-> Result<ApplyCommandResponse,()>;
}

