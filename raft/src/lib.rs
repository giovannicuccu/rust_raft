pub mod network;
pub mod common;
use crate::ServerState::Follower;
use crate::common::*;
use crate::network::*;
use std::time::Instant;
use rand::prelude::*;
use std::ops::Range;
use std::borrow::Borrow;


const NO_TERM : TermType = 0;
const NO_CANDIDATE_ID : CandidateIdType = 0;
const NO_VALUE : IndexType = 0;

enum CommandType {
    Put,
    Delete,
}

struct StateMachineCommand {
    command_type: CommandType,
    key: String,
    value: String,
}

struct LogEntry {
    index: IndexType,
    term: TermType,
    state_machine_command: StateMachineCommand,
}

/*
Raft non specifica completamente il contenuto di LogEntry,
Si limita a fare riferimento alla State Machine
In questa implementazione si realizza un state machine con comandi
Put e Delete con variabili di tipo Stringa sia per i nomi degli stati che per i valori
 */

struct ServerPersistentState {
    current_term:TermType,
    voted_for:CandidateIdType,
    log:Vec<LogEntry>,

}

struct ServerVolatileState {
    commit_index: IndexType,
    last_applied: IndexType,
    last_heartbeat_time: Instant,
}

struct ServerConfig {
    //usare un range
    id: CandidateIdType,
    election_timeout_min: u32,
    election_timeout_max: u32,
}

impl ServerConfig {
    pub fn id(&self)-> CandidateIdType {
        self.id
    }
}

/*
Leader State specificato nel protocollo si applica solo se il server è di tipo Leader
Rust consente di specificare degli enum con valori diversi per cui uso questa potenzialità
 */
#[derive(PartialEq, PartialOrd)]
enum ServerState {
    Leader {
        next_index:Vec<IndexType>,
        match_index:Vec<IndexType>,
    },
    Follower,
    Candidate,
}

struct RaftServer<S: ServerChannel, C:ClientChannel> {
    persistent_state: ServerPersistentState,
    volatile_state: ServerVolatileState,
    server_state: ServerState,
    config: ServerConfig,
    other_servers_in_cluster: Vec<String>,
    clients_for_servers_in_cluster: Vec<C>,
    server_channel: S,
}

fn discover_other_nodes_in_cluster() -> Vec<String> {
    Vec::new()
}


impl <S: ServerChannel, C:ClientChannel>RaftServer<S,C> {

    pub fn new<N: NetworkChannel<S,C>>(server_config: ServerConfig, network_channel:N) -> RaftServer<S,C> {
        let other_nodes_in_cluster=discover_other_nodes_in_cluster();
        let server_channel=network_channel.server_channel();
        let clients=other_nodes_in_cluster.iter().map(|address|network_channel.client_channel(address)).collect();
        let raft_server= RaftServer {
            persistent_state: ServerPersistentState {
                current_term:NO_TERM,
                voted_for:NO_CANDIDATE_ID,
                log: Vec::new(),
            },
            volatile_state: ServerVolatileState {
                commit_index:NO_VALUE,
                last_applied:NO_VALUE,
                last_heartbeat_time: Instant::now(),
            },
            server_state: Follower,
            config: server_config,
            /*
            Perchè la cosa abbia un senso devo fare discover dei server del cluster
            all'avvio del server -> uso un metodo per fare discovery dei nodi
             */
            other_servers_in_cluster: other_nodes_in_cluster,

            clients_for_servers_in_cluster: clients,
            server_channel,
        };
        raft_server.install_handlers();
        raft_server
    }

    fn install_handlers(&self) {
        let callback = |request_vote_request| self.on_request_vote(request_vote_request);
        self.server_channel.handle_request_vote(callback);
    }

    fn on_request_vote(&self,request_vote_request: RequestVoteRequest) -> RequestVoteResponse {
        if request_vote_request.term()<self.persistent_state.current_term {
            return RequestVoteResponse::new(NO_TERM,false);
        }
        if (self.persistent_state.voted_for==NO_CANDIDATE_ID || self.persistent_state.voted_for==request_vote_request.candidate_id()) &&
            //verificare se è meglio ottenere il valore di last con getOrElse o qualcosa del genere
            self.persistent_state.log.last().is_some() && self.persistent_state.log.last().unwrap().index<=request_vote_request.last_log_index() {
            return RequestVoteResponse::new(request_vote_request.term(),true);
        }
        RequestVoteResponse::new(0,false)
    }

    pub fn start(&self) {
        //Questo va fatto in un thread a parte perchè deve essere in esecuzione sempre
        match self.server_state {
            ServerState::Follower => {
                let now = Instant::now();
                let mut rng = thread_rng();
                let election_timeout: u32 = rng.gen_range(self.config.election_timeout_min..=self.config.election_timeout_max);
                if now.duration_since( self.volatile_state.last_heartbeat_time).as_millis() >= election_timeout as u128 {
                //il timeout è random fra due range da definire--
                //Avviare la richiesta di voto
                }
            }
            _ => { ()}
        }
    }

    fn send_requests_vote(&self) {
        let last_log_index=self.persistent_state.log.last().unwrap().index;
        let last_log_term=self.persistent_state.log.last().unwrap().term;
        for client_channel in self.clients_for_servers_in_cluster.iter() {
            let request_vote_response=client_channel.send_request_vote(
            RequestVoteRequest::new(self.persistent_state.current_term, self.config.id(),last_log_index, last_log_term));
            println!("vote response {}",request_vote_response.vote_granted())
        }
    }
}

