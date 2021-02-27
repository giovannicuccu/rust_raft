pub mod network;
pub mod common;
use crate::ServerState::Follower;
use crate::common::*;
use crate::network::*;
use std::time::Instant;
use rand::prelude::*;
use std::ops::Range;
use std::borrow::Borrow;
use std::{thread, time};


const NO_INDEX : IndexType = 0;
const NO_TERM : TermType = 0;
const NO_CANDIDATE_ID : CandidateIdType = 0;
const NO_VALUE : IndexType = 0;

enum CommandType {
    Put,
    Delete,
}

struct StateMachineCommand {
    command_type: CommandType,
    //key: String,
    //value: String,
}

struct LogEntry {
    index: IndexType,
    term: TermType,
    state_machine_command: StateMachineCommand,
}

//const NULL_STR:String=String::from("");
/*
TODO: rivedere questo, forse non serve
 */
const NO_LOG_ENTRY:LogEntry = LogEntry {
    index: NO_INDEX,
    term: NO_TERM,
    state_machine_command: StateMachineCommand {
        command_type: CommandType::Put,
        //key: NULL_STR,
        //value: NULL_STR
    }
};

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

pub struct ServerConfig {
    //usare un range
    id: CandidateIdType,
    election_timeout_min: u32,
    election_timeout_max: u32,
    server_port:u16,
    other_nodes_in_cluster: Vec<String>,
}

impl ServerConfig {
    pub fn new(id: CandidateIdType,  election_timeout_min: u32, election_timeout_max: u32,
               server_port:u16, other_nodes_in_cluster: Vec<String>) -> ServerConfig {
        ServerConfig {
            id,
            election_timeout_min,
            election_timeout_max,
            server_port,
            other_nodes_in_cluster
        }
    }
    pub fn id(&self)-> CandidateIdType {
        self.id
    }
    pub fn server_port(&self) -> u16 {
        self.server_port
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

pub struct RaftServer<C:ClientChannel> {
    persistent_state: ServerPersistentState,
    volatile_state: ServerVolatileState,
    server_state: ServerState,
    config: ServerConfig,
    clients_for_servers_in_cluster: Vec<C>,
    //server_channel: S,
}

fn discover_other_nodes_in_cluster() -> Vec<String> {
    Vec::new()
}


impl <C:ClientChannel>RaftServer<C> {

    pub fn new<N: NetworkChannel<Client=C>>(server_config: ServerConfig, network_channel:N) -> RaftServer<C> {
        //let server_channel=network_channel.server_channel();
        let clients=server_config.other_nodes_in_cluster.iter().map(|address|network_channel.client_channel(String::from(address))).collect();
        RaftServer {
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

            clients_for_servers_in_cluster: clients,
        }

    }

    pub fn on_request_vote(&self,request_vote_request: RequestVoteRequest) -> RequestVoteResponse {
        if request_vote_request.term()<self.persistent_state.current_term {
            return RequestVoteResponse::new(NO_TERM,false);
        }
        if (self.persistent_state.voted_for==NO_CANDIDATE_ID || self.persistent_state.voted_for==request_vote_request.candidate_id()) &&
            //verificare se è meglio ottenere il valore di last con getOrElse o qualcosa del genere
            self.persistent_state.log.last().is_some() && self.persistent_state.log.last().unwrap().index<=request_vote_request.last_log_index() {
            return RequestVoteResponse::new(request_vote_request.term(),true);
        }
        RequestVoteResponse::new(NO_TERM,false)
    }

    pub fn start(&self) {
        //Questo va fatto in un thread a parte perchè deve essere in esecuzione sempre
        match self.server_state {
            ServerState::Follower => {
                println!(" start ServerState::Follower");
                let now = Instant::now();
                let mut rng = thread_rng();
                let election_timeout: u32 = rng.gen_range(self.config.election_timeout_min..=self.config.election_timeout_max);
                if now.duration_since( self.volatile_state.last_heartbeat_time).as_millis() >= election_timeout as u128 {
                //il timeout è random fra due range da definire--
                //Avviare la richiesta di voto
                    self.send_requests_vote();
                } else {
                    println!(" start ServerState::Follower before sleep");
                    thread::sleep(time::Duration::from_millis(self.config.election_timeout_max as u64) );
                    println!(" start ServerState::Follower after sleep");
                    self.start();
                }
            }
            _ => { ()}
        }
    }

    fn send_requests_vote(&self) {
        println!(" send_requests_vote start");
/*
TODO: capire come gestire lo start quando non ci sono entry e si deve chiedere una request vote
gestire forse non con NO_LOG_ENTRY ma con i singoli valori di default
 */
        let last_log_index=self.persistent_state.log.last().unwrap_or(&NO_LOG_ENTRY).index;
        let last_log_term=self.persistent_state.log.last().unwrap_or(&NO_LOG_ENTRY).term;
        for client_channel in self.clients_for_servers_in_cluster.iter() {
            println!(" before send_requests_vote rpc");
            let request_vote_response=client_channel.send_request_vote(
            RequestVoteRequest::new(self.persistent_state.current_term, self.config.id(),last_log_index, last_log_term));
            println!("vote response {}",request_vote_response.vote_granted())
        }
    }

    pub fn server_config(&self) -> &ServerConfig {
        &self.server_config()
    }
}

