pub mod network;
pub mod common;
use crate::ServerState::{Follower, Candidate, Leader};
use crate::common::*;
use crate::network::*;
use std::time::Instant;
use rand::prelude::*;
use std::ops::{Range, Deref};
use std::borrow::Borrow;
use std::{thread, time};
use std::sync::{Arc, Mutex};


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
    voted_for:Mutex<CandidateIdType>,
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
    server_state: Mutex<ServerState>,
    config: ServerConfig,
    clients_for_servers_in_cluster: Vec<C>,
    //server_channel: S,
}

fn discover_other_nodes_in_cluster() -> Vec<String> {
    Vec::new()
}


impl <C:ClientChannel+Send+Sync >RaftServer<C> {

    pub fn new<N: NetworkChannel<Client=C>>(server_config: ServerConfig, network_channel:N) -> RaftServer<C> {
        //let server_channel=network_channel.server_channel();
        let clients=server_config.other_nodes_in_cluster.iter().map(|address|network_channel.client_channel(String::from(address))).collect();
        RaftServer {
            persistent_state: ServerPersistentState {
                current_term:NO_TERM,
                voted_for:Mutex::new(NO_CANDIDATE_ID),
                log: Vec::new(),
            },
            volatile_state: ServerVolatileState {
                commit_index:NO_VALUE,
                last_applied:NO_VALUE,
                last_heartbeat_time: Instant::now(),
            },
            server_state: Mutex::new(Follower),
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
            println!("on_request_vote granted ko minor term id={}",self.config.id);
            return RequestVoteResponse::new(NO_TERM,false);
        }
        let mut last_index=NO_INDEX;
        if self.persistent_state.log.last().is_some() {
            let last_index=self.persistent_state.log.last().unwrap().index;
        }

        let mut voted_for_guard=self.persistent_state.voted_for.lock().unwrap();
        println!("on_request_vote voted_for={}, candidate={} term id={}",*voted_for_guard,request_vote_request.candidate_id(),self.config.id);
        if (*voted_for_guard==NO_CANDIDATE_ID || *voted_for_guard==request_vote_request.candidate_id()) &&
            //verificare se è meglio ottenere il valore di last con getOrElse o qualcosa del genere
            last_index<=request_vote_request.last_log_index() {
            *voted_for_guard= request_vote_request.candidate_id() as u16;
            println!("on_request_vote granted ok to {} id={}",*voted_for_guard, self.config.id);
            return RequestVoteResponse::new(request_vote_request.term(),true);
        }
        println!("on_request_vote granted ko id={}",self.config.id);
        RequestVoteResponse::new(NO_TERM,false)
    }

    pub fn manage_server_state(&self) {
        /*
        Nel nuovo desing questo gestisce le richieste di cambio stato da follower a candidate,etc
         */

        //Questo va fatto in un thread a parte perchè deve essere in esecuzione sempre
        /*
        una possibile soluzione è qui
        https://users.rust-lang.org/t/how-to-use-self-while-spawning-a-thread-from-method/8282/3
        ragionare su cosa fare

         */

        //let thread_server_state=self.server_state.clone();
        //thread::spawn(move || {
        let mut count = 0u32;
        loop {
            count+=1;
            let mut mutex_guard = self.server_state.lock().unwrap();
            //let mut inner_server_state=self.server_state.lock().unwrap();
            //let server_state: &ServerState = match mutex_guard {
            match *mutex_guard {
                ServerState::Follower => {
                    println!(" start ServerState::Follower id={}",self.config.id);
                    let now = Instant::now();
                    let mut rng = thread_rng();
                    let election_timeout: u32 = rng.gen_range(self.config.election_timeout_min..=self.config.election_timeout_max);
                    if now.duration_since(self.volatile_state.last_heartbeat_time).as_millis() >= election_timeout as u128 {
                        //il timeout è random fra due range da definire--
                        //Avviare la richiesta di voto
                        *mutex_guard = Candidate;
                    } else {
                        //println!(" start ServerState::Follower before sleep id={}",self.config.id);
                        thread::sleep(time::Duration::from_millis(self.config.election_timeout_max as u64));
                        //println!(" start ServerState::Follower after sleep id={}",self.config.id);
                        //self.start();
                    }
                }
                ServerState::Candidate =>{
                    println!("start ServerState::Candidate id={}",self.config.id);
                    if self.send_requests_vote() {
                        *mutex_guard = Leader {
                            next_index: vec![],
                            match_index: vec![]
                        };
                    }
                }
                ServerState::Leader =>{
                    println!("start ServerState::Leader id={}",self.config.id);
                }
                _ => { () }
            }
            println!("start count={} id={}",count, self.config.id);
            if count==20 {
                break;
            }
            //});
        }
    }

    fn send_requests_vote(&self)-> bool {
        //println!(" send_requests_vote start");
/*
TODO: capire come gestire lo start quando non ci sono entry e si deve chiedere una request vote
gestire forse non con NO_LOG_ENTRY ma con i singoli valori di default
 */
        let mut last_log_index=NO_INDEX;
        let mut last_log_term = NO_TERM;
        let last_log=self.persistent_state.log.last();
        if last_log.is_some() {
            let last_log_entry=self.persistent_state.log.last().unwrap();
            last_log_index=last_log_entry.index;
            last_log_term=last_log_entry.term;

        }
        let mut ok_votes=0u16;
        for client_channel in self.clients_for_servers_in_cluster.iter() {
            //println!(" before send_requests_vote rpc id={}",self.config.id);
            let request_vote_response=client_channel.send_request_vote(
            RequestVoteRequest::new(self.persistent_state.current_term, self.config.id(),last_log_index, last_log_term));
           if (request_vote_response.is_ok()) {

               if request_vote_response.ok().unwrap().vote_granted() {
                   println!("send_requests_vote vote response granted for id={}", self.config.id);
                   ok_votes+=1;
               }
               println!("send_requests_vote vote response NOT granted for id={}", self.config.id);
           } else {
               println!("send_requests_vote vote response ko id={}",self.config.id);
           }
        }
        println!("send_requests_vote result={} id={}",ok_votes>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as u16,self.config.id);
        ok_votes>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as u16
    }

}

