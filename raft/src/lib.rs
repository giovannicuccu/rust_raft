pub mod network;
pub mod common;
pub mod log;
mod state_machine;

use crate::ServerState::{Follower, Candidate, Leader};
use crate::common::*;
use crate::network::*;
use std::time::Instant;
use rand::prelude::*;
use std::{thread, time};
use std::sync::{Mutex, Arc};
use chrono::Utc;
use rayon::prelude::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::path::{PathBuf, Path};
use crate::log::WriteAheadLog;
use walkdir::WalkDir;


const NO_INDEX : IndexType = 0;
const NO_TERM : TermType = 0;
const NO_CANDIDATE_ID : CandidateIdType = 0;
const NO_VALUE : IndexType = 0;




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
    wal_dir:String,
}

impl ServerConfig {
    pub fn new(id: CandidateIdType,  election_timeout_min: u32, election_timeout_max: u32,
               server_port:u16, other_nodes_in_cluster: Vec<String>,wal_dir:String) -> ServerConfig {
        ServerConfig {
            id,
            election_timeout_min,
            election_timeout_max,
            server_port,
            other_nodes_in_cluster,
            wal_dir
        }
    }
    pub fn id(&self)-> CandidateIdType {
        self.id
    }
    pub fn server_port(&self) -> u16 {
        self.server_port
    }

    pub fn other_nodes_in_cluster(&self) -> &Vec<String> {
        &self.other_nodes_in_cluster
    }
}

/*
Leader State specificato nel protocollo si applica solo se il server è di tipo Leader
Rust consente di specificare degli enum con valori diversi per cui uso questa potenzialità
 */
#[derive(PartialEq, PartialOrd,Clone)]
enum ServerState {
    Leader {
        next_index:Vec<IndexType>,
        match_index:Vec<IndexType>,
    },
    Follower,
    Candidate,
}

//Capire perchè servono sia partialEq che Eq e Eq non basta
#[derive(Eq, PartialEq)]
pub enum RaftServerState {
    Leader,
    Follower,
    Candidate,
}

pub struct RaftServer<C:ClientChannel> {
    persistent_state: ServerPersistentState,
    volatile_state: Mutex<ServerVolatileState>,
    server_state: Mutex<ServerState>,
    config: ServerConfig,
    clients_for_servers_in_cluster: Vec<C>,
    wal: Arc<Mutex<WriteAheadLog>>,
    //server_channel: S,
}


//codice per trovare i file in una dir
//devo cercare i .wal e avere il più recente
//apro quello

/*
use walkdir::WalkDir;

fn main() -> Result<()> {
    for entry in WalkDir::new(".")
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok()) {
        let f_name = entry.file_name().to_string_lossy();
        let sec = entry.metadata()?.modified()?;

        if f_name.ends_with(".json") && sec.elapsed()?.as_secs() < 86400 {
            println!("{}", f_name);
        }
    }

    Ok(())
}
 */

impl <C:ClientChannel+Send+Sync >RaftServer<C> {

    pub fn new<N: NetworkChannel<Client=C>>(server_config: ServerConfig, network_channel:N) -> RaftServer<C> {
        //let server_channel=network_channel.server_channel();
        /*
        Capire perchè dopo che ho messo clients_len si incammella con il tipo
         */
        let clients:Vec<C>=server_config.other_nodes_in_cluster.iter().map(|address|network_channel.client_channel(String::from(address))).collect();
        let file_dir = server_config.wal_dir.as_str();
        let mut last_modified=0;
        let mut file_name:Option<String>=None;
        println!("file_dir {}",file_dir);
        for entry in WalkDir::new(file_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok()) {
            if entry.path().is_file() {
                let f_name = entry.file_name().to_string_lossy();
                println!("found file {}", f_name.parse::<String>().unwrap());
                let sec = entry.metadata().unwrap().modified().unwrap();
                if last_modified == 0 {
                    last_modified = sec.elapsed().unwrap().as_secs() + 1;
                    println!("last_modified {}", last_modified);
                }
                if f_name.ends_with(".wal") && sec.elapsed().unwrap().as_secs() < last_modified {
                    file_name = Some(Path::new(file_dir).join(f_name.parse::<String>().unwrap()).to_string_lossy().parse::<String>().unwrap());
                    println!("found wal {}", f_name.parse::<String>().unwrap());
                }
            }
        }
        let wal=match file_name {
            None => {println!("found wal dir {}",file_dir); WriteAheadLog::new(file_dir).unwrap()},
            Some(file_name) => {println!("found wal file {}",file_name);WriteAheadLog::from_path(&*file_name).unwrap()},
        };
        let wal_mutex=Arc::new(Mutex::new(wal));
        RaftServer {
            persistent_state: ServerPersistentState {
                current_term:NO_TERM,
                voted_for:Mutex::new(NO_CANDIDATE_ID),
                log: Vec::new(),
            },
            volatile_state: Mutex::new(ServerVolatileState {
                commit_index:NO_VALUE,
                last_applied:NO_VALUE,
                last_heartbeat_time: Instant::now(),
            }),
            server_state: Mutex::new(Follower),
            config: server_config,
            /*
            Perchè la cosa abbia un senso devo fare discover dei server del cluster
            all'avvio del server -> uso un metodo per fare discovery dei nodi
             */

            clients_for_servers_in_cluster: clients,
            wal: wal_mutex,
        }

    }

    pub fn on_request_vote(&self,request_vote_request: RequestVoteRequest) -> RequestVoteResponse {
        if request_vote_request.term()<self.persistent_state.current_term {
            println!("id:{} - on_request_vote granted ko minor term",self.config.id);
            return RequestVoteResponse::new(NO_TERM,false);
        }
        let mut last_index=NO_INDEX;
        if self.persistent_state.log.last().is_some() {
            last_index=self.persistent_state.log.last().unwrap().index();
        }

        let mut voted_for_guard=self.persistent_state.voted_for.lock().unwrap();
        //println!("id:{} - on_request_vote voted_for={}, candidate={} term",*voted_for_guard,request_vote_request.candidate_id(),self.config.id);
        if (*voted_for_guard==NO_CANDIDATE_ID || *voted_for_guard==request_vote_request.candidate_id()) &&
            //verificare se è meglio ottenere il valore di last con getOrElse o qualcosa del genere
            last_index<=request_vote_request.last_log_index() {
            *voted_for_guard= request_vote_request.candidate_id() as u16;
            println!("id:{} - on_request_vote granted ok to {}",self.config.id,*voted_for_guard);
            return RequestVoteResponse::new(request_vote_request.term(),true);
        }
        println!("id:{} - on_request_vote granted ko to {}",self.config.id,request_vote_request.candidate_id());
        RequestVoteResponse::new(NO_TERM,false)
    }

    pub fn on_append_entries(&self,append_entries_request: AppendEntriesRequest) -> AppendEntriesResponse {
        println!("id:{} - on_append_entries begin",self.config.id);
        let mut mutex_guard = self.server_state.lock().unwrap();
        println!("id:{} - on_append_entries got mutex at {}",self.config.id,Utc::now().timestamp_millis());
        match *mutex_guard {
            ServerState::Leader { .. } => {}
            Follower => {
                println!("id:{} - on_append_entries ServerState::Follower",self.config.id);
                let mut mutex_volatile_state_guard = self.volatile_state.lock().unwrap();
                mutex_volatile_state_guard.last_heartbeat_time=Instant::now();
            }
            Candidate => {
                println!("id:{} - on_append_entries ServerState::Candidate will become Follower",self.config.id);
                *mutex_guard = Follower;
                let mut mutex_volatile_state_guard = self.volatile_state.lock().unwrap();
                mutex_volatile_state_guard.last_heartbeat_time=Instant::now();
                let now = Utc::now();
                println!("id:{} - on_append_entries ServerState now is Follower={} now ={}",self.config.id,*mutex_guard==Follower, now.timestamp_millis());
            }
        }
        AppendEntriesResponse::new(self.persistent_state.current_term,true)
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
        println!("id:{} - manage_server_state start at {}",self.config.id, Utc::now().timestamp_millis());
        let mut count = 0u32;
        let mut rng = thread_rng();
        //loop {
        //    count+=1;

            /*
            Qui ci va &*mutex_guard perchè volgio un riferimento a ServerState
            NON E' VERO
            https://blog.rust-lang.org/2015/04/17/Enums-match-mutation-and-moves.html
            leggere laparte sul ref binding
            Se non è un riferimento non riesco a gestire il caso di Leader che fa borrow dei due campi
            Indagare meglio per capire
            Se non lo metto cosa succede?
            con *mutex_guard accedo alla variabile server state e faccio il borrow
            con &*mutex_guard accedo al riferimento alla variabile server state e NON faccio il borrow
            Perchè se faccio il borrow funziona con i valori semplici dell'enum?
            https://blog.rust-lang.org/2015/04/17/Enums-match-mutation-and-moves.html
            dice che
            In fact, in Rust, match is designed to work quite well without taking ownership
            In particular, the input to match is an L-value expression;
            this means that the input expression is evaluated to a memory location where the value lives.
            match works by doing this evaluation and then inspecting the data at that memory location.
             */
            let mutex_guard = self.server_state.lock().unwrap();
            let server_state_cloned=mutex_guard.clone();
            let now = Instant::now();
            drop(mutex_guard);
            match server_state_cloned {
                ServerState::Follower => {
                    let mutex_volatile_state_guard = self.volatile_state.lock().unwrap();
                    println!("id:{} - start ServerState::Follower last heartbit  {} ms ago",self.config.id,now.duration_since(mutex_volatile_state_guard.last_heartbeat_time).as_millis());
                    let election_timeout: u32 = rng.gen_range(self.config.election_timeout_min..=self.config.election_timeout_max);
                    if now.duration_since(mutex_volatile_state_guard.last_heartbeat_time).as_millis() >= election_timeout as u128 {
                        //il timeout è random fra due range da definire--
                        //Avviare la richiesta di voto
                        let mut mutex_guard = self.server_state.lock().unwrap();
                        *mutex_guard = Candidate;
                        let now_utc = Utc::now();
                        println!("id:{} - start ServerState::Follower becoming candidate at {}",self.config.id, now_utc.timestamp_millis());
                    } else {
                        thread::sleep(time::Duration::from_millis(election_timeout as u64));
                    }
                }
                ServerState::Candidate =>{
                    println!("id:{} - start ServerState::Candidate",self.config.id);
                    if self.send_requests_vote() {
                        let mut mutex_guard = self.server_state.lock().unwrap();
                        if *mutex_guard==Candidate {
                            *mutex_guard = Leader {
                                next_index: vec![],
                                match_index: vec![]
                            };
                            let mut voted_for_guard = self.persistent_state.voted_for.lock().unwrap();
                            *voted_for_guard = self.config.id;
                        }
                    } else {
                        let mutex_guard = self.server_state.lock().unwrap();
                        if *mutex_guard==Candidate {
                            let election_timeout: u32 = rng.gen_range(self.config.election_timeout_min..=self.config.election_timeout_max);
                            println!("id:{} - start ServerState::Candidate will sleep for  {} ms ", self.config.id, election_timeout);
                            thread::sleep(time::Duration::from_millis(election_timeout as u64));
                        }
                    }
                }
                //uso ref per non prendere la ownership
                ServerState::Leader { ref next_index, ref match_index} =>{
                    println!("id:{} - start ServerState::Leader at {}",self.config.id, Utc::now().timestamp_millis());
                    if self.send_append_entries() {

                    }
                    thread::sleep(time::Duration::from_millis(10 as u64));
                }
            }


            //println!("id:{} - start count={}",count, self.config.id);
 /*           if count==8 {
                break;
            }*/
            //});
        //}
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
            last_log_index=last_log_entry.index();
            last_log_term=last_log_entry.term();

        }
        let ok_votes=AtomicIsize::new(0);

        self.clients_for_servers_in_cluster.par_iter().for_each(|client_channel| {
           //println!(" before send_requests_vote rpc id={}",self.config.id);
           let request_vote_response=client_channel.send_request_vote(
            RequestVoteRequest::new(self.persistent_state.current_term, self.config.id(),last_log_index, last_log_term));
           if request_vote_response.is_ok() {

               if request_vote_response.ok().unwrap().vote_granted() {
                   //println!("id:{} - send_requests_vote vote response granted", self.config.id);
                   ok_votes.fetch_add(1,Ordering::SeqCst);
               }
               //println!("id:{} - send_requests_vote vote response NOT granted", self.config.id);
           } else {
               //println!("id:{} - send_requests_vote vote response ko",self.config.id);
           }
        });
        println!("id:{} - send_requests_vote result={} ok_votes={}",self.config.id, ok_votes.load(Ordering::SeqCst)>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as isize, ok_votes.load(Ordering::SeqCst));
        ok_votes.load(Ordering::SeqCst)>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as isize
    }

    fn send_append_entries(&self)-> bool {
        let now = Utc::now();
        println!("id:{} - send_append_entries at {}",self.config.id, now.timestamp_millis());
        let ok_votes=AtomicIsize::new(0);
        /*
        Spiegare bene par_iter come trait aggiuntivo ad un tipo esistente

         */
        self.clients_for_servers_in_cluster.par_iter().for_each(|client_channel| {
            //println!(" before send_requests_vote rpc id={}",self.config.id);
            let append_entries_response=client_channel.send_append_entries(
                AppendEntriesRequest::new(self.persistent_state.current_term,
                                          self.config.id(),1, 1,
                vec![],0));
            if append_entries_response.is_ok() {
                if append_entries_response.ok().unwrap().success() {
                    let now = Utc::now();
                    println!("id:{} - send_append_entries response succeded at {}", self.config.id, now.timestamp_millis());
                    ok_votes.fetch_add(1,Ordering::SeqCst);
                } else {
                    println!("id:{} - send_append_entries response NOT succeded", self.config.id);
                }
            } else {
                println!("id:{} - send_append_entries response ko",self.config.id);
            }
        });
        println!("id:{} - send_append_entries result={}",self.config.id, ok_votes.load(Ordering::SeqCst)>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as isize);
        ok_votes.load(Ordering::SeqCst)>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as isize
    }

    pub fn server_state(&self) ->RaftServerState {
        let server_state=self.server_state.lock().unwrap();
        match *server_state {
            ServerState::Follower => {
                RaftServerState::Follower
            }
            ServerState::Candidate => {
                RaftServerState::Candidate
            }
            ServerState::Leader {  .. } =>{
                RaftServerState::Leader
            }
        }
    }

}

