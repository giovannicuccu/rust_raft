pub mod network;
pub mod common;
pub mod log;
mod state_machine;

extern crate log as logging;

use crate::ServerState::{Follower, Candidate, Leader};
use crate::common::*;
use crate::network::*;
use std::time::{Instant, Duration};
use rand::prelude::*;
use std::{thread, time};
use std::sync::{Arc};
use parking_lot::{Mutex, Condvar};
use chrono::Utc;
use rayon::prelude::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::sync::atomic::{AtomicIsize, Ordering, AtomicU32};
use std::path::{PathBuf, Path};
use crate::log::{WriteAheadLog, WriteAheadLogEntry};
use walkdir::WalkDir;
use crate::state_machine::StateMachine;
use std::collections::HashMap;
use std::cmp::min;
use logging::{debug,error, info, warn};


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
    current_index:AtomicU32,
    voted_for:Mutex<CandidateIdType>,
    log: Arc<Mutex<WriteAheadLog>>,
    state_machine: Arc<Mutex<StateMachine>>,

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
    state_machine_dir: String,
}

struct LeaderState {
    next_index:Vec<AtomicU32>,
    match_index:Vec<AtomicU32>,
    last_heartbeat_time: Instant,
}

impl ServerConfig {
    pub fn new(id: CandidateIdType,  election_timeout_min: u32, election_timeout_max: u32,
               server_port:u16, other_nodes_in_cluster: Vec<String>,wal_dir:String, state_machine_dir: String) -> ServerConfig {
        ServerConfig {
            id,
            election_timeout_min,
            election_timeout_max,
            server_port,
            other_nodes_in_cluster,
            wal_dir,
            state_machine_dir
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
#[derive(PartialEq, Clone)]
enum ServerState {
    Leader,
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
    persistent_state: Arc<ServerPersistentState>,
    volatile_state: Arc<Mutex<ServerVolatileState>>,
    server_state: Mutex<ServerState>,
    config: Arc<ServerConfig>,
    clients_for_servers_in_cluster: Arc<Vec<C>>,
    leader_state: Arc<Mutex<LeaderState>>,
    wait_map: Arc<Mutex<HashMap<IndexType,Arc<(Mutex<bool>, Condvar)>>>>
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

impl <C:'static + ClientChannel+Send+Sync >RaftServer<C> {

    pub fn new<N: NetworkChannel<Client=C>>(server_config: ServerConfig, network_channel:N) -> RaftServer<C> {
        //let server_channel=network_channel.server_channel();
        /*
        Capire perchè dopo che ho messo clients_len si incammella con il tipo
         */
        let clients:Vec<C>=server_config.other_nodes_in_cluster.iter().map(|address|network_channel.client_channel(String::from(address))).collect();
        let log_index_for_remotes:Vec<AtomicU32>=server_config.other_nodes_in_cluster.iter().map(|address|AtomicU32::new(0)).collect();
        let file_dir = server_config.wal_dir.as_str();
        let mut last_modified=0;
        let mut file_name:Option<String>=None;
        debug!("file_dir {}",file_dir);
        for entry in WalkDir::new(file_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok()) {
            if entry.path().is_file() {
                let f_name = entry.file_name().to_string_lossy();
                debug!("found file {}", f_name.parse::<String>().unwrap());
                let sec = entry.metadata().unwrap().modified().unwrap();
                if last_modified == 0 {
                    last_modified = sec.elapsed().unwrap().as_secs() + 1;
                    debug!("last_modified {}", last_modified);
                }
                if f_name.ends_with(".wal") && sec.elapsed().unwrap().as_secs() < last_modified {
                    file_name = Some(Path::new(file_dir).join(f_name.parse::<String>().unwrap()).to_string_lossy().parse::<String>().unwrap());
                    debug!("found wal {}", f_name.parse::<String>().unwrap());
                }
            }
        }
        let wal=match file_name {
            None => {debug!("found wal dir {}",file_dir); WriteAheadLog::new(file_dir).unwrap()},
            Some(file_name) => {debug!("found wal file {}",file_name);WriteAheadLog::from_path(&*file_name).unwrap()},
        };
        let last_entry=wal.last_entry();
        let last_index =match last_entry {
            None => { NO_INDEX }
            Some( log_entry) => { log_entry.index() }
        };
        let wal_mutex=Arc::new(Mutex::new(wal));
        let state_machine_mutex=Arc::new(Mutex::new(StateMachine::open(server_config.state_machine_dir.clone())));

        RaftServer {
            persistent_state: Arc::new(ServerPersistentState {
                current_term:NO_TERM,
                current_index: AtomicU32::new(last_index),
                voted_for:Mutex::new(NO_CANDIDATE_ID),
                log: wal_mutex,
                state_machine: state_machine_mutex
            }),
            volatile_state: Arc::new(Mutex::new(ServerVolatileState {
                commit_index:NO_VALUE,
                last_applied:NO_VALUE,
                last_heartbeat_time: Instant::now(),
            })),
            server_state: Mutex::new(Follower),
            config: Arc::new(server_config),
            /*
            Perchè la cosa abbia un senso devo fare discover dei server del cluster
            all'avvio del server -> uso un metodo per fare discovery dei nodi
             */

            clients_for_servers_in_cluster: Arc::new(clients),
            leader_state: Arc::new(Mutex::new(LeaderState {next_index: vec![], match_index: vec![], last_heartbeat_time: Instant::now()} )),
            wait_map: Arc::new(Mutex::new(HashMap::new())),
        }

    }

    pub fn on_request_vote(&self,request_vote_request: RequestVoteRequest) -> RequestVoteResponse {
        debug!("id:{} - on_request_vote invoked",self.config.id);
        if request_vote_request.term()<self.persistent_state.current_term {
            debug!("id:{} - on_request_vote granted ko minor term",self.config.id);
            return RequestVoteResponse::new(NO_TERM,false);
        }
        let mut last_index=NO_INDEX;
        debug!("id:{} - on_request_vote log is locked  {}",self.config.id, self.persistent_state.log.is_locked());
        let log_mutex=self.persistent_state.log.lock();
        let last_entry_opt= log_mutex.last_entry();
        if last_entry_opt.is_some() {
            last_index=last_entry_opt.unwrap().index();
        }

        let mut voted_for_guard=self.persistent_state.voted_for.lock();
        let mut volatile_state=self.volatile_state.lock();
        volatile_state.last_heartbeat_time=Instant::now();
        drop(volatile_state);
        //println!("id:{} - on_request_vote voted_for={}, candidate={} term",*voted_for_guard,request_vote_request.candidate_id(),self.config.id);
        if (*voted_for_guard==NO_CANDIDATE_ID || *voted_for_guard==request_vote_request.candidate_id()) &&
            //verificare se è meglio ottenere il valore di last con getOrElse o qualcosa del genere
            last_index<=request_vote_request.last_log_index() {
            *voted_for_guard= request_vote_request.candidate_id() as u16;
            debug!("id:{} - on_request_vote granted ok to {}",self.config.id,*voted_for_guard);
            return RequestVoteResponse::new(request_vote_request.term(),true);
        }
        debug!("id:{} - on_request_vote granted ko to {}",self.config.id,request_vote_request.candidate_id());
        RequestVoteResponse::new(NO_TERM,false)
    }

    pub fn on_append_entries(&self,append_entries_request: AppendEntriesRequest) -> AppendEntriesResponse {
        debug!("id:{} - on_append_entries begin",self.config.id);
        let mut mutex_guard = self.server_state.lock();
        debug!("id:{} - on_append_entries got mutex",self.config.id);

        match *mutex_guard {
            ServerState::Leader => {
                debug!("id:{} - on_append_entries error server state Leader ",self.config.id);
                AppendEntriesResponse::new(self.persistent_state.current_term,false)
            }
            Follower => {
                self.internal_append_entries(append_entries_request)
            }
            Candidate => {
                debug!("id:{} - on_append_entries ServerState::Candidate will become Follower",self.config.id);
                *mutex_guard = Follower;
                self.internal_append_entries(append_entries_request)
                //println!("id:{} - on_append_entries ServerState now is Follower={} now ={}",self.config.id,*mutex_guard==Follower, now.timestamp_millis());
            }
        }

    }

    fn internal_append_entries(&self,append_entries_request: AppendEntriesRequest) -> AppendEntriesResponse{
        debug!("id:{} - on_append_entries ServerState::Follower",self.config.id);
        if self.persistent_state.current_term>append_entries_request.term() {
            return AppendEntriesResponse::new(self.persistent_state.current_term,false);
        }
        if append_entries_request.entries().len()>0 {
            let mut log = self.persistent_state.log.lock();
            let mut log_reader = log.record_entry_iterator().unwrap();
            let seek_result = log_reader.seek(append_entries_request.prev_log_index());
            if seek_result.is_ok() {
                let log_entry = seek_result.unwrap();
                if log_entry.term() != append_entries_request.term() {
                    return AppendEntriesResponse::new(self.persistent_state.current_term, false);
                }
                let mut prev_index_opt = None;
                for entry in append_entries_request.entries() {
                    let entry_opt = log_reader.next();
                    if entry_opt.is_none() {
                        let encoded_command: Vec<u8> = bincode::serialize(entry.state_machine_command()).unwrap();
                        log.append_entry(append_entries_request.term(), &encoded_command);
                        prev_index_opt = Some(entry.index());
                    } else {
                        let log_entry = entry_opt.unwrap();
                        if entry.index() != log_entry.index() {
                            //TODO: capire cosa fare non dovrebbe mai accadere
                        } else {
                            debug!("id:{} - on_append_entries ServerState::Follower replicating entry {}",self.config.id, entry.index());
                            if entry.term() != log_entry.term() {
                                match prev_index_opt {
                                    None => { log.seek_and_clear_after(append_entries_request.prev_log_index()); }
                                    Some(index) => { log.seek_and_clear_after(index); }
                                }
                                let encoded_command: Vec<u8> = bincode::serialize(entry.state_machine_command()).unwrap();
                                log.append_entry(append_entries_request.term(), &encoded_command);
                            }
                        }
                    }
                }
                let mut volatile_state = self.volatile_state.lock();
                if append_entries_request.entries().len() > 0 {
                    volatile_state.commit_index = min(append_entries_request.leader_commit(), append_entries_request.entries().last().unwrap().index());
                }
            } else {
                debug!("id:{} - on_append_entries no prev index {}",self.config.id, append_entries_request.prev_log_index());
            }
        }
        let mut mutex_volatile_state_guard = self.volatile_state.lock();
        mutex_volatile_state_guard.last_heartbeat_time=Instant::now();
        AppendEntriesResponse::new(self.persistent_state.current_term,true)

    }

    pub fn on_apply_command(&self,append_entries_request: ApplyCommandRequest) -> ApplyCommandResponse {
        debug!("id:{} - on_apply_command start",self.config.id);
        let encoded_command: Vec<u8> = bincode::serialize(&append_entries_request).unwrap();
        //self.persistent_state.current_index+=1;
        let mut log_mutex =self.persistent_state.log.lock();

        let result=log_mutex.append_entry(self.persistent_state.current_term, &encoded_command);
        debug!("id:{} - on_apply_command entry persistend on master",self.config.id);
        if result.is_ok() {
            let index=result.unwrap();
            self.persistent_state.current_index.store(index, Ordering::SeqCst);
            let mut wait_map=self.wait_map.lock();
            let pair=Arc::new((Mutex::new(false), Condvar::new()));
            wait_map.insert(index,pair.clone());
            let &(ref mutex, ref cond_var) = &*pair;
            let mut started = mutex.lock();
            if !*started {
                debug!("id:{} - on_apply_command waiting for remote insertion",self.config.id);
                // TODO Rendere parametrizzabile i millisecondi di attesa
                cond_var.wait_for(&mut started, Duration::from_millis(500));
                debug!("id:{} - after wait_for propagation for index {}",self.config.id, index);
            }
        }

        ApplyCommandResponse::new(ApplyCommandStatus::Ok)
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
        //println!("id:{} - manage_server_state start at {}",self.config.id, Utc::now().timestamp_millis());
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
            let mutex_guard = self.server_state.lock();
            let server_state_cloned=mutex_guard.clone();
            let now = Instant::now();
            drop(mutex_guard);
            match server_state_cloned {
                ServerState::Follower => {
                    let mutex_volatile_state_guard = self.volatile_state.lock();
                    debug!("id:{} - manage_server_state ServerState::Follower last heartbit  {} ms ago",self.config.id,now.saturating_duration_since(mutex_volatile_state_guard.last_heartbeat_time).as_millis());
                    let election_timeout: u32 = rng.gen_range(self.config.election_timeout_min..=self.config.election_timeout_max);
                    if now.saturating_duration_since(mutex_volatile_state_guard.last_heartbeat_time).as_millis() >= election_timeout as u128 {
                        //il timeout è random fra due range da definire--
                        //Avviare la richiesta di voto
                        let mut mutex_guard = self.server_state.lock();
                        *mutex_guard = Candidate;
                        debug!("id:{} - manage_server_state ServerState::Follower becoming candidate",self.config.id);
                    } else {
                        thread::sleep(time::Duration::from_millis(election_timeout as u64));
                    }
                }
                ServerState::Candidate =>{
                    debug!("id:{} - manage_server_state ServerState::Candidate",self.config.id);
                    if self.send_requests_vote() {
                        let mut mutex_guard = self.server_state.lock();
                        if *mutex_guard==Candidate {
                            let remotes_num=self.clients_for_servers_in_cluster.len();
                            let mut log =self.persistent_state.log.lock();
                            let opt_last_log_id=log.last_entry().map(|entry| entry.index());
                            *mutex_guard = Leader;
                            debug!("id:{} - manage_server_state ServerState::Leader",self.config.id);
                            let mut leader_state=self.leader_state.lock();
                            let mut vec=Vec::new();
                            for _ in 1..=remotes_num {
                                vec.push(AtomicU32::new(opt_last_log_id.unwrap_or(0)));
                            }
                            leader_state.next_index=vec;
                            let mut vec=Vec::new();
                            for _ in 1..=remotes_num {
                                vec.push(AtomicU32::new(0));
                            }
                            leader_state.match_index=vec;
                            let mut voted_for_guard = self.persistent_state.voted_for.lock();
                            *voted_for_guard = self.config.id;
                            self.start_senders();
                        }
                    } else {
                        let mutex_guard = self.server_state.lock();
                        if *mutex_guard==Candidate {
                            let election_timeout: u32 = rng.gen_range(self.config.election_timeout_min..=self.config.election_timeout_max);
                            debug!("id:{} - start ServerState::Candidate will sleep for  {} ms ", self.config.id, election_timeout);
                            thread::sleep(time::Duration::from_millis(election_timeout as u64));
                        }
                    }
                }
                //uso ref per non prendere la ownership
                ServerState::Leader =>{
                    //println!("id:{} - start ServerState::Leader at {}",self.config.id, Utc::now().timestamp_millis());
                    //if self.send_append_entries() {

                    //}
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

    pub fn start_senders(&self) {
        debug!("id:{} - start_senders start",self.config.id);
        /*let mutex_guard = self.server_state.lock();
        let server_state_cloned=mutex_guard.clone();
        let now = Instant::now();
        drop(mutex_guard);
        match server_state_cloned {
            ServerState::Leader =>{*/
                for i in 0..self.clients_for_servers_in_cluster.len() {
                    let th_persistent_state=Arc::clone(&self.persistent_state);
                    let th_leader_state=Arc::clone(&self.leader_state);
                    let th_config=Arc::clone(&self.config);
                    let th_wait_map=Arc::clone(&self.wait_map);
                    let th_volatile_state=Arc::clone(&self.volatile_state);
                    let th_clients_for_servers_in_cluster=Arc::clone(&self.clients_for_servers_in_cluster);
                    debug!("{} spawn thread for destination {}",self.config.id, th_clients_for_servers_in_cluster.get(i).unwrap().get_destination());
                    thread::spawn(move || {
                        loop {

                            RaftServer::send_append_entries_to_remote(i, &th_persistent_state,
                                                                      &th_leader_state, &th_config, &th_wait_map,
                                                                      &th_volatile_state, &th_clients_for_servers_in_cluster);
                        }
                    });
                }
            /*}
            _ => {}
        }*/
    }

    fn send_requests_vote(&self)-> bool {
        debug!("id:{} - send_requests_vote start",self.config.id);
/*
TODO: capire come gestire lo start quando non ci sono entry e si deve chiedere una request vote
gestire forse non con NO_LOG_ENTRY ma con i singoli valori di default
 */
        let mut last_log_index=NO_INDEX;
        let mut last_log_term = NO_TERM;
        //println!("id:{} - send_requests_vote before persistent_state.log.lock()",self.config.id);
        let log_mutex=self.persistent_state.log.lock();
        let last_entry_opt= log_mutex.last_entry();
        if last_entry_opt.is_some() {
            let last_log_entry=last_entry_opt.unwrap();
            last_log_index=last_log_entry.index();
        }
        drop(log_mutex);
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
        debug!("id:{} - send_requests_vote result={} ok_votes={}",self.config.id, ok_votes.load(Ordering::SeqCst)>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as isize, ok_votes.load(Ordering::SeqCst));
        ok_votes.load(Ordering::SeqCst)>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as isize
    }

/*    fn send_append_entries(&self)-> bool {
        let now = Utc::now();
        debug!("id:{} - send_append_entries at {}",self.config.id, now.timestamp_millis());
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
                    debug!("id:{} - send_append_entries response succeded", self.config.id);
                    ok_votes.fetch_add(1,Ordering::SeqCst);
                } else {
                    debug!("id:{} - send_append_entries response NOT succeded", self.config.id);
                }
            } else {
                debug!("id:{} - send_append_entries response ko",self.config.id);
            }
        });
        debug!("id:{} - send_append_entries result={}",self.config.id, ok_votes.load(Ordering::SeqCst)>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as isize);
        ok_votes.load(Ordering::SeqCst)>= ((self.config.other_nodes_in_cluster.len() / 2)+1) as isize
    }*/

    pub fn server_state(&self) ->RaftServerState {
        let server_state=self.server_state.lock();
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



    //TODO: qui posso usare le reference dove serve non gli oggetti
    fn send_append_entries_to_remote(index:usize, persistent_state: &ServerPersistentState, leader_state: &Mutex<LeaderState>,
                                     config: &ServerConfig, wait_map: &Mutex<HashMap<IndexType,Arc<(Mutex<bool>, Condvar)>>>,
                                     volatile_state: &Mutex<ServerVolatileState>, clients_for_servers_in_cluster: &Vec<C>) {
        let destination=clients_for_servers_in_cluster.get(index).unwrap().get_destination();
        debug!("id:{} - send_append_entries to {} start",config.id,destination);
        //TODO pensare se non ci sia un pattern per liberare le risorse senza usare option
        let mut log =persistent_state.log.lock();
        let mut log_reader = log.record_entry_iterator().unwrap();
        drop(log);
        let leader_state_mg=leader_state.lock();

        let log_index=&leader_state_mg.next_index[index];
        let last_log_index=log_index.load(Ordering::Relaxed);
        //TODO sistemare questo calcolo con un valore che eviti una nuova elezione
        let force_send=Instant::now().saturating_duration_since(leader_state_mg.last_heartbeat_time).as_millis() >= (config.election_timeout_min-10) as u128;
        drop(leader_state_mg);


        if last_log_index==persistent_state.current_index.load(Ordering::Relaxed) && !force_send {
            debug!("id:{} - send_append_entries last_log_index={} current_index= {}", config.id, last_log_index,persistent_state.current_index.load(Ordering::Relaxed));
            thread::sleep(time::Duration::from_millis(50 as u64));
            return;
        }


        if !force_send {
            debug!("id:{} - send_append_entries_to_remote {} last_log_index={} current_index= {} start", config.id,destination, last_log_index,persistent_state.current_index.load(Ordering::Relaxed));
        } else {
            debug! ("id:{} - force_send true sending empty request to {}", config.id,destination );
        }
        let mut log_entries =vec![];
        let mut last_log_index_sent = last_log_index;
        if !force_send {
            debug!("id:{} - send_append_entries some entry to propagate", config.id);
            //let mut log_reader = log.record_entry_iterator().unwrap();
            if last_log_index > 0 {
                let seek_result = log_reader.seek(last_log_index);
                if !seek_result.is_ok() {
                    //TODO: decidere cosa fare non dovrebbe mai accadere.....
                    debug!("id:{} - send_append_entries unable to find index {} in log ",config.id, last_log_index);
                    return;
                }
            }

            while let Some(wal_entry) = log_reader.next() {
                let data = wal_entry.data();
                let state_machine_command = bincode::deserialize(&data).unwrap();
                last_log_index_sent = wal_entry.index();
                log_entries.push(LogEntry::new(persistent_state.current_term, wal_entry.index(), state_machine_command));
            }
        }
        let mutex_volatile_state_guard = volatile_state.lock();
        let append_entries_request=AppendEntriesRequest::new(persistent_state.current_term,
                                  config.id(),last_log_index, 1,
                                  log_entries,mutex_volatile_state_guard.commit_index);
        let client_channel=&clients_for_servers_in_cluster[index];
        let append_entries_response=client_channel.send_append_entries(append_entries_request);
        if append_entries_response.is_ok() {
            if append_entries_response.ok().unwrap().success() {
                debug!("id:{} - send_append_entries to {} succeded", config.id, destination);
                if !force_send {
                    let leader_state_mg=leader_state.lock();
                    let match_index=&leader_state_mg.match_index[index];
                    let log_index=&leader_state_mg.next_index[index];
                    match_index.store(last_log_index_sent, Ordering::Release);
                    log_index.store(last_log_index_sent + 1, Ordering::Release);
                    drop(leader_state_mg);
                    let mut wait_map = wait_map.lock();
                    for sent_index in last_log_index..=last_log_index_sent {
                        let opt_cond_var = wait_map.get(&sent_index);
                        if opt_cond_var.is_some() {
                            let pair = opt_cond_var.unwrap();
                            let &(ref lock, ref cvar) = &**pair;
                            let mut started = lock.lock();
                            *started = true;
                            cvar.notify_one();
                            debug!("notify one for index {}", sent_index);
                        }
                        wait_map.remove(&sent_index);
                    }
                }
                let mut leader_state_mg=leader_state.lock();
                leader_state_mg.last_heartbeat_time=Instant::now();
                drop(leader_state_mg);
            } else {
                let leader_state_mg=leader_state.lock();
                let log_index=&leader_state_mg.next_index[index];
                log_index.fetch_sub(1,Ordering::Release);
                drop(leader_state_mg);
                debug!("id:{} - send_append_entries response NOT succeded", config.id);
            }
        } else {
            debug!("id:{} - send_append_entries response ko",config.id);
        }
    }

}

