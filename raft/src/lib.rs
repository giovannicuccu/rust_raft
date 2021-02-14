use network::*;

use crate::ServerState::Follower;

type TermType = u32;
type IndexType= u32;
type CandidateIdType = u16;

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
    term_id: TermType,
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
}

/*
Leader State specificato nel protocollo si applica solo se il server è di tipo Leader
Rust consente di specificare degli enum con valori diversi per cui uso questa potenzialità
 */
enum ServerState {
    Leader {
        next_index:Vec<IndexType>,
        match_index:Vec<IndexType>,
    },
    Follower,
    Candidate
}

struct RaftServer<S: ServerChannel, C:ClientChannel> {
    persistent_state: ServerPersistentState,
    volatile_state: ServerVolatileState,
    server_state: ServerState,
    other_servers_in_cluster: Vec<String>,
    clients_for_servers_in_cluster: Vec<C>,
    server_channel: S,
}

fn discover_other_nodes_in_cluster() -> Vec<String> {
    Vec::new()
}

pub fn on_request_vote1() {

}

impl <S: ServerChannel, C:ClientChannel>RaftServer<S,C> {

    pub fn new<N: NetworkChannel<S,C>>(network_channel:N) -> RaftServer<S,C> {
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
            },
            server_state: Follower,
            /*
            Perchè la cosa abbia un senso devo fare discover dei server del cluster
            all'avvio del server -> uso un metodo per fare discovery dei nodi
             */
            other_servers_in_cluster: other_nodes_in_cluster,

            clients_for_servers_in_cluster: clients,
            server_channel,
        };
        raft_server.install_hndlers();
        raft_server
    }

    fn install_hndlers(&self) {
        let callback = || self.on_request_vote();
        self.server_channel.handle_request_vote(callback);
    }

    pub fn on_request_vote(&self) {

    }
}

