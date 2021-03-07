
pub type TermType = u32;
pub type IndexType= u32;
pub type CandidateIdType = u16;

pub struct RequestVoteRequest {
    term: TermType,
    candidate_id: CandidateIdType,
    last_log_index: IndexType,
    last_log_term: TermType,
}

impl RequestVoteRequest {
    pub fn new(term: TermType, candidate_id: CandidateIdType,last_log_index: IndexType, last_log_term: TermType) -> RequestVoteRequest {
        RequestVoteRequest{
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }

    pub fn term(&self) -> TermType {
        self.term
    }

    pub fn candidate_id(&self) -> CandidateIdType {
        self.candidate_id
    }

    pub fn last_log_index(&self) -> IndexType {
        self.last_log_index
    }

    pub fn last_log_term(&self) -> TermType {
        self.last_log_term
    }
}

pub struct RequestVoteResponse {
    term: TermType,
    vote_granted: bool,
}

impl RequestVoteResponse {
    pub fn new(term: TermType, vote_granted: bool,) -> RequestVoteResponse {
        RequestVoteResponse{
            term,
            vote_granted
        }
    }
    pub fn term(&self) -> TermType {
        self.term
    }
    pub fn vote_granted(&self) -> bool {
        self.vote_granted
    }
}

pub enum CommandType {
    Put=1,
    Delete=2,
}

pub struct StateMachineCommand {
    command_type: CommandType,
    key: String,
    value: String,
}

impl StateMachineCommand {
    pub fn new(command_type: CommandType, key: String, value: String) -> Self {
        StateMachineCommand { command_type, key, value }
    }
    pub fn command_type(&self) -> &CommandType {
        &self.command_type
    }
    pub fn key(&self) -> &String {
        &self.key
    }
    pub fn value(&self) -> &String {
        &self.value
    }

}

pub struct LogEntry {
    index: IndexType,
    term: TermType,
    state_machine_command: StateMachineCommand,
}

impl LogEntry {

    pub fn new(index: IndexType, term: TermType, state_machine_command: StateMachineCommand) -> Self {
        LogEntry { index, term, state_machine_command }
    }

    pub fn index(&self) -> IndexType {
        self.index
    }

    pub fn term(&self) -> TermType {
        self.term
    }

    pub fn state_machine_command(&self) -> &StateMachineCommand {
        &self.state_machine_command
    }
}

pub struct AppendEntriesRequest {
    term: TermType,
    leader_id: CandidateIdType,
    prev_log_index: IndexType,
    prev_log_term: TermType,
    entries: Vec<LogEntry>,
    leader_commit: IndexType,
}

impl AppendEntriesRequest {

    pub fn new(term: TermType, leader_id: CandidateIdType, prev_log_index: IndexType, prev_log_term: TermType, entries: Vec<LogEntry>, leader_commit: IndexType) -> Self {
        AppendEntriesRequest { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit }
    }

    pub fn term(&self) -> TermType {
        self.term
    }
    pub fn leader_id(&self) -> CandidateIdType {
        self.leader_id
    }
    pub fn prev_log_index(&self) -> IndexType {
        self.prev_log_index
    }
    pub fn prev_log_term(&self) -> TermType {
        self.prev_log_term
    }
    pub fn entries(&self) -> &Vec<LogEntry> {
        &self.entries
    }
    pub fn leader_commit(&self) -> IndexType {
        self.leader_commit
    }

}

pub struct AppendEntriesResponse {
    term: TermType,
    success: bool,
}

impl AppendEntriesResponse {
    pub fn new(term: TermType, success: bool) -> Self {
        AppendEntriesResponse { term, success }
    }

    pub fn term(&self) -> TermType {
        self.term
    }
    pub fn success(&self) -> bool {
        self.success
    }
}