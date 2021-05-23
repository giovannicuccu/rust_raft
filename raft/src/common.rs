
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


#[derive(PartialEq, PartialOrd,Clone)]
pub enum StateMachineCommand {
    Put {
        key:String,
        value:String,
    },
    Delete {
        key:String,
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

pub struct ApplyCommandRequest {
    command: StateMachineCommand,
}

impl ApplyCommandRequest {
    pub fn new(command: StateMachineCommand) -> Self {
        ApplyCommandRequest { command }
    }
}

pub enum ApplyCommandStatus {
    Ok,
    Ko,
    Pending {
        token: u64
    },
    Redirect {
        leader: String
    }
}

pub struct ApplyCommandResponse {
    status: ApplyCommandStatus,
}

impl ApplyCommandResponse {
    pub fn new(status: ApplyCommandStatus) -> Self {
        ApplyCommandResponse { status }
    }


    pub fn status(&self) -> &ApplyCommandStatus {
        &self.status
    }
}