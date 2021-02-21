

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