package raft

// Role represents the current consensus state of a Raft node
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry stores an individual command with the term it was received
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// ApplyMsg is sent on applyCh whenever a log entry is committed
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

// RequestVoteArgs for leader election
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply response from voters
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs for heartbeats and log replication
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply response to AppendEntries RPC
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}
