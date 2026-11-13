package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	SeqNumber int
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

type GetArgs struct {
	Key       string
	ClientId  int64
	SeqNumber int
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
}

type Op struct {
	Type      string
	Key       string
	Value     string
	ClientId  int64
	SeqNumber int
}

type OpResult struct {
	Err   Err
	Value string
}
