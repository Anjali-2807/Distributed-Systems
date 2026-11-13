package raft

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type Raft struct {
	mu        sync.Mutex
	peers     []string
	me        int
	dead      int32
	applyCh   chan ApplyMsg

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state          Role
	lastHeartbeat  time.Time
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
}

func Make(peers []string, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		me:          me,
		applyCh:     applyCh,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1), // 1-based indexing with dummy entry at 0
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		state:       Follower,
	}

	rf.log[0] = LogEntry{Index: 0, Term: 0, Command: nil}
	rf.lastHeartbeat = time.Now()

	go rf.ticker()
	return rf
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) GetMe() int {
	return rf.me
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	lastIdx := len(rf.log) - 1
	return lastIdx, rf.log[lastIdx].Term
}

// Start accepts a new client command to replicate
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	go rf.broadcastAppendEntries()

	return index, term, true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	lastLogIdx, lastLogTerm := rf.getLastLogIndexAndTerm()
	logUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx {
		logUpToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	return nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	rf.lastHeartbeat = time.Now()

	// Check log consistency
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	// Append any new entries not already in the log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}

	// Update commit index
	if args.LeaderCommit > rf.commitIndex {
		lastIdx, _ := rf.getLastLogIndexAndTerm()
		if args.LeaderCommit < lastIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIdx
		}
		go rf.applyEntries()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	return nil
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()

	term := rf.currentTerm
	lastLogIdx, lastLogTerm := rf.getLastLogIndexAndTerm()
	votes := 1
	nPeers := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < nPeers; i++ {
		if i == rf.me {
			continue
		}

		go func(peerIdx int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIdx,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply

			client, err := rpc.Dial("tcp", rf.peers[peerIdx])
			if err != nil {
				return
			}
			defer client.Close()

			err = client.Call("Raft.RequestVote", &args, &reply)
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				return
			}

			if rf.state == Candidate && rf.currentTerm == term && reply.VoteGranted {
				votes++
				if votes > nPeers/2 {
					rf.state = Leader
					fmt.Printf("[Node %d] Elected LEADER for Term %d (received %d/%d votes)\n",
						rf.me, rf.currentTerm, votes, nPeers)
					lastIdx, _ := rf.getLastLogIndexAndTerm()
					for p := 0; p < nPeers; p++ {
						rf.nextIndex[p] = lastIdx + 1
						rf.matchIndex[p] = 0
					}
					rf.matchIndex[rf.me] = lastIdx
					go rf.broadcastAppendEntries()
				}
			}
		}(i)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	nPeers := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < nPeers; i++ {
		if i == rf.me {
			continue
		}

		go func(peerIdx int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			prevLogIdx := rf.nextIndex[peerIdx] - 1
			if prevLogIdx < 0 {
				prevLogIdx = 0
			}
			prevLogTerm := rf.log[prevLogIdx].Term

			entries := make([]LogEntry, 0)
			if len(rf.log) > rf.nextIndex[peerIdx] {
				entries = append(entries, rf.log[rf.nextIndex[peerIdx]:]...)
			}

			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply
			client, err := rpc.Dial("tcp", rf.peers[peerIdx])
			if err != nil {
				return
			}
			defer client.Close()

			err = client.Call("Raft.AppendEntries", &args, &reply)
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				return
			}

			if rf.state == Leader && rf.currentTerm == term {
				if reply.Success {
					rf.matchIndex[peerIdx] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1

					// Check if a log entry can be committed by majority
					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						if rf.log[N].Term == rf.currentTerm {
							count := 1
							for p := 0; p < nPeers; p++ {
								if p != rf.me && rf.matchIndex[p] >= N {
									count++
								}
							}
							if count > nPeers/2 {
								rf.commitIndex = N
								go rf.applyEntries()
								break
							}
						}
					}
				} else {
					if rf.nextIndex[peerIdx] > 1 {
						rf.nextIndex[peerIdx]--
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
			CommandTerm:  rf.log[rf.lastApplied].Term,
		}
		rf.applyCh <- msg
	}
}

func (rf *Raft) ticker() {
	for {
		// Randomized election timeout between 250ms and 450ms
		timeout := time.Duration(250+rand.Intn(200)) * time.Millisecond
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.broadcastAppendEntries()
			time.Sleep(80 * time.Millisecond)
			continue
		}

		if time.Since(rf.lastHeartbeat) > timeout {
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}
