package kvraft

import (
	"fmt"
	"replicated-kv/raft"
	"sync"
	"time"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	kvStore  map[string]string
	lastSeq  map[int64]int
	notifyCh map[int]chan OpResult
}

func StartKVServer(peers []string, me int) *KVServer {
	applyCh := make(chan raft.ApplyMsg, 100)
	kv := &KVServer{
		me:       me,
		applyCh:  applyCh,
		kvStore:  make(map[string]string),
		lastSeq:  make(map[int64]int),
		notifyCh: make(map[int]chan OpResult),
	}

	kv.rf = raft.Make(peers, me, applyCh)
	go kv.applyLoop()

	return kv
}

func (kv *KVServer) GetRaft() *raft.Raft {
	return kv.rf
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		SeqNumber: args.SeqNumber,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	ch := kv.getNotifyChan(index)
	defer kv.deleteNotifyChan(index)

	select {
	case res := <-ch:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(1500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	return nil
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		SeqNumber: args.SeqNumber,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	ch := kv.getNotifyChan(index)
	defer kv.deleteNotifyChan(index)

	select {
	case res := <-ch:
		reply.Err = res.Err
	case <-time.After(1500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	return nil
}

func (kv *KVServer) getNotifyChan(index int) chan OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exists := kv.notifyCh[index]
	if !exists {
		ch = make(chan OpResult, 1)
		kv.notifyCh[index] = ch
	}
	return ch
}

func (kv *KVServer) deleteNotifyChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyCh, index)
}

func (kv *KVServer) applyLoop() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		kv.mu.Lock()
		res := OpResult{Err: OK}

		// Check for duplicate request to guarantee linearizability and at-most-once semantics
		lastSeq, seen := kv.lastSeq[op.ClientId]
		isDuplicate := seen && op.SeqNumber <= lastSeq

		switch op.Type {
		case "Put":
			if !isDuplicate {
				kv.kvStore[op.Key] = op.Value
				kv.lastSeq[op.ClientId] = op.SeqNumber
				fmt.Printf("[Server %d] Committed PUT: key='%s' -> '%s' (Log Index: %d)\n",
					kv.me, op.Key, op.Value, msg.CommandIndex)
			}
		case "Append":
			if !isDuplicate {
				kv.kvStore[op.Key] = kv.kvStore[op.Key] + op.Value
				kv.lastSeq[op.ClientId] = op.SeqNumber
				fmt.Printf("[Server %d] Committed APPEND: key='%s' -> '%s' (Log Index: %d)\n",
					kv.me, op.Key, kv.kvStore[op.Key], msg.CommandIndex)
			}
		case "Get":
			val, exists := kv.kvStore[op.Key]
			if exists {
				res.Value = val
				fmt.Printf("[Server %d] Served GET: key='%s' -> '%s'\n", kv.me, op.Key, val)
			} else {
				res.Err = ErrNoKey
			}
		}

		// Notify waiting client RPC handler if this server originated the request
		if ch, exists := kv.notifyCh[msg.CommandIndex]; exists {
			ch <- res
		}
		kv.mu.Unlock()
	}
}
