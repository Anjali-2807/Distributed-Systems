package kvraft

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"time"
)

type Clerk struct {
	servers   []string
	leaderId  int
	clientId  int64
	seqNumber int
}

func nrand() int64 {
	max := new(big.Int).Lsh(big.NewInt(1), 62)
	n, _ := rand.Int(rand.Reader, max)
	return n.Int64()
}

func MakeClerk(servers []string) *Clerk {
	ck := &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		seqNumber: 0,
	}
	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.seqNumber++
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		SeqNumber: ck.seqNumber,
	}

	for {
		var reply GetReply
		serverAddr := ck.servers[ck.leaderId]
		client, err := rpc.Dial("tcp", serverAddr)
		if err == nil {
			rpcErr := client.Call("KVServer.Get", &args, &reply)
			client.Close()
			if rpcErr == nil {
				if reply.Err == OK {
					return reply.Value
				}
				if reply.Err == ErrNoKey {
					return ""
				}
			}
		}

		// Rotate to next server if current one failed or is not the leader
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.putAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.putAppend(key, value, "Append")
}

func (ck *Clerk) putAppend(key string, value string, op string) {
	ck.seqNumber++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		SeqNumber: ck.seqNumber,
	}

	for {
		var reply PutAppendReply
		serverAddr := ck.servers[ck.leaderId]
		client, err := rpc.Dial("tcp", serverAddr)
		if err == nil {
			rpcErr := client.Call("KVServer.PutAppend", &args, &reply)
			client.Close()
			if rpcErr == nil && reply.Err == OK {
				return
			}
		}

		// Try next server on error or leadership change
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}
