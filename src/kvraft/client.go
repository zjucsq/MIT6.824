package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64 // id for this clerk
	leaderId  int   // In previous rpc, id for the raft's leader
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.requestId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	Debug(dCLGet, "C%d Begin Send Get. Key: %s. init leaderId=%d", ck.clientId, key, ck.leaderId)
	args := GetArgs{
		Key: key,
	}

	leaderId := ck.leaderId

	for {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		// For ErrNoKey, return ""
		if !ok || (reply.Err != OK && reply.Err != ErrNoKey) {
			// if reply.Err == ErrWrongLeader {
			// 	Debug(dCLGet, "C%d Try Get in leaderId=%d failed because it is not leader.", ck.clientId, leaderId)
			// } else if reply.Err == ErrTimeout {
			// 	Debug(dCLGet, "C%d Try Get in leaderId=%d failed because timeout.", ck.clientId, leaderId)
			// } else {
			// 	Debug(dCLGet, "C%d Try Get in leaderId=%d failed because other unknown reason=%s.", ck.clientId, leaderId, reply.Err)
			// }
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		ck.leaderId = leaderId
		Debug(dCLGet, "C%d Get Success. leaderId=%d key=%s value=%s.", ck.clientId, ck.leaderId, key, reply.Value)
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	Debug(dCLPutAppend, "C%d Begin Send PutAppend. Key: %s, Val: %s, Op: %s. requestId=%d init leaderId=%d.", ck.clientId, key, value, op, ck.requestId, ck.leaderId)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		RequestId: ck.requestId,
		ClientId:  ck.clientId,
	}

	leaderId := ck.leaderId

	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err != OK {
			// if reply.Err == ErrWrongLeader {
			// 	Debug(dCLPutAppend, "C%d Try PutAppend in leaderId=%d failed because it is not leader.", ck.clientId, leaderId)
			// } else if reply.Err == ErrTimeout {
			// 	Debug(dCLPutAppend, "C%d Try PutAppend in leaderId=%d failed because timeout.", ck.clientId, leaderId)
			// } else {
			// 	Debug(dCLPutAppend, "C%d Try PutAppend in leaderId=%d failed because other unknown reason=%s.", ck.clientId, leaderId, reply.Err)
			// }
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		ck.leaderId = leaderId
		ck.requestId += 1
		Debug(dCLPutAppend, "C%d PutAppend Success. leaderId=%d. Key: %s, Val: %s, Op: %s.", ck.clientId, ck.leaderId, key, value, op)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
