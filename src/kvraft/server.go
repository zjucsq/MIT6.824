package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

//const Debug1 = false
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug1 {
//		log.Printf(format, a...)
//	}
//	return
//}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	ClientSeq int64
}

type OpResult struct {
	Error Err
	Value string
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvmap  map[string]string
	resChs sync.Map
	maxSeq map[int64]int64
	// maxSeq sync.Map
	// resChs map[int]chan OpResult
}

func (kv *KVServer) applyOne(op Op) OpResult {
	res := OpResult{
		Error: OK,
		Value: "",
	}
	switch op.Type {
	case PUT:
		if maxReq, ok := kv.maxSeq[op.ClientId]; !ok {
			kv.maxSeq[op.ClientId] = op.ClientSeq
			kv.kvmap[op.Key] = op.Value
		} else {
			if maxReq < op.ClientSeq {
				kv.maxSeq[op.ClientId] = op.ClientSeq
				kv.kvmap[op.Key] = op.Value
				//Debug(dKVPut, "KV%d update maxseq from %d to %d", kv.me, maxReq.(int64), op.ClientSeq)
				//Debug(dKVPut, "KV%d Put in map. key=%s, value=%s", kv.me, op.Key, kv.kvmap[op.Key])
			} else {
				Debug(dKVPut, "KV%d Put duplicate because key=%s, value=%s", kv.me, op.Key, kv.kvmap[op.Key])
			}
		}
		//if maxReq, ok := kv.maxSeq.Load(op.ClientId); !ok {
		//	kv.maxSeq.Store(op.ClientId, op.ClientSeq)
		//	kv.kvmap[op.Key] = op.Value
		//} else {
		//	if maxReq.(int64) < op.ClientSeq {
		//		kv.maxSeq.Store(op.ClientId, op.ClientSeq)
		//		kv.kvmap[op.Key] = op.Value
		//		//Debug(dKVPut, "KV%d update maxseq from %d to %d", kv.me, maxReq.(int64), op.ClientSeq)
		//		//Debug(dKVPut, "KV%d Put in map. key=%s, value=%s", kv.me, op.Key, kv.kvmap[op.Key])
		//	} else {
		//		Debug(dKVPut, "KV%d Append duplicate because key=%s, value=%s", kv.me, op.Key, kv.kvmap[op.Key])
		//	}
		//}
		// Debug(dKVPut, "KV%d Put in map. key=%s, value=%s", kv.me, op.Key, op.Value)
	case APPEND:
		if maxReq, ok := kv.maxSeq[op.ClientId]; !ok {
			kv.maxSeq[op.ClientId] = op.ClientSeq
			kv.kvmap[op.Key] += op.Value
		} else {
			if maxReq < op.ClientSeq {
				kv.maxSeq[op.ClientId] = op.ClientSeq
				kv.kvmap[op.Key] += op.Value
			}
		}
		//if maxReq, ok := kv.maxSeq.Load(op.ClientId); !ok {
		//	kv.maxSeq.Store(op.ClientId, op.ClientSeq)
		//	kv.kvmap[op.Key] += op.Value
		//} else {
		//	if maxReq.(int64) < op.ClientSeq {
		//		kv.maxSeq.Store(op.ClientId, op.ClientSeq)
		//		kv.kvmap[op.Key] += op.Value
		//		//Debug(dKVAppend, "KV%d update maxseq from %d to %d", kv.me, maxReq.(int64), op.ClientSeq)
		//		//Debug(dKVAppend, "KV%d Append in map. key=%s, value=%s", kv.me, op.Key, kv.kvmap[op.Key])
		//	}
		//}
	case GET:
		if value, ok := kv.kvmap[op.Key]; !ok {
			res.Error = ErrNoKey
			// Debug(dKVGet, "KV%d Get in map failed, no key=%s", kv.me, op.Key)
		} else {
			res.Value = value
			// Debug(dKVGet, "KV%d Get in map. key=%s, value=%s", kv.me, op.Key, res.Value)
		}
	}
	return res
}

func (kv *KVServer) apply() {
	for cmd := range kv.applyCh {
		if cmd.CommandValid {
			op := cmd.Command.(Op)
			res := kv.applyOne(op)
			// kv.mu.Lock()
			// if ch, ok := kv.resChs[cmd.CommandIndex]; ok {
			if ch, ok := kv.resChs.Load(cmd.CommandIndex); ok {
				ch.(chan OpResult) <- res
			}
			// kv.mu.Unlock()
		} else if cmd.SnapshotValid {
			// kv.applySnapshot(cmd.Snapshot)
		} else {
			Debug(dWarn, "Unknown command")
		}
	}
}

//func (kv *KVServer) makeSnapshot() []byte {
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//	e.Encode(kv.kvmap)
//	tmp := make(map[int64]int64)
//	kv.mu.Lock()
//	kv.maxSeq.Range(func(key, value any) bool {
//		tmp[key.(int64)] = value.(int64)
//		return true
//	})
//	kv.mu.Unlock()
//	e.Encode(tmp)
//	return w.Bytes()
//}

//func (kv *KVServer) applySnapshot(snapshot []byte) {
//	if len(snapshot) == 0 {
//		return
//	}
//	r := bytes.NewBuffer(snapshot)
//	d := labgob.NewDecoder(r)
//	var kvmap map[string]string
//	var maxSeq map[int64]uint64
//	if d.Decode(&kvmap) != nil || d.Decode(&maxSeq) != nil {
//		Debug(dError, "S%d KVServer Read Persist Error!", kv.me)
//	} else {
//		kv.kvmap = kvmap
//		kv.mu.Lock()
//		kv.client_to_last_process_seq = client_to_last_process_seq
//		kv.client_to_last_process_result = client_to_last_process_result
//		kv.mu.Unlock()
//		Debug(dPersist, "S%d KVServer ReadPersist. Data: %v, Seq: %v", kv.me, kv.kvmap, kv.maxSeq)
//	}
//}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:  GET,
		Key:   args.Key,
		Value: "",
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	tmpCh := make(chan OpResult, 1)
	// kv.mu.Lock()
	// kv.resChs[index] = tmpCh
	// kv.mu.Unlock()
	kv.resChs.Store(index, tmpCh)

	select {
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	case res := <-tmpCh:
		reply.Value = res.Value
		reply.Err = res.Error
	}
	//if reply.Err == OK {
	//	Debug(dCLGet, "KV%d Get in map sucess. key=%s, value=%s", kv.me, op.Key, reply.Value)
	//}
	// kv.mu.Lock()
	// delete(kv.resChs, index)
	// kv.mu.Unlock()
	kv.resChs.Delete(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//if value, ok := kv.maxSeq.Load(args.ClientId); ok {
	//	Debug(dKVAppend, "KV%d PutAppend check multiple rpc. key=%s, value=%s, value.(int64)=%d, args.RequestId=%d", kv.me, args.Key, args.Value, value.(int64), args.RequestId)
	//	if value.(int64) >= args.RequestId {
	//		reply.Err = OK
	//		return
	//	}
	//}

	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		ClientSeq: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	tmpCh := make(chan OpResult, 1)
	// kv.mu.Lock()
	// kv.resChs[index] = tmpCh
	// kv.mu.Unlock()
	kv.resChs.Store(index, tmpCh)

	select {
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	case res := <-tmpCh:
		reply.Err = res.Error
	}
	if reply.Err == OK {
		Debug(dKVAppend, "KV%d PutAppend in map success. key=%s, value=%s", kv.me, op.Key, op.Value)
	}
	// kv.mu.Lock()
	// delete(kv.resChs, index)
	// kv.mu.Unlock()
	kv.resChs.Delete(index)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvmap = make(map[string]string)
	kv.maxSeq = make(map[int64]int64)
	// kv.resChs = make(map[int]chan OpResult)

	// snapshot
	//snapshot := persister.ReadSnapshot()
	//kv.applySnapshot(snapshot)
	//kv.persister = persister

	// apply cmd to kvserver
	go kv.apply()

	return kv
}
