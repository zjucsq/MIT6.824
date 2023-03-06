package shardkv

import "time"

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64
	ClientId  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type ClientOp struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	ClientSeq int64
}

func (kv *ShardKV) PrintRetState(lt logTopic, err *Err) {
	Debug(lt, "G%d KV%d Return %s", kv.gid, kv.me, *err)
}

func (kv *ShardKV) checkShard(key string) bool {
	shardId := key2shard(key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Shards[shardId]; !ok {
		return false
	}
	if kv.Shards[shardId].State != Serving {
		return false
	}
	// Debug(dLog, "G%d KV%d's ShardDict=%d", kv.gid, kv.me, kv.Shards)
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	defer kv.PrintRetState(dKVGet, &reply.Err)

	// First check
	shardId := key2shard(args.Key)
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	data := ClientOp{
		Type:  GET,
		Key:   args.Key,
		Value: "",
	}
	op := Op{
		OpType: ClientOpType,
		Data:   data,
	}

	Debug(dKVGet, "G%d KV%d Start Get in shardId=%d. key=%s, value=%s", kv.gid, kv.me, shardId, data.Key, data.Value)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	tmpCh := make(chan OpResult, 1)
	kv.resChs.Store(index, tmpCh)

	select {
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	case res := <-tmpCh:
		reply.Value = res.Value
		reply.Err = res.Error
	}
	kv.resChs.Delete(index)
	//Debug(dKVGet, "G%d KV%d ret Get. key=%s, value=%s", kv.gid, kv.me, data.Key, data.Value)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer kv.PrintRetState(dKVPutAppend, &reply.Err)

	// First check
	shardId := key2shard(args.Key)
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	data := ClientOp{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		ClientSeq: args.RequestId,
	}
	op := Op{
		OpType: ClientOpType,
		Data:   data,
	}

	Debug(dKVAppend, "G%d KV%d Start PutAppend in shardId=%d. key=%s, value=%s.", kv.gid, kv.me, shardId, data.Key, data.Value)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	tmpCh := make(chan OpResult, 1)
	kv.resChs.Store(index, tmpCh)

	select {
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	case res := <-tmpCh:
		reply.Err = res.Error
	}
	//Debug(dKVAppend, "G%d KV%d ret PutAppend. key=%s, value=%s", kv.gid, kv.me, data.Key, data.Value)
	kv.resChs.Delete(index)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
}
