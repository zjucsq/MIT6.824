package shardkv

import "time"

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
	Debug(lt, "Return %s", *err)
}

func (kv *ShardKV) checkShard(key string) bool {
	shardId := key2shard(key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Shards[shardId]; !ok {
		return false
	}
	if kv.Shards[shardId].state != Serving {
		return false
	}
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

	Debug(dKVGet, "KV%d Start Get in shardId=%d. key=%s, value=%s", kv.me, shardId, data.Key, data.Value)
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
	//Debug(dKVGet, "KV%d ret Get. key=%s, value=%s", kv.me, data.Key, data.Value)
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

	Debug(dKVAppend, "KV%d Start PutAppend in shardId=%d. key=%s, value=%s.", kv.me, shardId, data.Key, data.Value)
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
	//Debug(dKVAppend, "KV%d ret PutAppend. key=%s, value=%s", kv.me, data.Key, data.Value)
	kv.resChs.Delete(index)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
}
