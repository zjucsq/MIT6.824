package shardkv

import "time"

type GCArgs struct {
	CfgNum  int
	ShardId int
}

type GCReply struct {
	Err Err
}

type GCOp struct {
	CfgNum  int
	ShardId int
}

func (kv *ShardKV) sendGC(shardId, cfgNum int, servers []string) {
	// Debug(dKVGC, "G%d KV%d Begin Send gc to serverlist=%v.", kv.gid, kv.me, servers)
	args := GCArgs{
		CfgNum:  cfgNum,
		ShardId: shardId,
	}

	// try each server for the shard.
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply PushShardReply
		Debug(dKVGC, "G%d KV%d Begin Send gc to server%s.", kv.gid, kv.me, servers[si])
		ok := srv.Call("ShardKV.GC", &args, &reply)
		if ok && (reply.Err == OK) {
			return
		}
	}
}

func (kv *ShardKV) checkGC(shardId, cfgNum int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfgNum != kv.currentConfig.Num {
		Debug(dKVGC, "G%d KV%d cfgnum not match, arg.cfgnum=%d, curnum=%d.", kv.gid, kv.me, cfgNum, kv.currentConfig.Num)
		return false
	}
	if _, ok := kv.Shards[shardId]; ok {
		return true
	}
	Debug(dKVGC, "G%d KV%d cannot find shardId=%d, shard=%v.", kv.gid, kv.me, shardId, kv.Shards)
	return false
}

func (kv *ShardKV) GC(args *GCArgs, reply *GCReply) {
	defer kv.PrintRetState(dKVGC, &reply.Err)

	// First check
	if !kv.checkGC(args.ShardId, args.CfgNum) {
		reply.Err = ErrWrongGroup
		return
	}

	data := GCOp{
		CfgNum:  args.CfgNum,
		ShardId: args.ShardId,
	}
	op := Op{
		OpType: GCOpType,
		Data:   data,
	}

	Debug(dKVGC, "G%d KV%d in group=%d num=%d, Start GC log in shardId=%d.", kv.gid, kv.me, kv.gid, kv.currentConfig.Num, args.ShardId)
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
	kv.resChs.Delete(index)
	//Debug(dKVGet, "G%d KV%d ret Get. key=%s, value=%s", kv.gid, kv.me, data.Key, data.Value)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
}
