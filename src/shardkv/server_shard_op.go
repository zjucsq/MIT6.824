package shardkv

import (
	"time"
)

type PushShardArgs struct {
	ShardId   int
	PushShard Shard
}

type PushShardReply struct {
	Err Err
}

type PushShardOp struct {
	ShardId   int
	PushShard Shard
}

func (kv *ShardKV) sendPushShard(shardId int, servers []string, shard Shard) {
	// Call only once, if fail, pushTicker will recall.
	args := PushShardArgs{
		ShardId:   shardId,
		PushShard: shard,
	}
	// try each server for the shard.
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply PushShardReply
		ok := srv.Call("ShardKV.PushShard", &args, &reply)
		if ok && (reply.Err == OK) {
			kv.mu.Lock()
			delete(kv.currentConfig.Groups, shardId)
			kv.mu.Unlock()
			return
		}
	}
}

func (kv *ShardKV) checkPush() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Debug(dKVPushShard, "KV%d Start check push.", kv.me)
	for shardId, shard := range kv.Shards {
		if shard.State == BePulling {
			go kv.sendPushShard(shardId, kv.currentConfig.Groups[kv.currentConfig.Shards[shardId]], shard)
		}
	}
}

func (kv *ShardKV) pushTicker() {
	for {
		time.Sleep(2 * ACQUIREINTERVAL * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.checkPush()
	}
}

func (kv *ShardKV) checkPushShard(shardId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Shards[shardId]; ok {
		if kv.Shards[shardId].State == Pulling {
			return true
		}
	}
	return false
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	defer kv.PrintRetState(dKVPushShard, &reply.Err)

	// First check
	if !kv.checkPushShard(args.ShardId) {
		reply.Err = ErrWrongGroup
		return
	}

	data := PushShardOp{
		ShardId:   args.ShardId,
		PushShard: args.PushShard,
	}
	op := Op{
		OpType: PushChardOpType,
		Data:   data,
	}

	Debug(dKVPushShard, "KV%d Start pushShard in shardId=%d.", kv.me, args.ShardId)
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
	//Debug(dKVGet, "KV%d ret Get. key=%s, value=%s", kv.me, data.Key, data.Value)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
}
