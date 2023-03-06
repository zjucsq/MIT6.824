package shardkv

import (
	"time"
)

type PushShardArgs struct {
	CfgNum    int
	ShardId   int
	PushShard Shard
}

type PushShardReply struct {
	Err Err
}

type PushShardOp struct {
	CfgNum    int
	ShardId   int
	PushShard Shard
}

func (kv *ShardKV) sendPushShard(shardId, cfgNum int, servers []string, shard Shard) {
	// Call only once, if fail, pushTicker will recall.
	args := PushShardArgs{
		CfgNum:    cfgNum,
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
			//if _, ok2 := kv.Shards[shardId]; ok2 {
			//	delete(kv.Shards, shardId)
			//}
			// kv.Shards[shardId] = Shard{
			// 	Kvmap:  kv.Shards[shardId].Kvmap,
			// 	MaxSeq: kv.Shards[shardId].MaxSeq,
			// 	State:  GCing,
			// }
			// Debug(dKVGC, "G%d KV%d in group=%d num=%d, start GC, shardId=%d, shard=%v, oldconfig=%v, config=%v", kv.gid, kv.me, kv.gid, kv.currentConfig.Num, shardId, kv.Shards[shardId], kv.lastConfig, kv.currentConfig)
			Debug(dKVGC, "G%d KV%d in group=%d num=%d, start GC, shardId=%d, shard=%v", kv.gid, kv.me, kv.gid, kv.currentConfig.Num, shardId, kv.Shards[shardId])
			go kv.sendGC(shardId, cfgNum, kv.lastConfig.Groups[kv.gid])
			kv.mu.Unlock()
			return
		}
	}
}

func (kv *ShardKV) checkPush() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for shardId, shard := range kv.Shards {
		if shard.State == BePulling {
			Debug(dKVPushShard, "G%d KV%d in group=%d num=%d, Start push shardId=%d to group=%d.", kv.gid, kv.me, kv.gid, kv.currentConfig.Num, shardId, kv.currentConfig.Shards[shardId])
			// Debug(dKVPushShard, "now shards=%v", kv.Shards)
			go kv.sendPushShard(shardId, kv.currentConfig.Num, kv.currentConfig.Groups[kv.currentConfig.Shards[shardId]], shard)
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

func (kv *ShardKV) checkPushShard(shardId, cfgNum int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfgNum != kv.currentConfig.Num {
		return false
	}
	if _, ok := kv.Shards[shardId]; ok {
		// if kv.Shards[shardId].State == Pulling {
		// 	return true
		// }
		return true
	}
	return false
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	defer kv.PrintRetState(dKVPushShard, &reply.Err)

	// First check
	if !kv.checkPushShard(args.ShardId, args.CfgNum) {
		reply.Err = ErrWrongGroup
		return
	}

	data := PushShardOp{
		CfgNum:    args.CfgNum,
		ShardId:   args.ShardId,
		PushShard: args.PushShard,
	}
	op := Op{
		OpType: PushChardOpType,
		Data:   data,
	}

	Debug(dKVPushShard, "G%d KV%d Start pushShard in shardId=%d.", kv.gid, kv.me, args.ShardId)
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
