package shardkv

type Op struct {
	OpType OpType
	Data   interface{}
}

type OpResult struct {
	Error Err
	Value string
}

func (kv *ShardKV) applyClient(op ClientOp) OpResult {
	res := OpResult{
		Error: OK,
		Value: "",
	}
	shardId := key2shard(op.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// First check the command is executed or not
	if op.Type != GET {
		if maxReq, ok := kv.Shards[shardId].MaxSeq[op.ClientId]; ok {
			if maxReq >= op.ClientSeq {
				return res
			}
		}
		kv.Shards[shardId].MaxSeq[op.ClientId] = op.ClientSeq
	}
	// Then do the command
	switch op.Type {
	case PUT:
		kv.Shards[shardId].Kvmap[op.Key] = op.Value
		Debug(dKVPut, "G%d KV%d Put in map. key=%s, value=%s", kv.gid, kv.me, op.Key, op.Value)
	case APPEND:
		kv.Shards[shardId].Kvmap[op.Key] += op.Value
		Debug(dKVAppend, "G%d KV%d Append in map. key=%s, value=%s", kv.gid, kv.me, op.Key, op.Value)
	case GET:
		if value, ok := kv.Shards[shardId].Kvmap[op.Key]; !ok {
			res.Error = ErrNoKey
			// Debug(dKVGet, "G%d KV%d Get in map failed, no key=%s", kv.gid, kv.me, op.Key)
		} else {
			res.Value = value
			// Debug(dKVGet, "G%d KV%d Get in map in applyone. key=%s, value=%s", kv.gid, kv.me, op.Key, res.Value)
		}
	}
	return res
}

func (kv *ShardKV) updateShardStatus() {
	Debug(dKVConfig, "G%d KV%d KVServer start update shard status, config=%v, last config=%v", kv.gid, kv.me, kv.currentConfig, kv.lastConfig)
	if kv.lastConfig.Num == 0 {
		// For init, add directly
		for shardId, gid := range kv.currentConfig.Shards {
			// Debug(dKVConfig, "gid=%d kv.gid=%d", gid, kv.gid)
			if gid == kv.gid {
				kv.Shards[shardId] = Shard{
					Kvmap:  make(map[string]string),
					MaxSeq: make(map[int64]int64),
					State:  Serving,
				}
				Debug(dKVConfig, "G%d KV%d KVServer get new Shard! shardid=%d", kv.gid, kv.me, shardId)
			}
		}
	} else {
		for shardId, gid := range kv.currentConfig.Shards {
			if gid == kv.gid {
				// Shard shardId will be in this ShardKV
				if _, ok := kv.Shards[shardId]; !ok {
					// If Shard shardId is not in this ShardKV now, we set its state to pulling
					kv.Shards[shardId] = Shard{
						Kvmap:  make(map[string]string),
						MaxSeq: make(map[int64]int64),
						State:  Pulling,
					}
					Debug(dKVConfig, "G%d KV%d KVServer set shardid=%d state to Pulling shard=%v", kv.gid, kv.me, shardId, kv.Shards)
				}
			} else {
				if _, ok := kv.Shards[shardId]; ok {
					kv.Shards[shardId] = Shard{
						Kvmap:  kv.Shards[shardId].Kvmap,
						MaxSeq: kv.Shards[shardId].MaxSeq,
						State:  BePulling,
					}
					Debug(dKVConfig, "G%d KV%d KVServer set shardid=%d state to BePulling shard=%v", kv.gid, kv.me, shardId, kv.Shards)
				}
			}
		}
	}
}

func (kv *ShardKV) applyConfig(op ConfigOp) OpResult {
	res := OpResult{
		Error: ErrOutOfDate,
		Value: "",
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.NewConfig.Num > kv.currentConfig.Num {
		Debug(dKVConfig, "G%d KV%d KVServer start apply configOp=%v, op.NewConfig.Num=%d, kv.currentConfig.Num=%d", kv.gid, kv.me, op, op.NewConfig.Num, kv.currentConfig.Num)
		kv.lastConfig = kv.currentConfig.DeepcopyConfig()
		kv.currentConfig = op.NewConfig.DeepcopyConfig()
		kv.updateShardStatus()
		res.Error = OK
	}
	return res
}

func (kv *ShardKV) applyPushShard(op PushShardOp) OpResult {
	res := OpResult{
		Error: ErrOutOfDate,
		Value: "",
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.CfgNum != kv.currentConfig.Num {
		return res
	}
	// if _, ok := kv.Shards[op.ShardId]; !ok {
	// 	return res
	// }
	// if kv.Shards[op.ShardId].State != Pulling {
	// 	return res
	// }
	Debug(dKVPushShard, "G%d KV%d KVServer start apply PushShardOp shardId=%d, shard=%v", kv.gid, kv.me, op.ShardId, op.PushShard)
	kv.Shards[op.ShardId] = op.PushShard.DeepcopyShardWithState(Serving)
	res.Error = OK
	Debug(dKVPushShard, "G%d KV%d KVServer after apply PushShardOp shard=%v", kv.gid, kv.me, kv.Shards)
	return res
}

func (kv *ShardKV) applyGC(op GCOp) OpResult {
	res := OpResult{
		Error: ErrOutOfDate,
		Value: "",
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.CfgNum != kv.currentConfig.Num {
		return res
	}
	if _, ok := kv.Shards[op.ShardId]; !ok {
		return res
	}
	// if kv.Shards[op.ShardId].State != BePulling {
	// 	return res
	// }
	Debug(dKVPushShard, "G%d KV%d KVServer start apply GCOp shardId=%d, shard=%v", kv.gid, kv.me, op.ShardId, kv.Shards[op.ShardId])
	delete(kv.Shards, op.ShardId)
	Debug(dKVPushShard, "G%d KV%d KVServer after apply GCOp shard=%v", kv.gid, kv.me, kv.Shards)
	res.Error = OK
	return res
}

func (kv *ShardKV) apply() {
	for cmd := range kv.applyCh {
		if cmd.CommandValid {
			op := cmd.Command.(Op)
			switch op.OpType {
			case ClientOpType:
				clientOp := op.Data.(ClientOp)
				Debug(dKVClient, "G%d KV%d KVServer receive clientOp! IDX:%d, type:%s, key:%s, value:%s", kv.gid, kv.me, cmd.CommandIndex, clientOp.Type, clientOp.Key, clientOp.Value)
				res := kv.applyClient(clientOp)
				if ch, ok := kv.resChs.Load(cmd.CommandIndex); ok {
					ch.(chan OpResult) <- res
				}
			case ConfigOpType:
				configOp := op.Data.(ConfigOp)
				Debug(dKVConfig, "G%d KV%d KVServer receive configOp! IDX:%d, config=%v", kv.gid, kv.me, cmd.CommandIndex, configOp.NewConfig)
				kv.applyConfig(configOp)
			case PushChardOpType:
				pushShardOp := op.Data.(PushShardOp)
				Debug(dKVPushShard, "G%d KV%d KVServer receive pushShardOp! IDX:%d, shard=%v", kv.gid, kv.me, cmd.CommandIndex, pushShardOp.PushShard)
				res := kv.applyPushShard(pushShardOp)
				if ch, ok := kv.resChs.Load(cmd.CommandIndex); ok {
					ch.(chan OpResult) <- res
				}
			case GCOpType:
				gcOp := op.Data.(GCOp)
				Debug(dKVPushShard, "G%d KV%d KVServer receive gcOp! IDX:%d, op=%v, config=%v", kv.gid, kv.me, cmd.CommandIndex, gcOp, kv.currentConfig)
				res := kv.applyGC(gcOp)
				if ch, ok := kv.resChs.Load(cmd.CommandIndex); ok {
					ch.(chan OpResult) <- res
				}
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= 6*kv.maxraftstate {
				snapshot := kv.makeSnapshot()
				go kv.rf.Snapshot(cmd.CommandIndex, snapshot)
				Debug(dSnap, "G%d KV%d KVServer Create Snapshot! IDX:%d, Snapshot:%v", kv.gid, kv.me, cmd.CommandIndex, snapshot)
			}
		} else if cmd.SnapshotValid {
			Debug(dSnap, "G%d KV%d KVServer receive Snapshot!", kv.me)
			kv.applySnapshot(cmd.Snapshot)
			Debug(dSnap, "G%d KV%d KVServer receive Snapshot! now log length=%d ", kv.gid, kv.me, kv.persister.RaftStateSize())
		} else {
			Debug(dWarn, "Unknown command")
		}
	}
}
