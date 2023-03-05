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
		Debug(dKVPut, "KV%d Put in map. key=%s, value=%s", kv.me, op.Key, op.Value)
	case APPEND:
		kv.Shards[shardId].Kvmap[op.Key] += op.Value
		Debug(dKVAppend, "KV%d Append in map. key=%s, value=%s", kv.me, op.Key, op.Value)
	case GET:
		if value, ok := kv.Shards[shardId].Kvmap[op.Key]; !ok {
			res.Error = ErrNoKey
			// Debug(dKVGet, "KV%d Get in map failed, no key=%s", kv.me, op.Key)
		} else {
			res.Value = value
			// Debug(dKVGet, "KV%d Get in map in applyone. key=%s, value=%s", kv.me, op.Key, res.Value)
		}
	}
	return res
}

func (kv *ShardKV) updateShardStatus() {
	Debug(dKVConfig, "S%d KVServer start update shard status, config=%v, last config=%v", kv.me, kv.currentConfig, kv.lastConfig)
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
				Debug(dKVConfig, "S%d KVServer get new Shard! shardid=%d", kv.me, shardId)
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
				}
			} else {
				if _, ok := kv.Shards[shardId]; ok {
					kv.Shards[shardId] = Shard{
						Kvmap:  kv.Shards[shardId].Kvmap,
						MaxSeq: kv.Shards[shardId].MaxSeq,
						State:  BePulling,
					}
				}
			}
		}
	}
}

func (kv *ShardKV) applyConfig(op ConfigOp) OpResult {
	Debug(dKVConfig, "S%d KVServer start apply configOp=%v, op.NewConfig.Num=%d, kv.currentConfig.Num=%d", kv.me, op, op.NewConfig.Num, kv.currentConfig.Num)
	res := OpResult{
		Error: ErrOutOfDate,
		Value: "",
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.NewConfig.Num > kv.currentConfig.Num {
		kv.lastConfig = kv.currentConfig.DeepcopyConfig()
		kv.currentConfig = op.NewConfig.DeepcopyConfig()
		kv.updateShardStatus()
		res.Error = OK
	}
	return res
}

func (kv *ShardKV) applyPushShard(op PushShardOp) OpResult {
	// Debug(dKVConfig, "S%d KVServer start apply configOp=%v, op.NewConfig.Num=%d, kv.currentConfig.Num=%d", kv.me, op, op.NewConfig.Num, kv.currentConfig.Num)
	res := OpResult{
		Error: ErrOutOfDate,
		Value: "",
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Shards[op.ShardId]; !ok {
		return res
	}
	if kv.Shards[op.ShardId].State != Pulling {
		return res
	}
	kv.Shards[op.ShardId] = op.PushShard.DeepcopyShardWithState(Serving)
	return res
}

func (kv *ShardKV) apply() {
	for cmd := range kv.applyCh {
		if cmd.CommandValid {
			op := cmd.Command.(Op)
			switch op.OpType {
			case ClientOpType:
				clientOp := op.Data.(ClientOp)
				Debug(dSnap, "S%d KVServer receive clientOp! IDX:%d, type:%s, key:%s, value:%s", kv.me, cmd.CommandIndex, clientOp.Type, clientOp.Key, clientOp.Value)
				res := kv.applyClient(clientOp)
				if ch, ok := kv.resChs.Load(cmd.CommandIndex); ok {
					ch.(chan OpResult) <- res
				}
			case ConfigOpType:
				configOp := op.Data.(ConfigOp)
				Debug(dSnap, "S%d KVServer receive configOp! IDX:%d, config=%v", kv.me, cmd.CommandIndex, configOp.NewConfig)
				kv.applyConfig(configOp)
			case PushChardOpType:
				pushShardOp := op.Data.(PushShardOp)
				Debug(dSnap, "S%d KVServer receive pushShardOp! IDX:%d, shard=%v", kv.me, cmd.CommandIndex, pushShardOp.PushShard)
				kv.applyPushShard(pushShardOp)
			}
		} else if cmd.SnapshotValid {
			Debug(dWarn, "Not support snapshot now")
		} else {
			Debug(dWarn, "Unknown command")
		}
	}
}
