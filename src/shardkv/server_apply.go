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
	// First check the command is executed or not
	if op.Type != GET {
		if maxReq, ok := kv.maxSeq[op.ClientId]; ok {
			if maxReq >= op.ClientSeq {
				return res
			}
		}
		kv.maxSeq[op.ClientId] = op.ClientSeq
	}
	// Then do the command
	shardId := key2shard(op.Key)
	switch op.Type {
	case PUT:
		kv.Shards[shardId].kvmap[op.Key] = op.Value
		Debug(dKVPut, "KV%d Put in map. key=%s, value=%s", kv.me, op.Key, op.Value)
	case APPEND:
		kv.Shards[shardId].kvmap[op.Key] += op.Value
		Debug(dKVAppend, "KV%d Append in map. key=%s, value=%s", kv.me, op.Key, op.Value)
	case GET:
		if value, ok := kv.Shards[shardId].kvmap[op.Key]; !ok {
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
					kvmap: make(map[string]string),
					state: Serving,
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
						kvmap: make(map[string]string),
						state: Pulling,
					}
				}
			}
		}
		// Not set BePulling state now, maybe it will be needed later
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
			}
		} else if cmd.SnapshotValid {
			Debug(dWarn, "Not support snapshot now")
		} else {
			Debug(dWarn, "Unknown command")
		}
	}
}
