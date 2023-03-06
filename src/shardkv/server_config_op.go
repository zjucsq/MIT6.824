package shardkv

import (
	"time"

	"6.824/shardctrler"
)

type ConfigOp struct {
	NewConfig shardctrler.Config
}

func (kv *ShardKV) checkNewConfig(num int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return num > kv.currentConfig.Num
}

func (kv *ShardKV) checkConfigMatch() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shard := range kv.Shards {
		if shard.State == BePulling {
			return false
		}
	}
	// for shardId, gid := range kv.currentConfig.Shards {
	// 	if gid == kv.gid {
	// 		// Shard shardId should be in this ShardKV's Shards
	// 		if _, ok := kv.Shards[shardId]; !ok {
	// 			return false
	// 		} else {
	// 			if kv.Shards[shardId].State != Serving {
	// 				return false
	// 			}
	// 		}
	// 	} else {
	// 		// Shard shardId should not be in this ShardKV's Shards
	// 		if _, ok := kv.Shards[shardId]; ok {
	// 			return false
	// 		}
	// 	}
	// }
	return true
}

func (kv *ShardKV) acquireConfig() {
	for {
		time.Sleep(ACQUIREINTERVAL * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		if !kv.checkConfigMatch() {
			continue
		}
		// Debug(dKVConfig, "S%d KVServer try acquire new config!", kv.me)
		newConfig := kv.sc.Query(-1)
		if !kv.checkNewConfig(newConfig.Num) {
			continue
		}
		Debug(dKVConfig, "S%d KVServer acquire new config! config=%v", kv.me, newConfig)
		data := ConfigOp{
			NewConfig: newConfig,
		}
		op := Op{
			OpType: ConfigOpType,
			Data:   data,
		}
		kv.rf.Start(op)
	}
}
