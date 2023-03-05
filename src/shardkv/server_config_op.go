package shardkv

import (
	"6.824/shardctrler"
	"time"
)

type ConfigOp struct {
	NewConfig shardctrler.Config
}

func (kv *ShardKV) checkNewConfig(num int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return num > kv.currentConfig.Num
}

func (kv *ShardKV) acquireConfig() {
	for {
		time.Sleep(ACQUIREINTERVAL * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
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
