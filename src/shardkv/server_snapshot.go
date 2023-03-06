package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/shardctrler"
)

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.Shards) != nil || e.Encode(kv.currentConfig) != nil || e.Encode(kv.lastConfig) != nil {
		Debug(dError, "G%d KV%d Save Persist Error!", kv.me)
		return nil
	}
	return w.Bytes()
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards map[int]Shard
	var currentConfig shardctrler.Config
	var lastConfig shardctrler.Config
	if d.Decode(&shards) != nil || d.Decode(&currentConfig) != nil || d.Decode(&lastConfig) != nil {
		Debug(dError, "G%d KV%d KVServer Read Persist Error!", kv.me)
	} else {
		kv.mu.Lock()
		kv.Shards = shards
		kv.currentConfig = currentConfig
		kv.lastConfig = lastConfig
		kv.mu.Unlock()
		Debug(dPersist, "G%d KV%d KVServer ReadPersist.", kv.me)
	}
}
