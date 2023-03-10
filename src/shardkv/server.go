package shardkv

import (
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	Serving   = "Serving"
	Pulling   = "Pulling"
	BePulling = "BePulling"
	GCing     = "GCing"
)

type ShardState string

type Shard struct {
	Kvmap  map[string]string
	MaxSeq map[int64]int64
	State  ShardState
}

func (src *Shard) DeepcopyShardWithState(state ShardState) Shard {
	dst := Shard{
		Kvmap:  make(map[string]string),
		MaxSeq: make(map[int64]int64),
		State:  state,
	}
	for k, v := range src.Kvmap {
		dst.Kvmap[k] = v
	}
	for k, v := range src.MaxSeq {
		dst.MaxSeq[k] = v
	}
	return dst
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister     *raft.Persister
	Shards        map[int]Shard
	resChs        sync.Map
	sc            *shardctrler.Clerk
	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft State along with the snapshot.
//
// the k/v server should snapshot when Raft's saved State exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ClientOp{})
	labgob.Register(ConfigOp{})
	labgob.Register(PushShardOp{})
	labgob.Register(GCOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.Shards = make(map[int]Shard)
	//kv.MaxSeq = make(map[int64]int64)
	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.lastConfig = shardctrler.Config{
		Num:    0,
		Groups: make(map[int][]string),
	}
	kv.currentConfig = shardctrler.Config{
		Num:    0,
		Groups: make(map[int][]string),
	}
	snapshot := persister.ReadSnapshot()
	kv.applySnapshot(snapshot)
	kv.persister = persister

	// apply cmd to kvserver
	//for i := 0; i < shardctrler.NShards; i++ {
	//	kv.Shards[i] = Shard{
	//		Kvmap: make(map[string]string),
	//		State: Serving,
	//	}
	//}
	go kv.acquireConfig()
	go kv.apply()
	go kv.pushTicker()

	return kv
}
