package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	ClientOpType    = "ClientOpType"
	ConfigOpType    = "ConfigOpType"
	PushChardOpType = "PushChardOpType"
)

type OpType string

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

const (
	TIMEOUT         = 100
	ACQUIREINTERVAL = 100
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOutOfDate   = "ErrOutOfDate"
)

type Err string
