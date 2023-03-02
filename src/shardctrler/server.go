package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	// mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs    []Config // indexed by config num
	curConfig  Config
	resChs     sync.Map
	maxSeq     map[int64]int64
	freeShards []int
	gidList    []int // For deterministic map iteration order
	gid2Shards map[int][]int
}

type Op struct {
	// Your data here.
	Type      string
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
	ClientId  int64
	ClientSeq int64
}

type OpResult struct {
	Config Config
	Error  Err
}

func (sc *ShardCtrler) rebalance() {
	if len(sc.curConfig.Groups) == 0 {
		return
	}
	// First use all free shards
	for len(sc.freeShards) > 0 {
		mingid, mingidNum := 0, 20
		for _, gid := range sc.gidList {
			if len(sc.gid2Shards[gid]) < mingidNum {
				mingid = gid
				mingidNum = len(sc.gid2Shards[gid])
			}
		}
		sc.curConfig.Shards[sc.freeShards[0]] = mingid
		sc.gid2Shards[mingid] = append(sc.gid2Shards[mingid], sc.freeShards[0])
		sc.freeShards = sc.freeShards[1:]
	}
	// Then check the difference between max and min shard number a gid has.
	for {
		mingid, mingidNum := -1, 20
		maxgid, maxgidNum := -1, -1
		for _, gid := range sc.gidList {
			if len(sc.gid2Shards[gid]) < mingidNum {
				mingid = gid
				mingidNum = len(sc.gid2Shards[gid])
			}
			if len(sc.gid2Shards[gid]) > maxgidNum {
				maxgid = gid
				maxgidNum = len(sc.gid2Shards[gid])
			}
		}
		if maxgidNum-mingidNum <= 1 {
			break
		}
		sc.curConfig.Shards[sc.gid2Shards[maxgid][0]] = mingid
		sc.gid2Shards[mingid] = append(sc.gid2Shards[mingid], sc.gid2Shards[maxgid][0])
		sc.gid2Shards[maxgid] = sc.gid2Shards[maxgid][1:]
	}
}

func (sc *ShardCtrler) leaveOne(GID int) {
	delete(sc.curConfig.Groups, GID)
	deleteGidPos := -1
	for i, gid := range sc.gidList {
		if gid == GID {
			deleteGidPos = i
		}
	}
	sc.gidList = append(sc.gidList[:deleteGidPos], sc.gidList[deleteGidPos+1:]...)
	sc.freeShards = append(sc.freeShards, sc.gid2Shards[GID]...)
	delete(sc.gid2Shards, GID)
}

func (sc *ShardCtrler) applyOne(op Op) OpResult {
	// Only query rpc need Config
	res := OpResult{
		Error: OK,
	}
	// First check the command is executed or not
	if op.Type != QUERY {
		if maxReq, ok := sc.maxSeq[op.ClientId]; ok {
			if maxReq >= op.ClientSeq {
				return res
			}
		}
		sc.maxSeq[op.ClientId] = op.ClientSeq
	}
	// Then do the command
	switch op.Type {
	case JOIN:
		sc.curConfig = sc.configs[len(sc.configs)-1].deepcopyConfig()
		sc.curConfig.Num++
		for gid, servers := range op.Servers {
			Debug(dSCJoin, "SC%d add gid=%d servers=%v", sc.me, gid, servers)
			sc.curConfig.Groups[gid] = servers
			sc.gidList = append(sc.gidList, gid)
		}
		sort.Ints(sc.gidList)
		Debug(dSCJoin, "SC%d new config=%v", sc.me, sc.curConfig)
		sc.rebalance()
		sc.configs = append(sc.configs, sc.curConfig)
		Debug(dSCJoin, "SC%d join finish new configs=%v", sc.me, sc.configs)
	case LEAVE:
		sc.curConfig = sc.configs[len(sc.configs)-1].deepcopyConfig()
		sc.curConfig.Num++
		for _, GID := range op.GIDs {
			sc.leaveOne(GID)
		}
		sc.rebalance()
		sc.configs = append(sc.configs, sc.curConfig)
	case MOVE:
		sc.curConfig = sc.configs[len(sc.configs)-1].deepcopyConfig()
		sc.curConfig.Num++
		orgGid := sc.curConfig.Shards[op.Shard]
		sc.curConfig.Shards[op.Shard] = op.GID
		sc.gid2Shards[op.GID] = append(sc.gid2Shards[op.GID], op.Shard)
		deleteShardPos := -1
		for i, s := range sc.gid2Shards[orgGid] {
			if s == op.Shard {
				deleteShardPos = i
			}
		}
		sc.gid2Shards[orgGid] = append(sc.gid2Shards[orgGid][:deleteShardPos], sc.gid2Shards[orgGid][deleteShardPos+1:]...)
		sc.rebalance()
		sc.configs = append(sc.configs, sc.curConfig)
	case QUERY:
		if op.Num < len(sc.configs) {
			Debug(dSCQuery, "SC%d now configs=%v, want config id=%d", sc.me, sc.configs, op.Num)
			if op.Num == -1 {
				res.Config = sc.configs[len(sc.configs)-1]
			} else {
				res.Config = sc.configs[op.Num]
			}
			Debug(dSCQuery, "SC%d ret configs=%v", sc.me, res.Config)
		} else {
			Debug(dWarn, "Query out of bounds")
		}
	}
	return res
}

func (sc *ShardCtrler) apply() {
	for cmd := range sc.applyCh {
		if cmd.CommandValid {
			op := cmd.Command.(Op)
			res := sc.applyOne(op)
			if ch, ok := sc.resChs.Load(cmd.CommandIndex); ok {
				ch.(chan OpResult) <- res
			}
		} else if cmd.SnapshotValid {
			Debug(dWarn, "Not support snapshot now")
		} else {
			Debug(dWarn, "Unknown command")
		}
	}
}

func (sc *ShardCtrler) printReturnState(t string, wrongleader bool, err string) {
	Debug(dLogs, "SC%d %s return, wrongleader=%t, err=%s", sc.me, t, wrongleader, err)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	Debug(dSCJoin, "SC%d Start join, servers=%v", sc.me, args.Servers)
	defer sc.printReturnState(JOIN, reply.WrongLeader, string(reply.Err))

	op := Op{
		Type:      JOIN,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		ClientSeq: args.RequestId,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	tmpCh := make(chan OpResult, 1)
	sc.resChs.Store(index, tmpCh)

	select {
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	case res := <-tmpCh:
		reply.Err = res.Error
	}

	sc.resChs.Delete(index)
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	Debug(dSCLeave, "SC%d Start leave, gids=%v", sc.me, args.GIDs)
	defer sc.printReturnState(LEAVE, reply.WrongLeader, string(reply.Err))

	op := Op{
		Type:      LEAVE,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		ClientSeq: args.RequestId,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	tmpCh := make(chan OpResult, 1)
	sc.resChs.Store(index, tmpCh)

	select {
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	case res := <-tmpCh:
		reply.Err = res.Error
	}

	sc.resChs.Delete(index)
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	Debug(dSCMove, "SC%d Start move, gid=%d, shard=%d", sc.me, args.GID, args.Shard)
	defer sc.printReturnState(MOVE, reply.WrongLeader, string(reply.Err))

	op := Op{
		Type:      MOVE,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		ClientSeq: args.RequestId,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	tmpCh := make(chan OpResult, 1)
	sc.resChs.Store(index, tmpCh)

	select {
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	case res := <-tmpCh:
		reply.Err = res.Error
	}

	sc.resChs.Delete(index)
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	Debug(dSCQuery, "SC%d Start query, num=%v", sc.me, args.Num)
	defer sc.printReturnState(QUERY, reply.WrongLeader, string(reply.Err))

	op := Op{
		Type: QUERY,
		Num:  args.Num,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	tmpCh := make(chan OpResult, 1)
	sc.resChs.Store(index, tmpCh)

	select {
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	case res := <-tmpCh:
		reply.Config = res.Config
		reply.Err = res.Error
	}

	sc.resChs.Delete(index)
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.maxSeq = make(map[int64]int64)
	sc.gid2Shards = make(map[int][]int)
	sc.freeShards = make([]int, NShards)
	for i := 0; i < NShards; i++ {
		sc.freeShards[i] = i
	}

	go sc.apply()

	return sc
}
