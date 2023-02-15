package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate RaftState = "Candidate"
	Leader    RaftState = "Leader"
)

const (
	ELECTION_TIMER_RESOLUTION = 5 // check whether timer expire every 5 millisecond.
	ELECTION_EXPIRE_LEFT      = 180
	ELECTION_EXPIRE_RIGHT     = 360
	APPEND_SEND_TIME          = 100 // Minimum time to send information at least once
	//APPEND_EXPIRE_TIME        = 200 // If follower do not receive a heartbeat rpc, it will think the leader is dead.
	APPEND_TIMER_RESOLUTION = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Not mentioned in paper
	// heartBeatExpireTime time.Time // For follower to check if the leader is alive
	cv                *sync.Cond
	state             RaftState
	heartBeatSendTime time.Time // Next times to send heartbeat
	eleExpireTime     time.Time
	receiveVoteNum    int
	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// get the first dummy log index
func (rf *Raft) GetFirstIndex() int {
	return rf.log[0].Index
}

// get the first dummy log term
func (rf *Raft) GetFirstTerm() int {
	return rf.log[0].Term
}

// get the last log term
func (rf *Raft) GetLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// get the last log index
func (rf *Raft) GetLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// get the Term of index
// compute the location in log and return the result
func (rf *Raft) GetTermForIndex(index int) int {
	return rf.log[index-rf.GetFirstIndex()].Term
}

// get the command of index
// compute the location in log and return the result
func (rf *Raft) GetCommand(index int) interface{} {
	return rf.log[index-rf.GetFirstIndex()].Command
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return -1, -1, false
	}
	if rf.state != Leader {
		return -1, -1, false
	}

	index = rf.GetLastIndex() + 1
	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.persist()
	DebugNewCommand(rf)
	rf.nextIndex[rf.me] = index + 1
	//log.Print(index, term, isLeader)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// Your initialization code here (2A, 2B, 2C).
	numServers := len(peers)

	expireTime := GetRandomExpireTime()
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		currentTerm:       0,
		votedFor:          -1,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make([]int, numServers),
		matchIndex:        make([]int, numServers),
		log:               make([]LogEntry, 0),
		state:             Follower,
		heartBeatSendTime: expireTime,
		eleExpireTime:     expireTime,
		//heartBeatExpireTime: expireTime,
	}
	rf.cv = sync.NewCond(&rf.mu)
	// create a dummy log
	rf.log = append(rf.log, LogEntry{
		Index:   0,
		Term:    0,
		Command: nil,
	})

	Fill(&rf.nextIndex, 1)
	//for i := range rf.peers {
	//	rf.heartBeatSendTime[i] = time.Now()
	//}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutines
	go rf.voteTicker()
	go rf.appendTicker()

	// Apply command
	go rf.ApplyCmd(applyCh)

	return rf
}
