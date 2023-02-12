package raft

import (
	"math/rand"
	"time"
)

// For state change
func (rf *Raft) ToFollower(term int) {
	DebugToFollower(rf, term)
	rf.state = Follower
	rf.currentTerm = term
	rf.receiveVoteNum = 0
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) ToCandidate() {
	DebugToCandidate(rf)
	rf.state = Candidate
	rf.receiveVoteNum = 1
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) ToLeader(term int) {
	DebugToLeader(rf.me, term, rf.receiveVoteNum)
	rf.state = Leader
	rf.currentTerm = term
	Fill(&rf.matchIndex, 0)
	Fill(&rf.nextIndex, len(rf.log))
	// go rf.Start(nil)
}

// Calculate random expire time
func GetRandomExpireTime() time.Time {
	t := rand.Intn(ELECTION_EXPIRE_RIGHT - ELECTION_EXPIRE_LEFT)
	return time.Now().Add(time.Duration(t+ELECTION_EXPIRE_LEFT) * time.Millisecond)
}

func (rf *Raft) SetRandomExpireTime() {
	t := rand.Intn(ELECTION_EXPIRE_RIGHT - ELECTION_EXPIRE_LEFT)
	rf.eleExpireTime = time.Now().Add(time.Duration(t+ELECTION_EXPIRE_LEFT) * time.Millisecond)
}

//func (rf *Raft) SetHeartBeatExpireTime() {
//	t := APPEND_EXPIRE_TIME
//	rf.heartBeatExpireTime = time.Now().Add(time.Duration(t) * time.Millisecond)
//}

func (rf *Raft) SetHeartBeatSendTime() {
	t := APPEND_SEND_TIME
	rf.heartBeatSendTime = time.Now().Add(time.Duration(t) * time.Millisecond)
}

func Fill(array *[]int, num int) {
	for i := range *array {
		(*array)[i] = num
	}
}
