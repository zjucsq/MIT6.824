package raft

import (
	"log"
	"math/rand"
	"time"
)

// For state change
func (rf *Raft) ToFollower(term int, reason StateChangeReason) {
	if reason != CandidateDiscoverHigherTerm && reason != LeaderDiscoverHigherTerm && reason != FollowerDiscoverHigherTerm {
		log.Fatalln("ToFollower wrong")
	}
	DebugToFollower(rf, term)
	rf.state = Follower
	rf.currentTerm = term
	Fill(&rf.receiveVote, 0)
	rf.receiveVoteNum = 0
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) ToCandidate(reason StateChangeReason) {
	if reason != FollowerTimeout && reason != CandidateTimeout {
		log.Fatalln("ToCandidate wrong")
	}
	DebugToCandidate(rf)
	rf.state = Candidate
	rf.currentTerm += 1
	Fill(&rf.receiveVote, 0)
	rf.receiveVote[rf.me] = 1
	rf.receiveVoteNum = 1
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) ToLeader(term int, reason StateChangeReason) {
	if reason != CandidateReceiveMajor {
		log.Fatalln("ToLeader wrong")
	}
	DebugToLeader(rf.me, term, rf.receiveVoteNum)
	rf.state = Leader
	rf.currentTerm = term
	rf.appendId = 0
	Fill(&rf.receiveAppendId, 0)
	Fill(&rf.matchIndex, 0)
	Fill(&rf.nextIndex, rf.GetLastIndex()+1)
	// Send heartbeat immediately
	rf.SetHeartBeatSendTime(true)
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

func (rf *Raft) SetHeartBeatSendTime(IsNow bool) {
	if !IsNow {
		t := APPEND_SEND_TIME
		rf.heartBeatSendTime = time.Now().Add(time.Duration(t) * time.Millisecond)
	} else {
		rf.heartBeatSendTime = time.Now()
	}
}

func Fill(array *[]int, num int) {
	for i := range *array {
		(*array)[i] = num
	}
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

// get the Index of index
// compute the location in log and return the result
func (rf *Raft) GetIndexForIndex(index int) int {
	return index - rf.GetFirstIndex()
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
