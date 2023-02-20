package raft

import (
	"log"
	"math/rand"
	"time"
)

// For state change
func (rf *Raft) ToFollower(term int, reason StateChangeReason) {
	if reason != CandidateDiscoverHigherTerm && reason != LeaderDiscoverHigherTerm && reason != DiscoverHigherTerm {
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
	if reason != FollowTimeout && reason != CandidateTimeout {
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
	Fill(&rf.nextIndex, len(rf.log))
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
