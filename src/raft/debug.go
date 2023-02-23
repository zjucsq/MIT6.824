package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
	// dApply   logTopic = "APPL"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// send vote
func DebugGrantVote(s1, s2, term int) {
	Debug(dVote, "S%d -> S%d at T%d", s1, s2, term)
}

// receive vote
func DebugGetVote(s1, s2, term int) {
	Debug(dVote, "S%d <- S%d at T%d", s1, s2, term)
}

// become to leader
func DebugToLeader(s, term, num int) {
	Debug(dLeader, "S%d Receive Majority for T%d (%d), converting to Leader", s, term, num)
}

// become to follower
func DebugToCandidate(rf *Raft) {
	Debug(dTrace, "S%d Change State From [%s:%d] To [C]", rf.me, rf.state, rf.currentTerm)
}

// become to follower
func DebugToFollower(rf *Raft, new_term int) {
	Debug(dTrace, "S%d Change State From [%s:%d] To [F:%d]", rf.me, rf.state, rf.currentTerm, new_term)
}

// election timeout
func DebugELT(s, term int, t time.Time) {
	Debug(dTimer, "S%d Election Timeout, Begin Election for T%d, Eleexpire time = %s", s, term, t)
}

//func DebugResetHBT(rf *Raft, idx int) {
//	Debug(dTimer, "S%d at T%d Reset HBT to %06d", rf.me, rf.currentTerm, rf.AppendExpireTime[idx].Sub(debugStart).Microseconds()/100)
//}

func DebugNewCommand(rf *Raft) {
	Debug(dLeader, "S%d T%d Roler: %s Receive New Command, Now Leader Log:%v", rf.me, rf.currentTerm, rf.state, rf.log)
}

//
//func DebugAppendEntriesMismatch(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	Debug(dLog, "S%d at T%d Reply AppendEntries From S%d at T%d With [OK: %v T:%v CFT: %d CFI: %d]", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, reply.Term, reply.ConflictTerm, reply.ConflictIndex)
//	Debug(dLog, "S%d at T%d After Reply AppendEntries. CI:%d, log is - %v", rf.me, rf.currentTerm, rf.commitIndex, rf.log)
//}

//func DebugAfterReceiveAppendEntries(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	Debug(dLog, "S%d at T%d Reply AppendEntries From S%d at T%d With [OK: %v T:%v CFT: %d CFI: %d]", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, reply.Term, reply.ConflictTerm, reply.ConflictIndex)
//	Debug(dLog, "S%d at T%d After Reply AppendEntries. CI:%d, log is - %v", rf.me, rf.currentTerm, rf.commitIndex, rf.log)
//}

// rpc fail
func DebugRpcFail(topic logTopic, rpcname string) {
	Debug(topic, "%s rpc failed", rpcname)
}
