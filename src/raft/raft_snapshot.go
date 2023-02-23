package raft

import (
	"bytes"

	"6.824/labgob"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// For simplicity, we do not have offest and done arguments because we can send entire the snapshot at one time.
}

type InstallSnapshotReply struct {
	Term int
}

func SerilizeState(rf *Raft) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		Debug(dError, "S%d Save Persist Error!", rf.me)
		return nil
	}
	return w.Bytes()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(dSnap, "S%d at T%d GetAPP IDX:%d, SnapShot: %v", rf.me, rf.currentTerm, index, snapshot)
	Debug(dSnap, "S%d at T%d before GetAPP, log is %v", rf.me, rf.currentTerm, rf.log)

	if index <= rf.GetFirstIndex() {
		Debug(dError, "S%d Application Set out of data snapshot!, Index: %d, FirstIndex: %d", rf.me, index, rf.GetFirstIndex())
		return
	}

	firstLogIndex := rf.GetFirstIndex()
	lastLogIndex := rf.GetLastIndex()
	old_log := rf.log
	rf.log = make([]LogEntry, lastLogIndex-index+1)
	copy(rf.log, old_log[index-firstLogIndex:])
	// Note we save the dummy log
	rf.log[0].Command = nil

	state_serilize_res := SerilizeState(rf)

	rf.persister.SaveStateAndSnapshot(state_serilize_res, snapshot)
	Debug(dPersist, "S%d Persiste Snapshot Before Index: %d", rf.me, index)
	Debug(dSnap, "S%d at T%d After GetAPP, log is %v", rf.me, rf.currentTerm, rf.log)
}

func (rf *Raft) CallInstallSnapshot(idx, term, leader, lastIncludedIndex, lastIncludedTerm int, data []byte) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          leader,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(idx, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader {
			return
		}

		if reply.Term < rf.currentTerm {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.ToFollower(reply.Term, LeaderDiscoverHigherTerm)
			return
		}

		// check args.Term and curTerm
		if args.Term != rf.currentTerm {
			// out-of-date reply!
			return
		}

		// when reach hear
		// args.Term == rf.currentTerm == reply.Term
		Debug(dSnap, "S%d Receive Snap Reply from S%d, lastIncludedIndex=%d, rf.matchIndex[idx]=%d", rf.me, idx, lastIncludedIndex, rf.matchIndex[idx])
		if lastIncludedIndex > rf.matchIndex[idx] {
			Debug(dTrace, "S%d Change the MIX and NIX of S%d, MID: %d->%d, NIX: %d->%d", rf.me, idx, rf.matchIndex[idx], lastIncludedIndex, rf.nextIndex[idx], lastIncludedIndex+1)
			rf.matchIndex[idx] = lastIncludedIndex
			rf.nextIndex[idx] = lastIncludedIndex + 1
		}

	} else {
		// DebugRpcFail(dVote, "RequestVote")
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// The leader's term is smaller, return directly.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// update the server's term
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		// rf.currentTerm = args.Term
		// rf.persist()
		if rf.state == Leader {
			Debug(dLog, "S%d get reply from S%d: reply.Term=%d, currentTerm=%d in InstallSnapshot", rf.me, args.LeaderId, reply.Term, rf.currentTerm)
			rf.ToFollower(args.Term, LeaderDiscoverHigherTerm)
		} else if rf.state == Candidate {
			rf.ToFollower(args.Term, CandidateDiscoverHigherTerm)
		} else if rf.state == Follower {
			rf.ToFollower(args.Term, FollowerDiscoverHigherTerm)
		}
	}

	rf.SetRandomExpireTime()

	Debug(dSnap, "S%d Receive Snapshot From S%d T%d, LII: %d, LIT:%d, Snap:%v",
		rf.me, args.LeaderId, args.Term,
		args.LastIncludedIndex, args.LastIncludedTerm,
		args.Data)
	Debug(dSnap, "S%d Beform Process, rf.GetFirstIndex()=%d rf.GetLastIndex()=%d Log is: %v ", rf.me, rf.GetFirstIndex(), rf.GetLastIndex(), rf.log)

	// check the snapshot with the peer's snapshot
	curSnapLastIndex := rf.GetFirstIndex()
	curLogLastIndex := rf.GetLastIndex()
	if args.LastIncludedIndex <= curSnapLastIndex {
		// peer's log contain more logs, ignore this rpc
		reply.Term = args.Term
	} else {
		reply.Term = args.Term
		rf.applyMsgCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		}
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		if args.LastIncludedIndex < curLogLastIndex {
			// Follower has extra logs
			old_log := rf.log
			rf.log = make([]LogEntry, curLogLastIndex-args.LastIncludedIndex+1)
			copy(rf.log, old_log[args.LastIncludedIndex-curSnapLastIndex:])
			rf.log[0].Command = nil
			rf.cv.Broadcast()
		} else {
			// add one dummy entry
			rf.log = make([]LogEntry, 0)
			rf.log = append(rf.log, LogEntry{
				Index:   args.LastIncludedIndex,
				Term:    args.LastIncludedTerm,
				Command: nil,
			})
			// rf.commitIndex = args.LastIncludedIndex
		}
		state_serilize_res := SerilizeState(rf)
		rf.persister.SaveStateAndSnapshot(state_serilize_res, args.Data)
		// rf.cv.Broadcast()
	}

	Debug(dSnap, "S%d After Process, Log is: %v lastApplied=%d, commitIndex=%d", rf.me, rf.log, rf.lastApplied, rf.commitIndex)

	return
}
