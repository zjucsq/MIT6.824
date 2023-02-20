package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term          int // leader's term
	LeaderId      int // leader's id
	PrevLogIndex  int
	PrevTermIndex int
	Entries       []LogEntry
	LeaderCommit  int // leader’s commitIndex
	AppendId      int
}

type AppendEntriesReply struct {
	Term    int  // currentTerm (for follower), for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	// The following three member only valid when Sucess == false
	XTerm  int // conflict log's term, if no log at all, set -1
	XIndex int // For term XTerm, the first index
	XLen   int // if XTerm = -1, return len(log)
	// For check out-of-date
	AppendId int
}

func (rf *Raft) appendTicker() {
	for rf.killed() == false {

		time.Sleep(APPEND_TIMER_RESOLUTION * time.Millisecond)
		// time.Sleep(APPEND_SEND_TIME * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			if time.Now().After(rf.heartBeatSendTime) {
				rf.SetHeartBeatSendTime(false)
				rf.appendId += 1
				for i := range rf.peers {
					if i != rf.me {
						if rf.nextIndex[i] < len(rf.log) {
							Debug(dLog, "S%d -> S%d: send log from I%dT%d to I%dT%d, prevLogIndex=%d, prevLogTerm=%d, leaderCommit=%d",
								rf.me, i, rf.nextIndex[i], rf.log[rf.nextIndex[i]].Term, len(rf.log)-1, rf.log[len(rf.log)-1].Term, rf.nextIndex[i]-1,
								rf.log[rf.nextIndex[i]-1].Term, rf.commitIndex)
						} else {
							//Debug(dLog, "S%d -> S%d: T%d send empty log %s, prevLogIndex=%d, prevLogTerm=%d, leaderCommit=%d",
							//	rf.me, i, rf.currentTerm, rf.log[rf.nextIndex[i]:], rf.nextIndex[i]-1,
							//	rf.log[rf.nextIndex[i]-1].Term, rf.commitIndex)
						}
						logs := make([]LogEntry, rf.GetLastIndex()-rf.nextIndex[i]+1)
						copy(logs, rf.log[rf.nextIndex[i]-rf.GetFirstIndex():])
						go rf.CallAppendEntries(i, rf.currentTerm, rf.me, rf.nextIndex[i]-1,
							rf.log[rf.nextIndex[i]-1].Term, rf.commitIndex, rf.appendId, logs)
					}
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) CallAppendEntries(idx, term, leader, prevLogIndex, prevLogTerm, leaderCommit, appendId int, logs []LogEntry) {
	args := AppendEntriesArgs{
		Term:          term,
		LeaderId:      leader,
		PrevTermIndex: prevLogTerm,
		PrevLogIndex:  prevLogIndex,
		Entries:       logs,
		LeaderCommit:  leaderCommit,
		AppendId:      appendId,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(idx, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader {
			return
		}

		if rf.receiveAppendId[idx] < reply.AppendId {
			rf.receiveAppendId[idx] = reply.AppendId
			if !reply.Success {
				// Debug(dLog, "S%d, reply.Term%d, currentTerm%d", rf.me, reply.Term, rf.currentTerm)
				if reply.Term > rf.currentTerm {
					// Find other server has a bigger term
					rf.ToFollower(reply.Term, LeaderDiscoverHigherTerm)
				} else {
					// In theory, the heartbeat packet(len(logs) == 0) will not go into this branch
					// log mismatch
					if reply.XTerm == -1 {
						rf.nextIndex[idx] = reply.XLen
					} else {
						// three situations
						// S1 455	444		4
						// S2 4666	4666	4666
						// Find the first index of XTerm in the leader
						newNextIndex := reply.XIndex
						for rf.log[newNextIndex].Term == reply.XTerm {
							newNextIndex++
						}
						rf.nextIndex[idx] = newNextIndex
						//findIdx := -1
						//for i := rf.GetLastIndex() + 1; i > rf.GetFirstIndex(); i-- {
						//	if rf.GetTermForIndex(i-1) == reply.XTerm {
						//		findIdx = i
						//		break
						//	}
						//}
						//if findIdx != -1 {
						//	rf.nextIndex[idx] = findIdx
						//} else {
						//	rf.nextIndex[idx] = reply.XIndex
						//}
					}
					// rf.nextIndex[idx] -= 1
				}
			} else {
				// append successfully
				// update nextIndex
				// For duplicated rpc call, check if the response is out-of-date or not, throw the out-of-date reply.
				if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[idx] {
					rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[idx] = rf.matchIndex[idx] + 1
					//rf.nextIndex[idx] = rf.nextIndex[idx] + len(logs)
					Debug(dLog, "S%d receive from S%d, PrevLogIndex=%d, len_entities=%d, new nextIndex[idx]=%d", rf.me, idx, args.PrevLogIndex, len(args.Entries), rf.nextIndex[idx])
					//rf.matchIndex[idx] = rf.nextIndex[idx] - 1
					// update commitIdx
					// leader can only commit log in its currentTerm
					if rf.log[rf.matchIndex[idx]].Term == rf.currentTerm {
						newCommitIndex := rf.matchIndex[idx]
						if newCommitIndex > rf.commitIndex {
							cnt := 0
							for _, v := range rf.nextIndex {
								if v > newCommitIndex {
									cnt += 1
								}
							}
							if cnt > len(rf.peers)/2 {
								Debug(dCommit, "%d follow recieved, update leader S%d commitIndex I%d -> I%d", cnt, rf.me, rf.commitIndex, newCommitIndex)
								rf.commitIndex = newCommitIndex
								rf.cv.Signal()
							}
						}
					}
				}
			}
		}
	} else {
		// DebugRpcFail(dVote, "RequestVote")
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(dInfo, "S%d receive append entries log = %s commitid = %d", rf.me, args.Entries, args.LeaderCommit)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.AppendId = args.AppendId
	// The leader's term is smaller, return directly.
	if args.Term < rf.currentTerm {
		//Debug(dLog, "S%d, append fail", rf.me)
		return
	}

	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.ToFollower(args.Term, CandidateDiscoverHigherTerm)
	}

	// update the server's term
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		// rf.currentTerm = args.Term
		// rf.persist()
		rf.ToFollower(args.Term, DiscoverHigherTerm)
	}

	//if rf.state == Leader {
	//	return
	//}

	// Leader is right, so we reset expire time.
	// rf.SetHeartBeatExpireTime()
	rf.SetRandomExpireTime()

	// Check if the logs is match?
	// Note: For heartbeat packet, we still need to check if it is matched.
	// If args.PrevLogIndex > rf.GetLastIndex(), there must be a vacancy in the log.
	// If rf.log[args.PrevLogIndex].Term != args.PrevTermIndex, the leader think its log must be right, so rf.log[args.PrevLogIndex] in the follower must be wrong, the follow need more logs.
	// If args.PrevLogIndex < rf.GetLastIndex() and rf.log[args.PrevLogIndex].Term == args.PrevTermIndex, we can overwrite directly. (This maybe duplicate rpc call)
	if args.PrevLogIndex > rf.GetLastIndex() || rf.log[args.PrevLogIndex].Term != args.PrevTermIndex {
		// three situations
		// S1 455	444		4
		// S2 4666	4666	4666  send 6 prevIndex = 3, prevTerm = 6
		if len(rf.log) <= args.PrevLogIndex {
			reply.XTerm = -1
			reply.XLen = len(rf.log)
		} else {
			// Return first index of the conflict term in the follower
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
					reply.XIndex = i + 1
					break
				}
			}
		}
		Debug(dLog, "S%d get last log: [I%d, T%d], expected last log: [I%d, T%d], return XTerm=%d, XIndex=%d, XLog=%d",
			rf.me, args.PrevLogIndex, args.PrevTermIndex, rf.GetLastIndex(), rf.GetLastTerm(), reply.XTerm, reply.XIndex, reply.XLen)
		if args.PrevLogIndex <= rf.GetLastIndex() {
			Debug(dLog, "term in I%d = T%d,", args.PrevLogIndex, rf.log[args.PrevLogIndex].Term)
		}
		return
	}

	// Append success
	reply.Success = true
	//Debug(dLog, "args.PrevLogIndex=%d rf.GetLastIndex()=%d rf.log[args.PrevLogIndex].Term=%d args.PrevTermIndex=%d", args.PrevLogIndex, rf.GetLastIndex(), rf.log[args.PrevLogIndex].Term, args.PrevTermIndex)
	//Debug(dLog, "S%d, append true, len(log) = %d", rf.me, len(args.Entries))

	// If args.PrevLogIndex < rf.GetLastIndex() and rf.log[args.PrevLogIndex].Term == args.PrevTermIndex, we just overwrite directly.
	// two situations: 1) duplicated rpc call; 2) TestFailNoAgree2B: too many followers disconnect, remain alive servers will have unwanted uncommitted logs.
	// Note here rf.log[args.PrevLogIndex].Term == args.PrevTermIndex must be satisfied.
	if len(args.Entries) > 0 {
		// Debug(dClient, "S%d T%d Roler: %s will append logs, before append Log:%v, append:%v", rf.me, rf.currentTerm, rf.state, rf.log, args.Entries)
		index := args.PrevLogIndex
		for i, entry := range args.Entries {
			index++
			if index < len(rf.log) {
				// here index is always same, so we do not need to check.
				if rf.log[index].Term == entry.Term {
					continue
				}
				// rf.log[index] is the first log that not same as the leader.
				Debug(dLog, "S%d cut logs that do not match leader%d: [(I%d,T%d)-(I%d,T%d)] success",
					rf.me, args.LeaderId, rf.log[index].Index, rf.log[index].Term, rf.GetLastIndex(), rf.GetLastTerm())
				rf.log = rf.log[:index]
			}
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			Debug(dLog, "S%d append log: [(I%d,T%d)-(I%d,T%d)] success",
				rf.me, args.Entries[i].Index, args.Entries[i].Term, rf.GetLastIndex(), rf.GetLastTerm())
		}

		//maybe wrong implementation
		//keepIndex := len(rf.log) - 1
		//for ; keepIndex > rf.commitIndex; keepIndex-- {
		//	if args.PrevLogIndex == rf.log[keepIndex].Index {
		//		break
		//	}
		//}
		//rf.log = rf.log[:keepIndex+1]
		//rf.log = append(rf.log, args.Entries...)
	}

	// Update commit index
	if rf.commitIndex < args.LeaderCommit {
		// log.Print(rf.commitIndex, args.LeaderCommit)
		oldCommitIdx := rf.commitIndex
		lastIndex := rf.GetLastIndex()
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		Debug(dCommit, "update S%d commitIndex I%d -> I%d", rf.me, oldCommitIdx, rf.commitIndex)
		rf.cv.Signal()
	}

	return
}
