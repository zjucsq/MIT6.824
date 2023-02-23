package raft

import "time"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate's id
	LastLogIndex int // candidate's last log index
	LastLogTerm  int // candidate's last log term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm (for follower), for leader to update itself
	VoteGranted bool // true means candidate received vote
	LeaderTerm  int  // vote for which term's leader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) voteTicker() {
	for !rf.killed() {

		time.Sleep(ELECTION_TIMER_RESOLUTION * time.Millisecond)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state != Leader {
			if time.Now().After(rf.eleExpireTime) {
				rf.startVote()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startVote() {
	rf.SetRandomExpireTime()
	if rf.state == Follower {
		rf.ToCandidate(FollowerTimeout)
	} else if rf.state == Candidate {
		rf.ToCandidate(CandidateTimeout)
	}
	DebugELT(rf.me, rf.currentTerm, rf.eleExpireTime)
	for i := range rf.peers {
		if i != rf.me {
			go rf.CallForVote(i, rf.currentTerm, rf.me, rf.GetLastIndex(), rf.GetLastTerm())
		}
	}
}

func (rf *Raft) CallForVote(idx, term, candidate, lastIndex, lastTerm int) {
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  candidate,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(idx, &args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// Throw out-of-date reply
		if rf.currentTerm == reply.LeaderTerm && rf.state == Candidate {
			if reply.Term > rf.currentTerm {
				rf.ToFollower(reply.Term, CandidateDiscoverHigherTerm)
				// rf.SetRandomExpireTime()
			} else {
				if reply.VoteGranted {
					// First check whether already receive the idx's vote.
					if rf.receiveVote[idx] == 0 {
						rf.receiveVote[idx] = 1
						DebugGetVote(rf.me, idx, term)
						rf.receiveVoteNum++
						if rf.receiveVoteNum > len(rf.peers)/2 {
							rf.ToLeader(term, CandidateReceiveMajor)
						}
					}
				}
			}
		}
	} else {
		// DebugRpcFail(dVote, "RequestVote")
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	reply.LeaderTerm = args.Term
	// The candidate has a smaller term, do not vote for it.
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		// rf.ToFollower(args.Term, CandidateDiscoverHigherTerm)
		if rf.state == Leader {
			Debug(dLog, "S%d get args from S%d: args.Term=%d, currentTerm=%d in RequestVote", rf.me, args.CandidateId, args.Term, rf.currentTerm)
			rf.ToFollower(args.Term, LeaderDiscoverHigherTerm)
		} else if rf.state == Candidate {
			rf.ToFollower(args.Term, CandidateDiscoverHigherTerm)
		} else if rf.state == Follower {
			rf.ToFollower(args.Term, FollowerDiscoverHigherTerm)
		}
		// rf.SetRandomExpireTime()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.GetLastTerm() > args.LastLogTerm ||
			(rf.GetLastTerm() == args.LastLogTerm && rf.GetLastIndex() > args.LastLogIndex) {
			return
		}
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		// rf.SetHeartBeatExpireTime()
		rf.SetRandomExpireTime()
	}
}
