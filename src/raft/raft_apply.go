package raft

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) ApplyCmd(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			// Wait()方法内部会先释放锁，因此在调用Wait()方法前必须保证持有锁。在唤醒之后会自动持有锁，因此Wait()方法之后再不能加锁。
			// rf.applyCh <- msg这条语句可能会阻塞，因此执行这条语句前先释放锁，但构建msg的过程需要持有锁
			rf.cv.Wait()
		}
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		tmpLog := make([]LogEntry, commitIndex-lastApplied)
		copy(tmpLog, rf.log[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()
		for _, l := range tmpLog {
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      l.Command,
				CommandIndex: l.Index,
			}
		}
		rf.mu.Lock()
		Debug(dCommit, "S%d applys I%d-I%d", rf.me, rf.lastApplied+1, rf.commitIndex)
		rf.lastApplied = commitIndex
		//if rf.lastApplied < commitIndex {
		//	rf.lastApplied = commitIndex
		//}
		rf.mu.Unlock()
	}
}
