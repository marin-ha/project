package raft

import (
	"sort"
	"time"
)

// Automatic Failure Recovery manifests in 2 aspect:
// 1.First, a leader crashed, the Rafts will elect a new leader, it proceeds the work of
// old crashed leader to replicate its logs to majority of followers.
// 2.Second, leader has resposibility to send missing logs of follower's log queue,
// even the follower lose its all logs, leader still need re-transmit all logs
//
// spawn log-sync goroutine for every other peer nodes
func (rf *Raft) replicateLogs() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// construct single goroutine for every single peer node
		// replicate logs or send heartbeats, periodically
		go func(peer int) {
			for {

				rf.appendCond.L.Lock()
				if rf.killed() || rf.status != SLEADER {
					rf.appendCond.L.Unlock()
					return
				}
				for rf.NextIndex[peer] > rf.lastEntryIndex() {
					rf.appendCond.Wait()
				}
				rf.appendCond.L.Unlock()

				go rf.syncLog(peer) // send log or heartbeat

				time.Sleep(10 * time.Millisecond)
			}
		}(i)

		// heartbeat periodically
		go func(peer int) {
			for {
				rf.lock()
				if rf.killed() || rf.status != SLEADER {
					rf.unlock()
					return
				}
				rf.unlock()

				go rf.syncLog(peer) // send log or heartbeat

				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

	// maintain Leader's CommitIndex in background
	go rf.commitLogs()
}

// "Log Matching Property" refers to section 5.3 of original paper
func (rf *Raft) syncLog(peer int) {
	rf.lock()

	// check if current leader can replicate this log into follower
	if rf.NextIndex[peer] <= rf.lastEntryIndex() && rf.lastEntryTerm() != rf.currentTerm {
		// it will breaks Leader Completeness Property if send log belongs to old leader
		rf.unlock()
		return
	}

	// Otherwise replicate leader log into follower or send heartbeat

	var (
		term = rf.currentTerm // need Double-Check after RPC

		/**Index of the will-sending suffix log
		 * Log[1:maxIdx-1]: the log that needs replicate into followers by leader
		 * Log[x:maxIdx-1]:
		 *		the log suffix that needs send to follower by leader in this loop,
		 * 	could be empty, then will send a heartbeat.
		 */
		maxIdx = rf.lastEntryIndex() + 1

		/**Offset in rf.logs array, not real index of log entry
		 * no need double check whether sended log entries was changed, because:
		 * 	 Double-checking current term after RPC represents that the leader did not malfunction,
		 * plus Leader never updates/deletes/inserts entry into the prefix of its own log.
		 * so sended log entris in leader's log are always the same as before.
		 *   Leader only change log in two cases: Appending new entries into its log after position rf.le;
		 * Compacting a prefix of applied logs into snapshot.
		 *
		 * rf.logs[start:end-1] represents a suffix of leader's log, sending the log suffix later.
		 * sending an implicit heartbeat if start == end
		 */
		start = rf.offset(rf.NextIndex[peer])
		// end   = rf.le // may changed during RPC, don't depend on its current value
	)

	// query missing log entries of follower's log within the rf.logs[start:end]
	// if follower reply agreement, return
	for start > 0 {

		maxIdx = rf.lastEntryIndex() + 1 // must update it every loop
		entries := make([]LogEntry, rf.le-start)
		// Deep Copy
		copy(entries, rf.logs[start:rf.le]) // Deep Copy
		args := &AppendEntriesArgs{
			Term:         term,
			Entries:      entries,
			PrevLogTerm:  rf.prevEntryTerm(start),
			PrevLogIndex: rf.prevEntryIndex(start),
			LeaderCommit: rf.CommitIndex,
		}

		rf.unlock()

		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peer, args, &reply); !ok {
			return
		}

		rf.lock()
		// Double-Check if raft is still leader and alive
		// Double-Check if currentTerm is the same as original term before sending RPC
		// Double-Check if log[:maxIdx] have replicated by other goroutines (maybe next replication had run faster)
		//
		// Log's length maybe change after RPC, because snapshot can compact log
		if rf.killed() || rf.status != SLEADER || rf.currentTerm != term {
			// something changed, invalidate this reply
			rf.unlock()
			return
		}

		// check if detected higher term
		if reply.Term > term {
			rf.recvNewTerm(reply.Term)
			rf.unlock()
			return
		}

		// check if the append/overwrite operation is accepted by follower
		if reply.Success {
			// replicate Log[1:maxIdx-1] into peer node successfully
			rf.MatchIndex[peer] = maxIdx - 1
			rf.NextIndex[peer] = maxIdx
			rf.unlock()
			return
		} else {
			/**
			 * Grow the suffix on leader's log for next sending. if follower accepts it,
			 * represents that the suffix from leader's log and the prefix from follower's
			 * can constitute a log identical to leader's log.
			 *
			 * slower approach: growing suffix one by one
			 * faster approach: roll back the pointer start more at a time
			 */
			if reply.XLen <= args.PrevLogIndex {
				// follower's log is too short
				start = rf.offset(reply.XLen+1)
			} else {
				idx := -1
				// search for log entry with term=reply.XTerm
				for i := rf.le - 1; i > 0; i-- {
					if rf.logs[i].Term < reply.XTerm { // no more, fast quit
						break
					} else if rf.logs[i].Term == reply.XTerm {
						idx = i
						break
					}
				}

				if idx == -1 {
					start = rf.offset(reply.XIndex)
				} else {
					start = idx + 1
				}
			}

			if start > 0 {
				// speed up subsequent AppendEntries in this term
				// if not, may fail tests due to the long time spent
				rf.NextIndex[peer] = rf.index(start)
			}
		}
	}

	rf.unlock()

	// check whether follower loses/lacks part of log entries that were compacted by leader
	if start <= 0 {
		// send snapshot if follower's log lags behind leader too much
		rf.sendSnapshot(peer)
		return
	}
}

func (rf *Raft) commitLogs() {
	for !rf.killed() {
		rf.lock()
		if rf.status != SLEADER {
			rf.unlock()
			return
		}

		// check if current log belongs to current leader by checking last log entry's term
		if rf.lastEntryTerm() != rf.currentTerm {
			// Leader can not commit previous term's log
			rf.unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// get the largest index (that leader think so) in replicated logs of each node, except leader
		sortArr := copyIntSlice(rf.MatchIndex, rf.me)

		// descending sort
		sort.Slice(sortArr, func(i, j int) bool {
			return sortArr[i] > sortArr[j]
		})

		major := len(rf.peers) / 2 // assume server number is odd

		// get the max index that has major indexs larger than it
		// update rf.CommitIndex if true
		if t := sortArr[major-1]; t > rf.CommitIndex {
			if rf.logs[rf.offset(t)].Term == rf.currentTerm {
				// Leader replicates and commits only its own log
				rf.CommitIndex = t
				// apply committed log entries if rf.CommitIndex changed
				if rf.LastApplied < rf.CommitIndex {
					rf.applyCond.Broadcast() // wakeup
				}
			} else {
				// don't replicate/commit logs that belongs to Old Leader into followers.
				// maybe cause the newly elected Leader contains not all committed logs (Leader Completeness Property)
			}
		}

		rf.unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) send2StateMachine(applyCh chan ApplyMsg) {
	for !rf.killed() {

		if arr := rf.mq.pop(); len(arr) > 0 {
			for i := range arr {
				applyCh <- *(arr[i])
			}
		} else {
			rf.mq.wait() // wait new event
		}

		// time.Sleep(10*time.Millisecond) // busy wait not elegant
	}
}

func (rf *Raft) sendSnapshot(peer int) {

	rf.lock()
	var (
		term = rf.currentTerm // need Double-Check after RPC
		args = &InstallSnapshotArgs{
			Term:              term,
			LastIncludedTerm:  rf.snapshotTerm,
			LastIncludedIndex: rf.snapshotIndex,
			Snapshot:          rf.snapshot,
		}
	)
	rf.unlock()

	reply := InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(peer, args, &reply); !ok {
		return
	}

	rf.lock()
	defer rf.unlock()

	// check if higher term
	if reply.Term > rf.currentTerm {
		rf.recvNewTerm(reply.Term)
	}

	// Double-Check after RPC
	if rf.status != SLEADER || term != rf.currentTerm {
		return
	}
	// check reply's result
	if reply.Success {
		// update leader's states about the follower if need
		rf.NextIndex[peer] = args.LastIncludedIndex + 1
		rf.MatchIndex[peer] = args.LastIncludedIndex
	}

	// if fail, will try next time
}
