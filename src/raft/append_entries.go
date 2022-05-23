package raft

const (
	RejectReasonTerm = iota
	RejectReasonConflict
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                   int
	Success                bool
	IndexAppend            int
	SenderTerm             int
	TermConflict           int
	TermConflictFirstIndex int
	LogLength              int
	RejectReason           int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.persist()
	reply.SenderTerm = args.Term
	if rf.killed() {
		DPrintf(Info, "Raft instance killed. Quit")
		return
	}

	rf.sawTerm(args.Term, "when receving append entries") // update term if necessary also update timer

	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock() // lock the whole operation to prevent intermediate states
	tmpRole := rf.role
	reply.Term = rf.currentTerm

	var prevEntriesMatch bool
	if args.PrevLogIndex <= rf.log.BaseIndex { // is in snapshot so it's commited
		prevEntriesMatch = true
	} else {
		prevEntriesMatch = args.PrevLogIndex <= rf.log.LastIndex() && rf.log.Get(args.PrevLogIndex).Term == args.PrevLogTerm
	}

	if args.Term < reply.Term { // reject append if saw older term
		DPrintf(Info, "Receive AppendEntries but it's term %d is too old, reject. CurrentTerm: %d", args.Term, reply.Term)
		reply.RejectReason = RejectReasonTerm
		return
	}

	if tmpRole == Candidate && reply.Term == args.Term {
		DPrintf(Info, "Receive AppendEntry msg with the same term: %d, switch to follower", reply.Term)

		DPrintf(Info, "become follower")

		rf.role = Follower

		go rf.followerBackLoop() // will refresh timeout
	} else { // here must be follower, need to refresh timeout
		go func() { rf.backLoopChan <- backLoopRefresh }()
	}

	if prevEntriesMatch { // matched and apply Entries
		var findConflict bool = false

		nextIndex := args.PrevLogIndex + 1
		j := 0
		//  handle trimmed Entries
		for nextIndex <= rf.log.BaseIndex { // inc until the first index after snapshot
			nextIndex++
			j++
		}

		for ; nextIndex <= rf.log.LastIndex() && j < len(args.Entries); nextIndex++ {
			// check conflict. no conflict means the same entry
			if rf.log.Get(nextIndex).Term != args.Entries[j].Term {
				findConflict = true
				break
			}
			j++
		}
		if findConflict { // if found conflict, truncate
			DPrintf(Info, "truncate at %d, origin length %d", nextIndex, rf.log.LastIndex()+1)
			DPrintf(Info, "Entries %+v, args %+v", rf.log, args)
			rf.log.TruncateAt(nextIndex)
		}

		// append the rest
		for ; j < len(args.Entries); j++ {
			rf.log.Append(args.Entries[j])
		}

		if findConflict {
			DPrintf(Info, "Now Entries: %+v", rf.log)
		}

		entriesLength := rf.log.LastIndex() + 1
		commitIndex := -1
		if args.LeaderCommit > rf.commitIndex { // update commit index if needed
			rf.commitIndex = Min(args.LeaderCommit, rf.log.LastIndex())
			commitIndex = rf.commitIndex
		}

		reply.Success = true
		reply.IndexAppend = args.PrevLogIndex + len(args.Entries)
		DPrintf(Info, "Successfully append. last index append: %d, now Entries len: %d, append args: %+v", reply.IndexAppend, entriesLength, args)

		if commitIndex != -1 {
			go rf.applyMsg() // async apply msg when commit changes
			DPrintf(Info, "Followers commit updated to %d", commitIndex)
		}

	} else { // not PrevLogIndex not match, PrevLogIndex must be after the snapshot

		conflictIndex := Min(args.PrevLogIndex, rf.log.LastIndex())
		reply.TermConflict = rf.log.Get(conflictIndex).Term // so its safe to Get()
		reply.LogLength = rf.log.LastIndex() + 1

		for ; conflictIndex > rf.log.BaseIndex+1 && rf.log.Get(conflictIndex).Term == reply.TermConflict; conflictIndex-- {
		}
		reply.TermConflictFirstIndex = conflictIndex // at least 1
		reply.RejectReason = RejectReasonConflict
		DPrintf(Info, "Reject AppendEntry since prev Entries unmatch: PrevLogIndex %d, PrevLogTerm %d",
			args.PrevLogIndex, args.PrevLogTerm)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.persist()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) processAppendEntriesReply(server int, reply *AppendEntriesReply, term int) {
	if rf.sawTerm(reply.Term, "in AppendEntries reply") {
		return
	}

	if !reply.Success {
		if reply.RejectReason == RejectReasonTerm {
			// this is when reply.Term == myTerm but receive reject
			// cause by:
			// A is leader, B timeout, B inc to term K, A saw term and turn two candidate,
			// but K results no leader and turn to K + 1, A wins in K + 1 but receive the B's reject at term K
			DPrintf(Info, "Receive reject because term too old, but current term is >= reply.Term, that's rare and will ignore it")
			return
		}
		var nextIndex int

		nextIndex = reply.TermConflictFirstIndex // now peer side guarantee at least 1

		rf.mu.Lock()
		rf.leaderData.nextIndex[server] = nextIndex
		rf.mu.Unlock()
		DPrintf(Info, "AppendEntries reply from %d failed. Will retry with a smaller nextIndex: %d", server, nextIndex)
		// will retry until nolonger leader or stopped. THe Entriesic is handled inside doProperAppendEntries
		go rf.doProperAppendEntries(server, term)
	} else {
		commitUpdated := false

		rf.mu.Lock()

		if rf.role != Leader {
			// handle when is not leader. necessary since it will change the commit
			DPrintf(Info, "No longer Leader when processing append Entries Reply. Quit")
			rf.mu.Unlock()
			return
		}

		rf.leaderData.nextIndex[server] = rf.log.LastIndex() + 1 //always send full Entries
		newMatched := reply.IndexAppend
		rf.leaderData.matchIndex[server] = newMatched

		// update commitIndex if most are stored
		var nextIndex = rf.leaderData.nextIndex[server]
		if newMatched > rf.commitIndex {
			count := 0
			for _, id := range rf.leaderData.matchIndex {
				if id >= newMatched {
					count++
				}
			}
			if count*2 > len(rf.peers) && rf.log.Get(newMatched).Term == rf.currentTerm {
				rf.commitIndex = newMatched
				commitUpdated = true
			}
		}
		rf.mu.Unlock()

		DPrintf(Info, "AppendEntries reply from %d succuess: %+v. and now nextIndex is %d", server, reply, nextIndex)

		if commitUpdated {
			go rf.applyMsg() // do msg apply
			DPrintf(Info, "The Leader's commit index is updated to %d", newMatched)
		}
	}
}

func (rf *Raft) doProperAppendEntries(server int, term int) { //must be thread safe
	// if no longer leader, do not send, otherwise the processing result Entriesic will
	// continuesly interpret the fail from peers as preEntries mismatch and decrease to -1
	rf.mu.Lock()

	if term != rf.currentTerm || rf.role != Leader || rf.killed() {
		DPrintf(Info, "Stop send to peer %d, since not leader or killed", server)
		rf.mu.Unlock()
		return
	}

	// handle trim
	var sendSnapshot = false
	if rf.leaderData.nextIndex[server] <= rf.log.BaseIndex { // need to send snapshot
		sendSnapshot = true
	}

	PrevLogIndex := Max(rf.leaderData.nextIndex[server]-1, rf.log.BaseIndex)
	PrevLogTerm := rf.log.Get(PrevLogIndex).Term
	entrySlice := rf.log.SliceFrom(PrevLogIndex + 1)
	entries := make([]LogEntry, len(entrySlice))
	copy(entries, entrySlice)

	args := &AppendEntriesArgs{rf.currentTerm, rf.me, PrevLogIndex, PrevLogTerm, entries, rf.commitIndex}
	rf.mu.Unlock()

	if sendSnapshot {
		rf.doInstallSnapshot(server)
	}

	DPrintf(Info, "Sending append to server %d: currentTerm: %d PrevLogIndex: %d PrevLogTerm: %d EntriyNum %d",
		server, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)

	if ok {
		rf.processAppendEntriesReply(server, reply, term)
	}
}
