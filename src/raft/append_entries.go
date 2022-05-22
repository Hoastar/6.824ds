package raft

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = args.Term
	if rf.killed() {
		DPrintf(Warning, "AppendEntries: Raft instance killed. Quit\n")
		return
	}

	rf.sawTerm(args.Term, "when receving append entries") // update term
	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	tmpstate := rf.role
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf(Debug, "AppendEntries: voted reject; args.term %v(leadId %v) < rf.term %v(rf.server %v) \n", args.Term, args.LeaderId, rf.currentTerm, rf.me)
		return
	}

	if tmpstate == Candidate && reply.Term == args.Term {
		DPrintf(Debug, "Receive AppendEntiry msg with the same term: %d, switch to follower\n", reply.Term)

		DPrintf(Debug, "become follwer\n")

		rf.role = Follower
		rf.votedFor = -1

		go rf.followerBackLoop() // will refresh timeout
	} else { // here must be follower, need to reflash timeout
		go func() { rf.backLoopChan <- backLoopRefresh }()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.persist()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) doProperAppendEntries(server int, term int) { //must be thread safe
	// if no longer leader, do not send, otherwise the processing result logic will
	// continuesly interpret the fail from peers as prelog mismatch and decrease to -1
	rf.mu.Lock()
	if term != rf.currentTerm || rf.role != Leader || rf.killed() {
		DPrintf(Info, "Stop send to peer %d, since not leader or killedn\n", server)
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{rf.currentTerm, rf.me}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		go rf.processAppendEntriesReply(server, reply, term)
	}
}

func (rf *Raft) processAppendEntriesReply(server int, reply *AppendEntriesReply, term int) {
	if rf.sawTerm(reply.Term, "in AppendEntries reply") {
		return
	}
	if !reply.Success {
		DPrintf(Debug, "HeartBeat to %v failed; term: %v;\n", server, term)
	} else {
		DPrintf(Debug, "HeartBeat to %v Successed; term: %v; now: %v\n", server, term)
	}
}
