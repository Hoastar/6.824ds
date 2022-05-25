package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) noLockSafeSetSnapshot(snapshot []byte, index int) {
	// set snapshot safely since state machine is newer than snapshot
	// trim Entries
	DPrintf(Info, "Setting raft snapshot with index %d", index)
	if index <= rf.log.BaseIndex {
		DPrintf(Info, "received snapshot %d is no newer than current latest snapshot index: %d. Discard", index, rf.log.BaseIndex)
		return
	}
	// index为Leader Raft的快照的最后包含的索引

	// 丢弃index之前的日志
	rf.log.DiscardBefore(index)                         // Entries[index] is to store snapshot slot
	rf.log.Get(index).CommandType = CommandTypeSnapshot // set snapshot
	// 返回 index之后的entries及其currentTerm 和 votedFor
	stateData := rf.noLockPersist()

	// now save Entries：持久化包括index之后的entries，currentTerm以及votedFor State内容 与 快照内容
	rf.persister.SaveStateAndSnapshot(stateData, snapshot)
}

func (rf *Raft) ReceiveSnapshot(snapshot []byte, index int) {
	// receive snapshot from state machine

	DPrintf(Info, "Received Snapshot up to index %d from state machine", index)
	rf.mu.Lock()
	rf.noLockSafeSetSnapshot(snapshot, index)
	rf.mu.Unlock()

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.sawTerm(args.Term, "when in InstallSnapshot")

	DPrintf(Info, "Receive InstallSnapshot from %d, currentTerm %d, snapshot index %d, snapshot term %d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(Info, "InstallSnapshot term too old. Do nothing")
		rf.mu.Unlock()
		return
	}

	// when snapshot is older than self
	if args.LastIncludedIndex <= rf.log.BaseIndex {
		DPrintf(Info, "InstallSnapshot index older than self. Do nothing")
	} else if args.LastIncludedIndex <= rf.lastApplied { // just update raft
		// 更新raft
		rf.noLockSafeSetSnapshot(args.Data, args.LastIncludedIndex)
	} else { // need to update sate machine
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = Max(args.LastIncludedIndex, rf.commitIndex) // update commit if necessary
		DPrintf(Info, "Updating state machine")
		// have to send inside the lock. to ensure no apply command before that
		// need to send term to allow state machine performance  GC before update it's state
		rf.applyCh <- ApplyMsg{false, CommandTypeUpdateStateMachine, args.Data, -1, args.Term, -1} // not even a Entries

		// update Entries
		if rf.log.LastIndex() >= args.LastIncludedIndex {
			// it is long enough to do set snapshot
			rf.noLockSafeSetSnapshot(args.Data, args.LastIncludedIndex)
		} else { // discard whole Entries
			// 丢弃整个Entries, 接受来自Leader的快照
			DPrintf(Info, "Discard the whole Entries")
			rf.log.Entries = []LogEntry{}
			rf.log.Append(LogEntry{CommandTypeSnapshot, nil, args.LastIncludedIndex, args.LastIncludedTerm})
			rf.log.BaseIndex = args.LastIncludedIndex
			stateData := rf.noLockPersist()
			// 持久化state以及日志快照数据
			rf.persister.SaveStateAndSnapshot(stateData, args.Data)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) doInstallSnapshot(server int) {

	args := InstallSnapshotArgs{}
	var reply InstallSnapshotReply

	rf.mu.Lock()
	if rf.role != Leader || rf.killed() {
		DPrintf(Info, "Stop send InstallSnapshot to peer %d, since not leader or killed", server)
		rf.mu.Unlock()
		return
	}

	// 准备 sendInstallSnapshot RPC的参数
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.log.BaseIndex
	args.LastIncludedTerm = rf.log.Get(args.LastIncludedIndex).Term
	// 读取当前的快照
	args.Data = rf.persister.ReadSnapshot()
	DPrintf(Info, "Sending InstallSnapshot to peer %d, currentTerm %d, snapshot index %d, snapshot term %d", server, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	if ok {
		rf.mu.Lock()
		// 更新nextIndex与matchIndex
		rf.leaderData.nextIndex[server] = Max(rf.leaderData.nextIndex[server], args.LastIncludedIndex+1)
		rf.leaderData.matchIndex[server] = Max(rf.leaderData.matchIndex[server], args.LastIncludedIndex)
		rf.mu.Unlock()
		rf.sawTerm(reply.Term, "when get reply in InstallSnapshot")
	}
}
