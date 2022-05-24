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
	// 在AppendEntries 方法返回之前调用persist()进行持久化；因而此时raft实例的log,term,votedFor应该都有变化
	defer rf.persist() // AppendEntries return
	reply.SenderTerm = args.Term
	if rf.killed() {
		DPrintf(Info, "Raft instance killed. Quit")
		return
	}

	// 对于所有的服务器而言的规则
	// 1: 如果接收到的RPC请求或者响应中，Term T > currentTerm，则令 currentTerm = T，并切换为跟 随者状态(5.1 节)
	rf.sawTerm(args.Term, "when receving append entries") // update term if necessary also update timer

	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock() // lock the whole operation to prevent intermediate states
	tmpRole := rf.role
	reply.Term = rf.currentTerm

	// Implement AppendEntries RPC
	// 2. 是否匹配
	var prevEntriesMatch bool
	if args.PrevLogIndex <= rf.log.BaseIndex { // is in snapshot so it's commited
		prevEntriesMatch = true
	} else {
		// 2.1 匹配：
		// 在接收者日志中能找到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目
		prevEntriesMatch = args.PrevLogIndex <= rf.log.LastIndex() && rf.log.Get(args.PrevLogIndex).Term == args.PrevLogTerm
	}

	// Implement AppendEntries RPC
	// 1. 如果领导人的任期小于接收者的当前任期(译者注:这里的接收者是指跟随者或者候选人)(5.1 节)，则返回假
	if args.Term < reply.Term { // reject append if saw older term
		DPrintf(Info, "Receive AppendEntries but it's term %d is too old, reject. CurrentTerm: %d", args.Term, reply.Term)
		reply.RejectReason = RejectReasonTerm
		return
	}

	// 1.1. 如果接收者刚好是候选人且任期一致
	if tmpRole == Candidate && reply.Term == args.Term {
		DPrintf(Info, "Receive AppendEntry msg with the same term: %d, switch to follower", reply.Term)

		DPrintf(Info, "become follower")

		// 主动切换到Follower角色
		// switch to role
		rf.role = Follower

		go rf.followerBackLoop() // will refresh timeout
	} else { // here must be follower, need to refresh timeout
		//1.1. 如果接收者刚好是Follower，重置选举超时时间
		go func() { rf.backLoopChan <- backLoopRefresh }()
	}

	// Implement AppendEntries RPC
	// 2.1.1 matched (5.3 节)
	if prevEntriesMatch { // matched and apply Entries
		var findConflict bool = false
		// 3. 如果一个已经存在的条目和新条目(译者注:即刚刚接收到的日志条目)发生了冲突(因为索引相同， 任期不同)，
		// 那么就删除这个已经存在的条目以及它之后的所有条目 (5.3 节)
		nextIndex := args.PrevLogIndex + 1
		j := 0
		//  handle trimmed Entries

		// if args.PrevLogIndex <= rf.log.BaseIndex: prevEntriesMatch is true
		// 自增直到快照后的基础索引
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
			// 日志裁剪：删除这个已经存在的条目以及它之后的所有条目
			rf.log.TruncateAt(nextIndex)
		}

		// append the rest
		// 4. 追加日志中尚未存在的任何新条目
		for ; j < len(args.Entries); j++ {
			rf.log.Append(args.Entries[j])
		}

		if findConflict {
			DPrintf(Info, "Now Entries: %+v", rf.log)
		}

		entriesLength := rf.log.LastIndex() + 1
		commitIndex := -1
		// 5. 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交的最高日志条目的索引

		if args.LeaderCommit > rf.commitIndex { // update commit index if needed
			// 则把接收者的已知已提交的最高日志条目的索引值重置为
			// 领导人的已知已经提交的最高的日志条目的索引leaderCommit或者上一个新条目的索引取两者的最小值
			rf.commitIndex = Min(args.LeaderCommit, rf.log.LastIndex())
			commitIndex = rf.commitIndex
		}

		reply.Success = true
		// 已追加到当前接收者的的日志的索引值
		reply.IndexAppend = args.PrevLogIndex + len(args.Entries)
		DPrintf(Info, "Successfully append. last index append: %d, now Entries len: %d, append args: %+v", reply.IndexAppend, entriesLength, args)

		if commitIndex != -1 {
			go rf.applyMsg() // async apply msg when commit changes
			DPrintf(Info, "Followers commit updated to %d", commitIndex)
		}

	} else { // not PrevLogIndex not match, PrevLogIndex must be after the snapshot
		// Implement AppendEntries RPC
		// 2.1.2 not matched (5.3 节)
		// 找到冲突的索引位置以及对应的任期
		conflictIndex := Min(args.PrevLogIndex, rf.log.LastIndex())
		reply.TermConflict = rf.log.Get(conflictIndex).Term // so it's safe to Get()
		reply.LogLength = rf.log.LastIndex() + 1

		// feat: 当附加日志 RPC 的请求被拒绝的时候，跟随者可以(返回)冲突条目的任期号和该任期号对应的最小索引地址。(5.3 节)
		for ; conflictIndex > rf.log.BaseIndex+1 && rf.log.Get(conflictIndex).Term == reply.TermConflict; conflictIndex-- {
		}
		reply.TermConflictFirstIndex = conflictIndex // at least 1

		// 返回冲突任期号对应的最小索引地址
		reply.RejectReason = RejectReasonConflict
		DPrintf(Info, "Reject AppendEntry since prev Entries unmatch: PrevLogIndex %d, PrevLogTerm %d",
			args.PrevLogIndex, args.PrevLogTerm)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// 发送AppendEntries RPC之前调用
	rf.persist()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) processAppendEntriesReply(server int, reply *AppendEntriesReply, term int) {

	// 对于所有的服务器而言的规则:
	// 2: 如果commitIndex > lastApplied，则 lastApplied 递增，并将log[lastApplied]应用到状态机 中(5.3 节)
	if rf.sawTerm(reply.Term, "in AppendEntries reply") {
		return
	}

	if !reply.Success {
		if reply.RejectReason == RejectReasonTerm { // 因为此次请求的leader任期小于接收者的任期，而被拒绝。
			// this is when reply.Term == myTerm but receive reject
			// cause by:
			// A is leader, B timeout, B inc to term K, A saw term and turn two candidate,
			// but K results no leader and turn to K + 1, A wins in K + 1 but receive the B's reject at term K
			DPrintf(Info, "Receive reject because term too old, but current term is >= reply.Term, that's rare and will ignore it")
			return
		}
		// 如果因为日志不一致而失败，则 更新nextIndex并重试；
		var nextIndex int

		// 无需nextIndex递减：reply返回了在冲突任期的日志条目最小索引值
		nextIndex = reply.TermConflictFirstIndex // now peer side guarantee at least 1

		rf.mu.Lock()
		rf.leaderData.nextIndex[server] = nextIndex
		rf.mu.Unlock()
		DPrintf(Info, "AppendEntries reply from %d failed. Will retry with a smaller nextIndex: %d", server, nextIndex)
		// will retry until nolonger leader or stopped. THe Entriesic is handled inside doProperAppendEntries
		// 重试sendAppendEntries
		go rf.doProperAppendEntries(server, term)
	} else { // sendAppendEntries 成功
		commitUpdated := false

		rf.mu.Lock()

		if rf.role != Leader {
			// handle when is not leader. necessary since it will change the commit
			DPrintf(Info, "No longer Leader when processing append Entries Reply. Quit")
			rf.mu.Unlock()
			return
		}
		// 更新 nextIndex and matchIndex
		rf.leaderData.nextIndex[server] = rf.log.LastIndex() + 1 //always send full Entries
		newMatched := reply.IndexAppend
		rf.leaderData.matchIndex[server] = newMatched

		// update commitIndex if most are stored
		var nextIndex = rf.leaderData.nextIndex[server]

		// 假设存在N满足N > commitIndex，使得大多数的 matchIndex[i] ≥ N 以及
		// log[N].term == currentTerm 成立，则令 commitIndex = N(5.3 和 5.4 节)
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
			// 日志提交更新之后，进行应用
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

	// 获取当前raft leader实例猜想的server接受者的紧邻新日志条目之前的那个日志条目的索引与任期
	// Gets the index of the current raft leader instance conjecture that the recipient is the server immediately
	// before the new log entry
	PrevLogIndex := Max(rf.leaderData.nextIndex[server]-1, rf.log.BaseIndex)
	PrevLogTerm := rf.log.Get(PrevLogIndex).Term
	// 获取自PrevLogIndex+1起的新日志条目
	entrySlice := rf.log.SliceFrom(PrevLogIndex + 1)
	entries := make([]LogEntry, len(entrySlice))
	copy(entries, entrySlice)

	// 准备sendAppendEntries方法的请求参数 AppendEntriesArgs
	// Prepare the request parameters for the sendAppendEntries method
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
