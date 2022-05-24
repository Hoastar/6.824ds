package raft

import (
	"log"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// startElectDaemon
func (rf *Raft) startElectDaemon() {

	if rf.killed() {
		DPrintf(Info, "Raft instance killed. Quit")
		return
	}
	electionDeadline := time.Now().Add(randElectionTimeoutDuration())
	var copyCurrentTerm int
	rf.mu.Lock()
	rf.role = Candidate
	rf.votedFor = rf.me //vote for myself
	rf.currentTerm += 1 //new term
	copyCurrentTerm = rf.currentTerm
	// returns the term of the last log entry for the current raft instance
	lastLogIndex := rf.log.LastIndex()
	lastLogTerm := rf.log.Get(lastLogIndex).Term

	// build RequestVote parameters
	args := &RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	rf.mu.Unlock()
	DPrintf(Info, "Become candidate and start a new Term: %d", copyCurrentTerm)

	local_mu := sync.Mutex{}
	cv := sync.NewCond(&local_mu)
	var granted int
	var finished int

	local_mu.Lock()
	granted = 1 //1 since self voted
	finished = 0
	local_mu.Unlock()

	// 定义一个包括发起投票和处理投票请求结果的私有函数
	// Define a private function that includes send RequestVote  and processing the result of a RequestVote Reply
	doVote := func(server int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(server, args, reply)
		if ok {
			DPrintf(Info, "Receive vote reply from %d: %+v", server, *reply)
			rf.sawTerm(reply.Term, "In processing vote reply") // might change the state
			local_mu.Lock()
			if reply.VoteGranted {
				granted++
			}
		} else {
			local_mu.Lock()
		}
		finished++
		local_mu.Unlock()
		cv.Broadcast()
	}

	DPrintf(Info, "Requesting vote: %+v", *args)
	for i, _ := range rf.peers {

		if i != rf.me { // ok to read rf.me since no one will write it
			// 并行的发起投票
			go doVote(i, args)
		}
	}

	DPrintf(Info, "Waiting for voting result")

	for {
		if rf.killed() {
			DPrintf(Info, "Raft instance killed. Quit")
			return
		}
		rf.mu.Lock()
		nowState := rf.role
		rf.mu.Unlock()
		// concern for ABA problem, state is change back to candidate again
		// need to make sure there is no another follower backloop now

		if nowState != Candidate {
			DPrintf(Info, "In new election but current state is %d, quit new election function", nowState)
			break
		}

		// 判断是否超时
		if time.Now().After(electionDeadline) {
			DPrintf(Info, "Election timeout with no leader, start a new term for election")
			go rf.startElectDaemon()
			break
		}

		local_mu.Lock()

		// 判断投票结果
		if granted*2 > len(rf.peers) { //logic of turning self into leader
			DPrintf(Info, "Got grant from majority: %d for term: %d", granted, copyCurrentTerm)
			rf.role = Leader

			// Initialize leader data after the election is successful; matchIndex is 0
			rf.leaderData = &LeaderData{make([]int, len(rf.peers)), make([]int, len(rf.peers))}
			for i, _ := range rf.leaderData.nextIndex {
				// The initial value is +1 index of the leader's last log entry
				// nextIndex初始值为领导人最后的日志条目的索引+1
				rf.leaderData.nextIndex[i] = rf.log.LastIndex() + 1 // init nextIndex to lastLogIndex + 1
			}

			DPrintf(Info, "Become leader")

			// issue a command no op to come up to the commit, command it self stores term
			commandIndex := rf.log.Get(rf.log.LastIndex()).CommandIndex // non valid command doesn't inc command index
			// 领导者实例给自身追加一条"自己已是leader的"日志条目
			rf.log.Append(LogEntry{CommandTypeNewLeader, rf.currentTerm, commandIndex, rf.currentTerm})

			// 更新领导者实例自身的matchIndex值
			rf.leaderData.matchIndex[rf.me] = rf.log.LastIndex()

			go rf.leaderBackLoop(rf.currentTerm)
			local_mu.Unlock()
			return

		}
		local_mu.Unlock()

		time.Sleep(time.Millisecond)
	}

	DPrintf(Info, "Waiting the rest vote reply and do nothing else")

	local_mu.Lock() // wait rest to finish
	// 阻塞等待投票完成
	for finished < len(rf.peers)-1 {
		cv.Wait()
	}
	local_mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 完成RequestVote RPC调用结束，持久化
	defer rf.persist()
	if rf.killed() {
		DPrintf(Info, "Raft instance killed. Quit")
		return
	}

	// 对于所有的服务器而言的规则:
	// 2: 如果commitIndex > lastApplied，则 lastApplied 递增，并将log[lastApplied]应用到状态机 中(5.3 节)
	rf.sawTerm(args.Term, "when receving vote request") // now currentTerm >= args.Term
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	votedFor := rf.votedFor
	rf.mu.Unlock()

	// 如果请求Term比接收者Term小，拒绝投票
	if reply.Term > args.Term {
		reply.VoteGranted = false
		DPrintf(Info, "Reject vote request from %d since Term %d is older than self: %d.", args.CandidateId, args.Term, reply.Term)
		return
	}

	if reply.Term != args.Term { //just assert
		log.Fatalf("should never be here!")
	}

	if votedFor == args.CandidateId { // 如果在当前任期已经给当前candidate投过票了，VoteGranted 为true, return
		reply.VoteGranted = true // same request from the voted server
		return
	} else if votedFor != -1 && votedFor != args.CandidateId { // 如果在当前任期已经给其他candidate投过票了，拒绝投票
		reply.VoteGranted = false
		DPrintf(Info, "Reject vote request from %d since have voted for %d.", args.CandidateId, votedFor)
		return
	} else { //not voted yet
		var tmpLastLogTerm int
		var tmpLastIndex int
		rf.mu.Lock()
		// vote only at least as up-to-date as than
		// 获取接收者最后一条日志条目的索引和任期
		tmpLastIndex = rf.log.LastIndex()
		tmpLastLogTerm = rf.log.Get(tmpLastIndex).Term

		// 如果candidate的最后一条日志条目比接收者新，那么给他投票
		if args.LastLogTerm > tmpLastLogTerm || args.LastLogTerm == tmpLastLogTerm && args.LastLogIndex >= tmpLastIndex {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
		rf.mu.Unlock()
		if !reply.VoteGranted {
			DPrintf(Info, "Reject vote request from %d since not at least up to date. request %+v, lastlogterm %d, lastlogindex %d.", args.CandidateId, args, tmpLastLogTerm, tmpLastIndex)
		} else {
			// 投票完成之后，重置follower的选举超时时间
			rf.backLoopChan <- backLoopRefresh // need refresh if vote for
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// 调用persist()进行持久化
	rf.persist()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
