package raft

import (
	"log"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	//LastLogIndex int
	//LastLogTerm  int
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
		DPrintf(Warning, "startElectDaemon enter: Raft instance killed. Quit\n")
		return
	}
	// reset election timeout
	election_deadline := time.Now().Add(randElectionTimeoutDuration())
	var copyCurrentTerm int
	var copyServerId int
	rf.mu.Lock()
	DPrintf(Debug, "Raft instance %v is starting election", rf.me)
	rf.role = Candidate
	// vote for self
	rf.votedFor = rf.me
	rf.currentTerm += 1
	copyCurrentTerm = rf.currentTerm
	copyServerId = rf.me

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	DPrintf(Debug, "Server %v Become candidate and start a new Term: %d\n", copyServerId, copyCurrentTerm)
	rf.mu.Unlock()

	local_m := sync.Mutex{}
	cv := sync.NewCond(&local_m)
	var granted, finished int

	local_m.Lock()
	granted = 1
	finished = 0
	local_m.Unlock()
	doVote := func(server int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(server, args, reply)
		if ok {
			DPrintf(Debug, "Recevie vote reply from %d: %+v\n", server, *reply)
			rf.sawTerm(reply.Term, "In processing vote reply") // might change the state
			local_m.Lock()
			if reply.VoteGranted {
				granted++
				DPrintf(Debug, "Recevie vote reply from %d, votedCount:%v\n", server, granted)
			}
		} else {
			local_m.Lock()
		}
		finished++
		local_m.Unlock()
		cv.Broadcast()
	}

	DPrintf(Debug, "Requesting vote: %+v\n", *args)
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go doVote(peer, args)
		}
	}
	DPrintf(Debug, "Waiting for voting result for server %v term %v.\n", copyServerId, copyCurrentTerm)
	for {
		if rf.killed() {
			DPrintf(Warning, "StartElectDaemon wait result: Raft instance killed. Quit\n")
			return
		}
		rf.mu.Lock()
		nowState := rf.role
		if nowState != Candidate {
			DPrintf(Debug, "In new election but current rule is %d, quit new election function\n", nowState)
			rf.mu.Unlock()
			break
		}

		if time.Now().After(election_deadline) {
			DPrintf(Warning, "Election timemout with no leader, start a new term for election\n")
			go rf.startElectDaemon()
			rf.mu.Unlock()
			break
		}

		local_m.Lock()
		if granted > len(rf.peers)/2 {
			DPrintf(Debug, "Got grant from majority: %d granted for server: %d, copyTerm: %v\n", granted, copyServerId, copyCurrentTerm)
			rf.role = Leader

			DPrintf(Info, "CopyServerId %v Become leader copyTerm: %v\n", copyServerId, copyCurrentTerm)
			go rf.leaderBackLoopDaemon(rf.currentTerm)
			local_m.Unlock()
			rf.mu.Unlock()
			return
		}
		local_m.Unlock()
		rf.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	//fmt.Printf("Waiting the rest vote reply and do nothing else\n")
	local_m.Lock() // wait restVotedReply to finish
	for granted < len(rf.peers)/2 && finished != len(rf.peers)-1 {
		DPrintf(Debug, "The number of requests that have been sent is %v\n", finished)
		cv.Wait()
	}
	local_m.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Your code here (2A, 2B).
	// 2A
	if rf.killed() {
		//fmt.Printf("Raft instance killed. Quit\n")
		return
	}
	rf.sawTerm(args.Term, "when receving vote request\n") // now currentTerm >= args.Term
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	votedFor := rf.votedFor

	if reply.Term > args.Term {
		reply.VoteGranted = false
		//DPrintf(Debug, "Reject vote request from %d since Term %d is older than self: %d.\n", args.CandidateId, args.Term, reply.Term)
		rf.mu.Unlock()
		return
	}

	if reply.Term != args.Term {
		log.Fatalf("should never be here!")
	}

	if votedFor == args.CandidateId {
		reply.VoteGranted = true // same request from the voted server
	} else if votedFor != -1 && votedFor != args.CandidateId {
		reply.VoteGranted = false
		DPrintf(Debug, "Reject vote request from %d since have voted for %d.\n", args.CandidateId, votedFor)
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId // vote on a new term
		rf.mu.Unlock()
		if !reply.VoteGranted {
			DPrintf(Debug, "Reject vote request from %d since not at least up to date. request", args.CandidateId)
		} else {
			DPrintf(Debug, "Voted to server %v successful, need refresh\n", args.CandidateId)
			rf.backLoopChan <- backLoopRefresh
		}
		return
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.persist()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
