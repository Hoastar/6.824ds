package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824ds/src/labrpc"
)

// role
const (
	Follower = iota
	Candidate
	Leader
)

// timeout
const (
	heartBeatInterval time.Duration = time.Millisecond * 100
)

// talk to follower
const (
	backLoopWakeKill = -1 // receive kill msg
	backLoopRefresh  = -2 // refresh timer since a recent heartbeat sent
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role         int      // leader candidate folower
	backLoopChan chan int // chan talk to the background thread

	// raft persist state
	currentTerm int
	votedFor    int

	// apply Chan
	applyCh chan ApplyMsg
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	return index, term, isLeader
}

// if saw newer term, change to follower
func (rf *Raft) sawTerm(Term int, msg string) (changed bool) {
	changed = false
	becomeFollower := false
	var previous int
	rf.mu.Lock()
	previous = rf.currentTerm
	if Term > rf.currentTerm { // saw a lager term, return to follwer
		rf.currentTerm = Term
		rf.votedFor = -1 // reset voting
		changed = true
		if rf.role != Follower { // become a follower
			rf.role = Follower
			becomeFollower = true
		}
	}
	rf.mu.Unlock()
	if changed {
		DPrintf(Debug, "Saw term %d which is larger than current: %d, %s. Become follower\n", Term, previous, msg)
	}
	if becomeFollower {
		go rf.followerBackLoop()
	}
	return changed
}

func (rf *Raft) leaderBackLoopDaemon(term int) {
	electionDeadline := time.Now().Add(randElectionTimeoutDuration())
	for {
		if rf.killed() {
			//fmt.Printf("Raft instance killed. Quit\n")
			return
		}
		var stillLeader = false
		var leadId int
		rf.mu.Lock()
		stillLeader = rf.role == Leader && term == rf.currentTerm
		leadId = rf.me
		rf.mu.Unlock()

		if !stillLeader { // check if is leader
			DPrintf(Warning, "No longer the leader. Quit Term %d\n", term)
			return
		}

		if time.Now().After(electionDeadline) {
			DPrintf(Warning, "LeaderBackLoop: Election timemout with no leader, start a new term for election\n")
			rf.mu.Lock()
			rf.role = Follower
			rf.votedFor = -1
			go rf.followerBackLoop()
			rf.mu.Unlock()
			break
		}

		for server, _ := range rf.peers {
			if server == leadId {
				electionDeadline = time.Now().Add(randElectionTimeoutDuration())
				continue
			} else {
				DPrintf(Debug, "Heartbeat to server %v term %v;\n", server, term)
				go rf.doProperAppendEntries(server, term) // it's ok to send AppendEntries even if the server is no longer the true leader
			}
		}
		time.Sleep(heartBeatInterval)
	}

}

func (rf *Raft) followerBackLoop() {
	DPrintf(Debug, "Follower server start loop;\n")
	nowTimeout := randElectionTimeoutDuration()
	for {
		select {
		case recv := <-rf.backLoopChan:
			switch recv {
			case backLoopWakeKill:
				return
			case backLoopRefresh:
				nowTimeout = randElectionTimeoutDuration()
				continue
			default:
				fmt.Printf("Receved an unexpected msg.\n")
			}
		case <-time.After(nowTimeout):
			if rf.killed() {
				DPrintf(Warning, "followerBackLoop: Raft instance killed. Quit\n")
				return
			}
			DPrintf(Debug, "Heartbeat timeout, server start loop\n")
			go rf.startElectDaemon()
			return
		}
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	DPrintf(Debug, "Init raft instance... \n")
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.backLoopChan = make(chan int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.followerBackLoop()
	return rf
}
