package raft

import (
	"fmt"
	"log"
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

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role         int      // leader candidate follower
	backLoopChan chan int // chan talk to the background thread

	// raft persist state
	currentTerm int
	votedFor    int
	log         Log

	// raft common volatile state
	commitIndex int
	lastApplied int

	// leader data
	leaderData *LeaderData

	// apply Chan
	applyCh chan ApplyMsg
}

type LeaderData struct { // will update each term
	nextIndex  []int
	matchIndex []int
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

func (rf *Raft) LogPrintf(format string, a ...interface{}) (n int, err error) {
	s := fmt.Sprintf(format, a...)
	return DPrintf(Info, "[Server %d]: %s", rf.me, s)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	rf.mu.Lock()
	index = rf.log.Get(rf.log.LastIndex()).CommandIndex + 1 // previous Entries command index + 1
	term = rf.currentTerm
	isLeader = rf.role == Leader
	if isLeader {
		rf.log.Append(LogEntry{CommandTypeCommand, command, index, term})
		rf.leaderData.matchIndex[rf.me] = rf.log.LastIndex()
	}
	rf.mu.Unlock()

	if isLeader {
		DPrintf(Info, "Receving Command %+v: nextIndex: %d, term: %d", command,
			index, term)
		for i, _ := range rf.peers {
			if i != rf.me {
				go rf.doProperAppendEntries(i, term)
			}
		}
	}

	return index, term, isLeader
}

// if saw newer term, change to follower
func (rf *Raft) sawTerm(Term int, msg string) (changed bool) {
	changed = false
	becomeFollower := false
	var previous int
	rf.mu.Lock()
	previous = rf.currentTerm
	if Term > rf.currentTerm { // saw a lager term, return to follower
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
		DPrintf(Info, "Saw term %d which is larger than current: %d, %s. Become follower", Term, previous, msg)
	}
	if becomeFollower {
		go rf.followerBackLoop()
	}
	return changed
}

func (rf *Raft) leaderBackLoop(term int) {
	DPrintf(Info, "Leader back loop started")

	for {
		if rf.killed() {
			DPrintf(Info, "Raft instance killed. Quit")
			return
		}

		var stillLeader = false
		rf.mu.Lock()
		stillLeader = rf.role == Leader && term == rf.currentTerm
		rf.mu.Unlock()

		if !stillLeader { // check if is leader
			DPrintf(Info, "No longer the leader. Quit Term %d", term)
			return
		}

		for i, _ := range rf.peers { // send heartbeat to all followers
			if i != rf.me {
				go rf.doProperAppendEntries(i, term) // it's ok to send AppendEntries even if the server is no longer the true leader
			}
		}

		time.Sleep(heartBeatInterval)
	}
}

func (rf *Raft) followerBackLoop() {

	// begin as a follower
	nowTimeout := randElectionTimeoutDuration()
	DPrintf(Info, "Start the follower with timeout %d milliseconds", int64(nowTimeout)/1e6)

	for {
		select {
		case recv := <-rf.backLoopChan:
			switch recv {
			case backLoopWakeKill:
				return
			case backLoopRefresh:
				continue
			default:
				DPrintf(Info, "Received an unexpected msg.")
			}
		case <-time.After(nowTimeout):
			if rf.killed() {
				DPrintf(Info, "Raft instance killed. Quit")
				return
			}
			// follower timeout start a new election, and quit this loop
			DPrintf(Info, "Raft server %d (follower) timeout, start a new election", rf.me)
			go rf.startElectDaemon()
			return
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// setting logger attr
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// append a empty Entries for the initial consistance
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log.Append(LogEntry{CommandTypeNoop, nil, 0, 0})

	rf.backLoopChan = make(chan int)

	// still set committed and applied to 0
	rf.commitIndex = 0 // mark it as commited

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastApplied = rf.log.BaseIndex // set the last applied to the position of snapshot

	// start raft as follower
	go rf.followerBackLoop()
	return rf
}
