package raft

import "log"

type commandT interface{}

type ApplyMsg struct {
	CommandValid bool
	CommandType  int
	Command      commandT
	CommandIndex int // command index for valid command only
	Term         int
	LogIndex     int // for all kinds of commands' positions in the log
}

func (rf *Raft) applyMsg() {
	rf.mu.Lock()
	i := rf.lastApplied + 1
	if rf.commitIndex > rf.log.LastIndex() {
		DPrintf(Info, "bad commit index! commitIndex %d, log len %d", rf.commitIndex, rf.log.LastIndex()+1)
	}
	DPrintf(Info, "start sending index %d this log is to detect blocking time", i)
	for ; i <= rf.commitIndex; i++ {
		entry := *rf.log.Get(i)
		var msg ApplyMsg
		if entry.CommandType == CommandTypeCommand {
			msg = ApplyMsg{true, entry.CommandType, entry.Command, entry.CommandIndex, entry.Term, i}
		} else if entry.CommandType == CommandTypeSnapshot {
			log.Fatalf("Should never apply snapshot type command!")
		} else {
			msg = ApplyMsg{false, entry.CommandType, entry.Command, -1, entry.Term, i}
		}
		rf.applyCh <- msg
		DPrintf(Info, "send index %d, commit index %d, msg: %+v", i, rf.commitIndex, msg)
	}
	rf.lastApplied = i - 1
	rf.mu.Unlock()
}
