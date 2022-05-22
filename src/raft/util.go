package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const (
	Info = iota
	Warning
	Debug
	Test
)

const isOpen = 1

func DPrintf(logLevel int, format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if isOpen > 0 {
		switch logLevel {
		case Info:
			log.Printf("[Info]: "+format, a...)
		case Warning:
			log.Printf("[Warning]: "+format, a...)
		case Debug:
			log.Printf("[Debug]: "+format, a...)
		case Test:
			log.Printf("[test]: "+format, a...)
		default:
			return
		}
	} else {
		if logLevel == Test {
			log.Printf("[test]: "+format, a...)
		}
	}
	return
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randElectionTimeoutDuration() time.Duration {
	return time.Duration(250+rand.Int31n(150)) * time.Millisecond
}
