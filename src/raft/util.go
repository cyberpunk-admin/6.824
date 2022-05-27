package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func getRandTime() time.Duration {
	ms := 100 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}
