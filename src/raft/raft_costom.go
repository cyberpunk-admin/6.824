package raft

import (
	"math"
	"time"
)

type ticker struct {
	record  time.Time
	outTime time.Duration
}

func (t *ticker) set() {
	t.record = time.Now()
	t.outTime = getRandTime()
}

func (t *ticker) timeout() bool {
	return time.Now().Add(-1 * t.outTime).After(t.record)
}

func (rf *Raft) pingLoop() {
	for rf.killed() == false {
		if rf.state == leader {
			for i := range rf.peers {
				if i == rf.me {
					rf.electionTimeout.set()
					continue
				}
				go func(i int) {
					args := AppendEntriesArg{
						Term:         rf.term,
						LeaderId:     rf.me,
						PrevLogIndex: 1,
						PrevLogTerm:  0,
					}
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(i, &args, &reply)
				}(i)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) convertToFollower(term int, voteFor int) {
	rf.term = term
	rf.state = follower
	rf.voteFor = voteFor
	rf.persist()
}

func (rf *Raft) convertToCandidate() {
	rf.state = candidate
	rf.term += 1
	rf.voteFor = rf.me
	rf.totalVotes = 1
	rf.electionTimeout.set()
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.logs))
	rf.matchIndex = make([]int, len(rf.logs))
	for i := 0; i < len(rf.logs); i++ {
		rf.nextIndex[i] = len(rf.logs) + 1
		rf.matchIndex[i] = 0
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	ConflictIndex int
	ConflictTerm  int
	Success       bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: got AppendEntries from leader %d, args: %+v, current term: %d, current commitIndex: %d, current log: %v\n",
		rf.me, args.LeaderId, args, rf.term, rf.commitIndex, rf.logs)
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
	} else {
		rf.electionTimeout.set()
		rf.convertToFollower(args.Term, args.LeaderId)
		if args.PrevLogIndex == 0 {
			reply.Term = rf.term
			reply.Success = true
			originEntries := rf.logs
			lastNewEntry := 0
			if len(args.Entries) < len(originEntries) {
				lastNewEntry = len(args.Entries)
				for i := 0; i < len(args.Entries); i++ {
					if args.Entries[i].RevTerm != originEntries[i].RevTerm {
						rf.logs = append(rf.logs[:i], args.Entries[:i]...)
						lastNewEntry = len(rf.logs)
						break
					}
				}
			} else {
				rf.logs = append(rf.logs, args.Entries...)
				lastNewEntry = len(rf.logs)
			}
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < lastNewEntry {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = lastNewEntry
				}
			}
			rf.persist()
			// todo
			return
		}
		if len(rf.logs) < args.PrevLogIndex {
			reply.Term = rf.term
			reply.Success = false
			reply.ConflictIndex = len(rf.logs)
			reply.ConflictTerm = -1
		} else {
			preLogTerm := 0
			if args.PrevLogIndex > 0 {
				preLogTerm = rf.logs[args.PrevLogIndex-1].RevTerm
			}
			if args.PrevLogTerm != preLogTerm {
				reply.Term = rf.term
				reply.Success = false
				reply.ConflictTerm = preLogTerm
				for i := 0; i < len(rf.logs); i++ {
					if rf.logs[i].RevTerm == preLogTerm {
						reply.ConflictIndex = i + 1
						break
					}
				}
			} else {
				reply.Term = rf.term
				reply.Success = true
				originEntries := rf.logs
				lastNewEntries := 0
				if args.PrevLogIndex+len(args.Entries) < len(originEntries) {
					lastNewEntries = args.PrevLogIndex + len(args.Entries)
					for i := 0; i < len(args.Entries); i++ {
						if args.Entries[i].RevTerm != originEntries[args.PrevLogIndex+i].RevTerm {
							rf.logs = append(rf.logs[:args.PrevLogTerm+i], args.Entries[i:]...)
							lastNewEntries = len(rf.logs)
							break
						}
					}
				} else {
					rf.logs = append(rf.logs[:args.PrevLogIndex], args.Entries...)
					lastNewEntries = len(rf.logs)
				}
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntries)))
				}
				rf.persist()
			}
		}
	}
	DPrintf("======= server %d got AppendEntries from leader %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.LeaderId, args, rf.logs, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		if rf.electionTimeout.timeout() {
			DPrintf("%d start election", rf.me)
			rf.mu.Lock()
			rf.convertToCandidate()
			rf.mu.Unlock()

			rf.startRequestVote()
		}
	}
}

func (rf *Raft) startRequestVote() {
	DPrintf("Candidate %d: start sending RequestVote, current log: %v, current term: %d\n", rf.me, rf.logs, rf.term)

	rf.mu.Lock()
	if rf.state != candidate {
		rf.mu.Unlock()
		return
	}

	lastLogIndex := len(rf.logs)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].RevTerm
	}
	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogItem:  lastLogTerm,
	}
	nLeader := 0
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(id, &args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.term {
					rf.convertToFollower(reply.Term, -1)
					rf.mu.Unlock()
					return
				}
				if rf.term != args.Term || rf.state != candidate {
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					rf.totalVotes += 1
					if nLeader == 0 && rf.totalVotes >= len(rf.peers)/2+1 && rf.state == candidate {
						nLeader += 1
						rf.convertToLeader()
						DPrintf("%d is leader\n", rf.me)
					}
				}
				rf.mu.Unlock()
			} else {
				DPrintf("Candidate %d: sending RequestVote to server %d failed\n", rf.me, id)
			}
		}(i)
	}
}
