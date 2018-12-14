package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const (
	Follower = iota
	Candidate
	Leader
)

const (
	DefaultElectionTimeoutMin   = 150
	DefaultElectionTimeoutMax   = 300
	DefaultElectionTimeoutRange = DefaultElectionTimeoutMax - DefaultElectionTimeoutMin
	DefaultHeartbeatInterval    = 50
	DefaultChannelBufferSize    = 20
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single log entry.
//
type LogEntry struct {
	Term    int         // term number when the entry was received by the leader.
	Command interface{} // a state machine command
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state              int           // current state of this server
	votesGranted       int           // the votes this server granted when in election time of this term
	appendEntriesCome  chan bool     // When valid AppendEntries received, give it a signal true
	requestVoteReplied chan bool     // When valid RequestVote received and processed, give it a signal true
	electionWin        chan bool     // When this server wins the election of this term, give it a signal true
	commandApplied     chan ApplyMsg // When this peer want to apply one command, give it a signal
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term, -1 for none
	log         []LogEntry // log entries
	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) GetLastEntryIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) GetLastEntryTerm() int {
	return rf.log[len(rf.log)-1].Term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	msgId := rand.Int()
	DPrintf("%d: (ID=%016X) #%d received RequestVote, Term=%d, CandidateId=%d, LastLogIndex=%d, LastLogTerm=%d",
		time.Now().UnixNano(), msgId, rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// If RPC request or response contains term T>currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 // new term, reset votedFor
	}

	// If votedFor is null or candidateId,
	// and candidate's log is at least as up-to-date as receiver's log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.GetLastEntryTerm() ||
			args.LastLogTerm == rf.GetLastEntryTerm() && args.LastLogIndex >= rf.GetLastEntryIndex()) {
		rf.state = Follower
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.requestVoteReplied <- true // notify server itself
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	defer func() {
		DPrintf("%d: (ID=%016X) #%d replied RequestVote, Term=%d, VoteGranted=%t",
			time.Now().UnixNano(), msgId, rf.me, reply.Term, reply.VoteGranted)
	}()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d: #%d sent RequestVote to #%d, Term=%d, CandidateId=%d, LastLogIndex=%d, LastLogTerm=%d",
		time.Now().UnixNano(), rf.me, server, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// The response is only useful to Candidate
		DPrintf("%d: #%d received reply for RequestVote from #%d, Term=%d, VoteGranted=%t",
			time.Now().UnixNano(), rf.me, server, reply.Term, reply.VoteGranted)
		if reply.Term > rf.currentTerm {
			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if rf.state == Candidate {
			if reply.VoteGranted {
				rf.votesGranted++
				if rf.votesGranted > len(rf.peers)/2 {
					rf.electionWin <- true // get majority, notify server that it wins election
				}
			}
		}

		return ok
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if matching
	ConflictIndex int  // if unsuccessful, tell leader from what index confliction detected
	ConflictTerm  int  // if conflict, tell leader the term in log conflictIndex
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	msgId := rand.Int()
	DPrintf("%d: (ID=%016X) #%d received AppendEntries, Term=%d, LeaderId=%d, PrevLogIndex=%d, "+
		"PrevLogTerm=%d, Entries=%v(length:%d), LeaderCommit=%d",
		time.Now().UnixNano(), msgId, rf.me, args.Term, args.LeaderId, args.PrevLogIndex,
		args.PrevLogTerm, args.Entries, len(args.Entries), args.LeaderCommit)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// Notify server valid AppendEntries has come
	rf.appendEntriesCome <- true
	// If RPC request or response contains term T>currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 // new term, reset votedFor
	}
	reply.Term = args.Term
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches
	// prevLogTerm
	if rf.GetLastEntryIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		if rf.GetLastEntryIndex() < args.PrevLogIndex {
			// if a follower does not have prevLogIndex in its log
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1 // no entries here, so -1 for invalid
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// if a follower has this index but not the same term
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			// search the first index has the same term conflictTerm
			for i := args.PrevLogIndex - 1; i > 0; i-- {
				if rf.log[args.PrevLogIndex].Term != rf.log[i].Term {
					reply.ConflictIndex = i + 1
					break
				}
			}
		}
		return
	}

	// if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it.
	var i int
	for i = args.PrevLogIndex + 1; i <= rf.GetLastEntryIndex() && i-args.PrevLogIndex-1 < len(args.Entries); i++ {
		if rf.log[i].Term != args.Entries[i-args.PrevLogIndex-1].Term {
			rf.log = rf.log[:i]
			break
		}
	}
	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries[i-args.PrevLogIndex-1:]...)
	rf.persist()
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.GetLastEntryIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.GetLastEntryIndex()
		}
		// commitIndex changed! check apply
		for i = rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				Index:   i,
				Command: rf.log[i].Command,
			}
			rf.commandApplied <- msg
			DPrintf("%d: #%d applied command %v, index %d",
				time.Now().UnixNano(), rf.me, msg.Command, msg.Index)
		}
		rf.lastApplied = rf.commitIndex
	}
	reply.Success = true
	// keep ConflictXxx zero, since we won't check it.

	defer func() {
		DPrintf("%d: (ID=%016X) #%d replied AppendEntries, Term=%d, Success=%t, ConflictIndex=%d, ConflictTerm=%d",
			time.Now().UnixNano(), msgId, rf.me, reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
	}()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("%d: #%d sent AppendEntries to #%d, Term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d,"+
		"Entries=%v(length:%d), LeaderCommit=%d",
		time.Now().UnixNano(), rf.me, server, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
		args.Entries, len(args.Entries), args.LeaderCommit)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// The response is only useful to Leader
		DPrintf("%d: #%d received reply for AppendEntries from #%d, Term=%d, Success=%t, ConflictIndex=%d, ConflictTerm=%d",
			time.Now().UnixNano(), rf.me, server, reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
		if reply.Term > rf.currentTerm {
			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if rf.state == Leader {
			if reply.Success {
				// update nextIndex and matchIndex for follower
				rf.nextIndex[server] = rf.GetLastEntryIndex() + 1
				rf.matchIndex[server] = rf.GetLastEntryIndex()
				// matchIndex changed!
				// if there exists an N such that N > commitIndex
				for N := rf.commitIndex + 1; N < len(rf.log); N++ {
					count := 1
					foundN := false
					for _, v := range rf.matchIndex {
						if v >= N {
							count++
						}
						// majority of matchIndex[i] >= N, and log[N].Term == currentTerm
						if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
							rf.commitIndex = N
							DPrintf("%d: Leader #%d make %v committed.", time.Now().UnixNano(), rf.me, rf.log[rf.commitIndex])
							foundN = true
							break
						}
					}
					if foundN {
						break
					}
				}
				// commitIndex changed! check apply
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{
						Index:   i,
						Command: rf.log[i].Command,
					}
					rf.commandApplied <- msg
					DPrintf("%d: Leader #%d applied command %v, index %d",
						time.Now().UnixNano(), rf.me, msg.Command, msg.Index)
				}
				rf.lastApplied = rf.commitIndex
			} else {
				// failed due to log inconsistency
				if reply.ConflictTerm == -1 {
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					var i int
					for i = len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							rf.nextIndex[server] = i + 1
							break
						}
					}
					if i == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					}
				}
			}
		}
	}
	return ok
}

func (rf *Raft) doAsFollower() {
	DPrintf("%d: #%d is now follower, currentTerm=%d, votedFor=%d, log=%v(length=%d),"+
		"commitIndex=%d, lastApplied=%d",
		time.Now().UnixNano(), rf.me, rf.currentTerm, rf.votedFor, rf.log[1:], rf.GetLastEntryIndex(),
		rf.commitIndex, rf.lastApplied)
	// random election timeout
	electionTimeout := rand.Intn(DefaultElectionTimeoutRange) + DefaultElectionTimeoutMin
	select {
	case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
		// Any other messages not come and election timeout elapsed
		// convert to candidate
		rf.state = Candidate
	case <-rf.requestVoteReplied:
		// When getting this message, this server has voted for another server
		// Quit this function to destroy the timer
	case <-rf.appendEntriesCome:
		// When getting this message, the leader is alive
		// Quit this function to destroy the timer
	}
}

func (rf *Raft) doAsCandidate() {
	DPrintf("%d: #%d is now candidate, currentTerm=%d, votedFor=%d, log=%v(length=%d),"+
		"commitIndex=%d, lastApplied=%d",
		time.Now().UnixNano(), rf.me, rf.currentTerm, rf.votedFor, rf.log[1:], rf.GetLastEntryIndex(),
		rf.commitIndex, rf.lastApplied)
	rf.mu.Lock() // When we want to read to write multiple members of rf, lock it.
	// On conversion to candidate, start election:
	// Increment currentTerm
	rf.currentTerm++
	// Vote for self
	rf.votedFor = rf.me
	rf.votesGranted = 1
	rf.persist()
	rf.mu.Unlock() // Reading or writing finished. Release the lock.
	// Send RequestVote RPCs to all other servers
	go func() {
		var args RequestVoteArgs
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogTerm = rf.GetLastEntryTerm()
		args.LastLogIndex = rf.GetLastEntryIndex()
		rf.mu.Unlock()

		for i := range rf.peers {
			if i != rf.me {
				var reply RequestVoteReply
				go rf.sendRequestVote(i, args, &reply)
			}
		}
	}()
	// random election timeout
	electionTimeout := rand.Intn(DefaultElectionTimeoutRange) + DefaultElectionTimeoutMin
	select {
	case <-rf.electionWin:
		// If votes received majority of servers: become leader
		rf.mu.Lock()
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.GetLastEntryIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
	case <-rf.appendEntriesCome:
		// If AppendEntries RPC received from new leader: convert to follower
		rf.state = Follower
	case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
		// If election timeout elapses: start new election
		// Still candidate, just quit this function, nothing else to do
	}
}

func (rf *Raft) doAsLeader() {
	DPrintf("%d: #%d is now leader, currentTerm=%d, votedFor=%d, log=%v(length=%d),"+
		"commitIndex=%d, lastApplied=%d, nextIndex=%v, matchIndex=%v",
		time.Now().UnixNano(), rf.me, rf.currentTerm, rf.votedFor, rf.log[1:], rf.GetLastEntryIndex(),
		rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
	// Send AppendEntries periodically
	for i := range rf.peers {
		if i != rf.me {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
			copy(args.Entries, rf.log[rf.nextIndex[i]:])
			args.LeaderCommit = rf.commitIndex
			var reply AppendEntriesReply
			go rf.sendAppendEntries(i, args, &reply)
		}
	}
	// You know, periodically.
	time.Sleep(DefaultHeartbeatInterval * time.Millisecond)
}

func (rf *Raft) stateLoop() {
	for {
		switch rf.state {
		case Follower:
			rf.doAsFollower()
		case Candidate:
			rf.doAsCandidate()
		case Leader:
			rf.doAsLeader()
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.state == Leader
	if isLeader {
		rf.mu.Lock()
		DPrintf("%d: #%d received command %v from client.",
			time.Now().UnixNano(), rf.me, command)
		// Append entry to local log
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.persist()
		index = rf.GetLastEntryIndex()
		term = rf.currentTerm
		DPrintf("%d: #%d tell client command %v will put to index %d, term %d",
			time.Now().UnixNano(), command, index, term)
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votesGranted = 0
	rf.appendEntriesCome = make(chan bool, DefaultChannelBufferSize)
	rf.requestVoteReplied = make(chan bool, DefaultChannelBufferSize)
	rf.electionWin = make(chan bool, DefaultChannelBufferSize)
	rf.commandApplied = applyCh

	rf.log = []LogEntry{{Term: 0, Command: -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Solve the problem under Windows
	rand.Seed(time.Now().UnixNano() + int64(rf.me))
	go rf.stateLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
