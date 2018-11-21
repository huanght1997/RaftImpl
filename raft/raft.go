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
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state              int       // current state of this server
	votesGranted       int       // the votes this server granted when in election time of this term
	appendEntriesCome  chan bool // When valid AppendEntries received, give it a signal true
	requestVoteReplied chan bool // When valid RequestVote received and processed, give it a signal true
	electionWin        chan bool // When this server wins the election of this term, give it a signal true
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term, -1 for none
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
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int // candidate's term
	CandidateId int // candidate requesting vote
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	msgId := rand.Int()
	DPrintf("%d: (ID=%016X) #%d received RequestVote, Term=%d, CandidateId=%d",
		time.Now().UnixNano(), msgId, rf.me, args.Term, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	// If votedFor is null or candidateId, ..., grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
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
	DPrintf("%d: #%d sent RequestVote to #%d, Term=%d, CandidateId=%d",
		time.Now().UnixNano(), rf.me, server, args.Term, args.CandidateId)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// The response is only useful to Candidate
		DPrintf("%d: #%d received reply for RequestVote from #%d, Term=%d, VoteGranted=%t",
			time.Now().UnixNano(), rf.me, server, reply.Term, reply.VoteGranted)
		if rf.state == Candidate {
			if reply.Term > rf.currentTerm {
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				return ok
			}
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
	Term     int // leader's term
	LeaderId int // so follower can redirect clients
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if ...
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	msgId := rand.Int()
	DPrintf("%d: (ID=%016X) #%d received AppendEntries, Term=%d, LeaderId=%d",
		time.Now().UnixNano(), msgId, rf.me, args.Term, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	reply.Success = true

	defer func() {
		DPrintf("%d: (ID=%016X) #%d replied AppendEntries, Term=%d, Success=%t",
			time.Now().UnixNano(), msgId, rf.me, reply.Term, reply.Success)
	}()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("%d: #%d sent AppendEntries to #%d, Term=%d, LeaderId=%d",
		time.Now().UnixNano(), rf.me, server, args.Term, args.LeaderId)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// The response is only useful to Leader
		DPrintf("%d: #%d received reply for AppendEntries from #%d, Term=%d, Success=%t",
			time.Now().UnixNano(), rf.me, server, reply.Term, reply.Success)
		if rf.state == Leader {
			if reply.Term > rf.currentTerm {
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				return ok
			}
		}
	}
	return ok
}

func (rf *Raft) doAsFollower() {
	DPrintf("%d: #%d is now follower, currentTerm=%d, votedFor=%d",
		time.Now().UnixNano(), rf.me, rf.currentTerm, rf.votedFor)
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
	DPrintf("%d: #%d is now candidate, currentTerm=%d, votedFor=%d",
		time.Now().UnixNano(), rf.me, rf.currentTerm, rf.votedFor)
	rf.mu.Lock() // When we want to read to write multiple members of rf, lock it.
	// On conversion to candidate, start election:
	// Increment currentTerm
	rf.currentTerm++
	// Vote for self
	rf.votedFor = rf.me
	rf.votesGranted = 1
	rf.mu.Unlock() // Reading or writing finished. Release the lock.
	// Send RequestVote RPCs to all other servers
	go func() {
		var args RequestVoteArgs
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
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
		rf.state = Leader
	case <-rf.appendEntriesCome:
		// If AppendEntries RPC received from new leader: convert to follower
		rf.state = Follower
	case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
		// If election timeout elapses: start new election
		// Still candidate, just quit this function, nothing else to do
	}
}

func (rf *Raft) doAsLeader() {
	DPrintf("%d: #%d is now leader, currentTerm=%d, votedFor=%d",
		time.Now().UnixNano(), rf.me, rf.currentTerm, rf.votedFor)
	// Send AppendEntries periodically
	for i := range rf.peers {
		if i != rf.me {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
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
	isLeader := true

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
	// Solve the problem under Windows
	rand.Seed(time.Now().UnixNano() + int64(rf.me))
	go rf.stateLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
