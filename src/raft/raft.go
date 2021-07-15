package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, currentTerm, isleader)
//   start agreement on a new log entry
// rf.GetState() (currentTerm, isLeader)
//   ask a Raft for its current currentTerm, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State string

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	LEADER    State = "LEADER"
	FOLLOWER        = "FOLLOWER"
	CANDIDATE       = "CANDIDATE"
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

	// State of the peer leader, follower or candidate
	state State

	// Current currentTerm of the peer
	currentTerm int

	//Index of voter peer from the peers array
	votedFor int

	logEntries []LogEntry

	commitIndex int
	lastApplied int

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if current term is greater reject the RPC
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If current term is less or if current term is equal and not voted
	if rf.currentTerm < args.Term {
		log.Printf("Voting for the term %d Me :%d, VotedFor: %d, RequestedFrom %d\n", args.Term, rf.me, args.CandidateId, args.CandidateId)
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

}

func (rf *Raft) isLogUpToDate(cLastIndex int, cLastTerm int) bool {
	myLastIndex, myLastTerm := rf.getLastIndex(), rf.getLastTerm()

	if cLastTerm == myLastTerm {
		return cLastIndex >= myLastIndex
	}

	return cLastTerm > myLastTerm
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logEntries) - 1
}

func (rf *Raft) getLastTerm() int {
	if rf.getLastIndex() == -1 {
		return 0
	}
	return rf.logEntries[rf.getLastIndex()].Term
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

type AppendEntriesRequest struct {
	Term            int
	LeaderId        int
	PrevLogIndex    int
	PrevLogTerm     int
	Entries         []LogEntry
	LeaderCommitIdx int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()
	args := AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.getLastIndex(),
		PrevLogTerm:  rf.getLastTerm(),
	}
	rf.mu.Unlock()
	for idx := range rf.peers {
		if idx != rf.me {
			go func(peerIdx int) {
				reply := AppendEntriesReply{}
				rf.peers[peerIdx].Call("Raft.AppendEntries", &args, &reply)
			}(idx)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm <= args.Term {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.ResetElectionTimer()
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = false
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// currentTerm. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	DPrintf("server %v is being killed", rf.me)
	rf.electionTimer.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			DPrintf("server %v election timer fired", rf.me)

			rf.mu.Lock()
			rf.ResetElectionTimer()
			if rf.state == LEADER { // Do not start election on leader
				rf.mu.Unlock()
				break
			}

			DPrintf("server %v starting election", rf.me)

			rf.state = CANDIDATE // set state to candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			// Send vote request
			voteReceived := 1
			voteGranted := 1
			voteResultChan := make(chan bool)
			lastLogTerm := 0

			if len(rf.logEntries) > 0 {
				lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
			}

			voteArgs := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logEntries),
				LastLogTerm:  lastLogTerm,
			}
			rf.mu.Unlock()
			for peer := 0; peer < len(rf.peers); peer++ {
				if peer == rf.me {
					continue
				}
				go func(peerId int) {
					// DPrintf("server %v sending request vote to peer %v", rf.me, peerId)
					voteReply := RequestVoteReply{}
					ok := rf.sendRequestVote(peerId, &voteArgs, &voteReply)
					if ok {
						voteResultChan <- voteReply.VoteGranted
					} else {
						voteResultChan <- false
					}
				}(peer)
			}

			for {
				result := <-voteResultChan
				voteReceived++
				if result {
					voteGranted++
				}
				if voteGranted > len(rf.peers)/2 {
					break
				}
				if voteReceived >= len(rf.peers) {
					break
				}
			}

			// if state changed during election, ignore the couting
			rf.mu.Lock()
			if rf.state != CANDIDATE {
				DPrintf("Server %v is no longer candidate", rf.me)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			// If won election, immediately send out authorities
			if voteGranted > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.state = LEADER
				rf.ResetElectionTimer()
				rf.mu.Unlock()
				DPrintf("server %v won the election for term %v with %v votes", rf.me, rf.currentTerm, voteGranted)
				rf.sendAppendEntries()
			}

		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			DPrintf("server %v in state %v heartbeat ticker fired", rf.me, rf.state)
			state := rf.state
			rf.mu.Unlock()
			if state == LEADER { // Only send heartbeat if is leader
				DPrintf("leader %v heartbeatTicker tick", rf.me)
				rf.sendAppendEntries()
			}
		}
	}
}

func (rf *Raft) ResetElectionTimer() {
	DPrintf("Try to reset election timer for server %v", rf.me)
	if rf.electionTimer.Stop() { // Don't drain the channel, channel read only in ticker()
		DPrintf("Stopped election timer for server %v", rf.me)
		select {
		case <-rf.electionTimer.C:
			DPrintf("Time chanel of election timer for server %v is drained", rf.me)
		default:
			DPrintf("Time chanel of election timer for server %v is empty", rf.me)
		}
		// rf.electionTimerReset <- true
	}

	rf.electionTimer.Reset(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(100 * time.Millisecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
