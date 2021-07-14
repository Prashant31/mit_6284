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

	lastHeartbeat int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER && !rf.killed()
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

func (rf *Raft) StartElection() {
	voteCount := 1
	ch := make(chan *RequestVoteReply)
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.state = CANDIDATE
	log.Printf("Starting election by peer [%d] for term [%d]\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	for idx := range rf.peers {
		if idx != rf.me {
			go func(peerIdx int) {
				reply := &RequestVoteReply{}
				lastLogTerm := 0
				if len(rf.logEntries) != 0 {
					lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
				}
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.logEntries) - 1,
					LastLogTerm:  lastLogTerm,
				}
				log.Printf("Sending Vote Request to peer [%d] for term [%d] from [%d]\n", peerIdx, rf.currentTerm, rf.me)
				ok := rf.sendRequestVote(peerIdx, args, reply)
				if ok {
					ch <- reply
				} else {
					log.Printf("Vote Request Failed to peer [%d] for term [%d] from [%d]\n", peerIdx, rf.currentTerm, rf.me)
				}
			}(idx)
		}

	}

	// TODO : Implement this logic
	for i := 0; i < len(rf.peers)-1; i++ {
		v := <-ch
		if v.VoteGranted {
			voteCount += 1
			if isMajority(rf, voteCount) {
				rf.mu.Lock()
				log.Printf("Peer No. [%d] is voted as leader \n", rf.me)
				rf.state = LEADER
				rf.mu.Unlock()
				go rf.HandleLeaderElection()
				break
			}
		}
	}

	if !isMajority(rf, voteCount) {
		log.Printf("Peer No. [%d] is not voted as leader \n", rf.me)
	}
}

func isMajority(rf *Raft, count int) bool {
	return count >= (len(rf.peers)/2 + 1)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if current term is grater reject the RPC
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If current term is less or if current term is equal and not voted
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && (rf.votedFor < 0 || rf.votedFor == args.CandidateId)) {
		log.Printf("Voting for the term %d Me :%d, VotedFor: %d, RequestedFrom %d\n", args.Term, rf.me, args.CandidateId, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
		return
	} else {
		// Already voted for the term
		log.Printf("Already voted for the term %d Me :%d, VotedFor: %d, RequestedFrom %d\n", args.Term, rf.me, rf.votedFor, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
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

func (rf *Raft) HandleLeaderElection() {
	ch := make(chan *AppendEntriesReply)
	for idx := range rf.peers {
		if idx != rf.me {
			go func(peerIdx int) {
				reply := &AppendEntriesReply{}
				lastLogTerm := 0
				if len(rf.logEntries) != 0 {
					lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
				}
				args := &AppendEntriesRequest{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.logEntries),
					PrevLogTerm:  lastLogTerm,
				}

				ok := rf.sendAppendEntries(peerIdx, args, reply)
				if ok {
					ch <- reply
				}
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
		rf.lastHeartbeat = makeTimestamp()
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartTimeoutTicker(timeoutMs int) {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		elapsed := makeTimestamp() - rf.lastHeartbeat
		if elapsed > int64(timeoutMs) && rf.state == FOLLOWER {
			go rf.StartElection()
			rf.mu.Unlock()
			time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
		} else {
			if rf.state == LEADER {
				go rf.HandleLeaderElection()
			}
			rf.mu.Unlock()
			time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
		}
	}
}

func generateElectionTimeoutMs() int {
	rand.Seed(time.Now().UnixNano())
	min := 500
	max := 800
	return rand.Intn(max-min+1) + min
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
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
	rf.lastHeartbeat = makeTimestamp()
	go rf.StartTimeoutTicker(generateElectionTimeoutMs())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
