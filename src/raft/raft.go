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
	"fmt"
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

func (le *LogEntry) String() string {
	return fmt.Sprintf("Term :%d, %v\n", le.Term, le.Command)
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

	nextIndex  []int
	matchIndex []int

	applyChannel chan ApplyMsg
	heartbeat    time.Time
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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		log.Printf("Current Term Greater than Args Term Me %d Term %d Candidate %d Term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		log.Printf("Me %v Term %v Voting for %v ArgsLastLogIndex: %v, ArgsLastLogTerm %v, LenLogEntries %v\n", rf.me, rf.currentTerm, args.CandidateId, args.LastLogIndex, args.LastLogTerm, len(rf.logEntries))
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		return
	}
	//log.Printf("Did Noting in Voting Me %v Term %v State %v VotedFor %v, LastLog %v, LastLogTerm %v, ArgsLastLogIndex %v, ArgsLastLogTerm %v, Requester %v\n", rf.me, rf.currentTerm, rf.state, rf.votedFor, rf.getLastIndex(), rf.getLastTerm(), args.LastLogIndex, args.LastLogTerm, args.CandidateId)
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

func (aer *AppendEntriesRequest) String() string {
	return fmt.Sprintf("Term :%d, LeaderId %d, PrevLogIndex: %d, PrevLogTerm: %d, LenEntries: %d", aer.Term, aer.LeaderId, aer.PrevLogIndex, aer.PrevLogTerm, len(aer.Entries))
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(heartbeat bool) {
	for idx := range rf.peers {
		if idx != rf.me {
			if rf.getLastIndex() >= rf.nextIndex[idx] || heartbeat {
				args := AppendEntriesRequest{
					Term:            rf.currentTerm,
					LeaderId:        rf.me,
					PrevLogIndex:    rf.nextIndex[idx] - 1,
					PrevLogTerm:     rf.logEntries[rf.nextIndex[idx]-1].Term,
					LeaderCommitIdx: rf.commitIndex,
				}
				entries := rf.logEntries[rf.nextIndex[idx]:]
				args.Entries = make([]LogEntry, len(entries))
				copy(args.Entries, entries)
				go rf.sendAppendEntry(idx, &args)
			}
		}
	}
}

func (rf *Raft) sendAppendEntry(peerIdx int, args *AppendEntriesRequest) {
	reply := AppendEntriesReply{}
	ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != LEADER || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
			// If not leader throw away the response
			return
		}
		if rf.currentTerm < reply.Term {
			rf.convertToFollower(reply.Term)
			return
		}
		if reply.Success {
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[peerIdx] {
				rf.matchIndex[peerIdx] = newMatchIndex
			}
			rf.nextIndex[peerIdx] = newMatchIndex + 1
			//log.Printf("Append Enrtries SUCCESS Me:%d, peer %d, NextIndex: %d, MatchIndex: %d, LogEntries: %d\n", rf.me, peerIdx, rf.nextIndex[peerIdx], rf.matchIndex[peerIdx], rf.logEntries)
		} else {
			//log.Printf("Append NotSuccessful Retrying Me:%d, State: %s, NextIndex: %d, MatchIndex: %d, CommitIdx: %d\n", rf.me, rf.state, rf.nextIndex[peerIdx], rf.matchIndex[peerIdx], rf.commitIndex)
			rf.nextIndex[peerIdx] -= 1
		}
		rf.applyLogs()
	}
}

func (rf *Raft) applyLogs() {
	// if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N
	for n := rf.getLastIndex(); n >= rf.commitIndex; n-- {
		count := 1
		if rf.logEntries[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.ApplyMessages()
			break
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.state == LEADER) {
		reply.Term = rf.currentTerm
		reply.Success = false
		//log.Printf("Returning False ME: %d, MyTerm %d, LogEntries %v, AppendEntried %v\n", rf.me, rf.currentTerm, rf.logEntries, args)
		return
	}
	//log.Printf("Me : %d, Current Term %d, Append Request %s\n", rf.me, rf.currentTerm, args.String())
	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term)
	} else {
		rf.setElectionTimer()
	}

	reply.Term = rf.currentTerm
	if args.PrevLogIndex > rf.getLastIndex() || args.PrevLogTerm != rf.logEntries[args.PrevLogIndex].Term {
		reply.Success = false
	} else {
		rf.logEntries = append(rf.logEntries[:args.PrevLogIndex+1], args.Entries...)
		reply.Success = true
	}

	if args.LeaderCommitIdx > rf.commitIndex {
		if args.LeaderCommitIdx > rf.getLastIndex() {
			rf.commitIndex = rf.getLastIndex()
		} else {
			rf.commitIndex = args.LeaderCommitIdx
		}
		go rf.ApplyMessages()
	}
}

func (rf *Raft) ApplyMessages() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastApplied >= rf.commitIndex {
		return
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyChannel <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
	//log.Printf("Applied Log Entries Me: %v, CommitIdx: %v, LastApplied %v LenLogs: %v\n", rf.me, rf.commitIndex, rf.lastApplied, len(rf.logEntries))
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logEntries = append(rf.logEntries, entry)

	//log.Printf("Me %d Logs %v Commanf Received : %v\n", rf.me, entry, rf.logEntries)
	rf.matchIndex[rf.me] += 1
	rf.nextIndex[rf.me] += 1
	rf.sendAppendEntries(false)
	//log.Printf("Sending append Entries For Log")
	return rf.getLastIndex(), rf.currentTerm, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startServer() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		switch state {
		case LEADER:
			rf.setElectionTimer()
			rf.sendAppendEntries(true)
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		case FOLLOWER, CANDIDATE:
			if time.Now().After(rf.heartbeat) {
				rf.setElectionTimer()
				go rf.startElection()
			}
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}

}

func (rf *Raft) setElectionTimer() {
	rf.heartbeat = time.Now().Add(time.Duration(600+rand.Int63()%300) * time.Millisecond)
}

// Should be called with lock held
func (rf *Raft) convertToCandidate() {
	rf.state = CANDIDATE // set state to candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.setElectionTimer()
}

//Should be called with lock held
func (rf *Raft) convertToFollower(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.setElectionTimer()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// Already Leader No need for election
	if rf.state == LEADER {
		rf.mu.Unlock()
		return
	}
	rf.convertToCandidate()
	log.Printf("Starting Election Me %d, Term %d\n", rf.me, rf.currentTerm)
	voteReceived := 1
	voteGranted := 1
	voteResultChan := make(chan bool)
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
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
	defer rf.mu.Unlock()
	log.Printf("Election process Completed for Me %d, Myterm %d ArgsTerm %d VoteGranted %d, VoteReceived %d\n", rf.me, rf.currentTerm, voteArgs.Term, voteGranted, voteReceived)

	if rf.state != CANDIDATE {
		log.Printf("Server %v is no longer candidate", rf.me)
		return
	}

	// If won election, immediately send out authorities
	if voteGranted > len(rf.peers)/2 {
		log.Printf("Me %v won the election for term %v Logs %v CommitIdx %v\n", rf.me, rf.currentTerm, rf.logEntries, rf.commitIndex)
		rf.state = LEADER
		//for i:= range rf.peers{
		//	rf.nextIndex[i] = rf.getLastIndex() + 1
		//}
		rf.sendAppendEntries(true)
	}

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
	rf.setElectionTimer()
	rf.applyChannel = applyCh

	rf.logEntries = []LogEntry{{
		Command: nil,
		Term:    0,
	}}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logEntries)
		rf.matchIndex[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startServer()

	return rf
}
