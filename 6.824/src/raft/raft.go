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
	//	"flag"
//	"go/ast"
//	"image"
//	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
//	"syscall"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int32
const (
	Leader State = iota
	Follower
	Candidate
)

var stateNames = []string {"Leader", "Follower", "Candidate"}
func (s State) String() string {
	return stateNames[s]
}

const (
	HeartbeatInterval 		= 	time.Millisecond * 50	// 50 ms
	ElectionTimeoutLower 	=	time.Millisecond * 150  // 150 ms
	ElectionTimeoutUpper	= 	time.Millisecond * 300  // 300 ms
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// Raft
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
	applyCh     chan ApplyMsg
	currentTerm    int
	votedFor       int
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	logWriter      log.Logger

	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, atomic.LoadInt32((*int32)(&rf.state)) == int32(Leader)
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

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted		bool
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.logWriter.Printf("debug: request with term [%d], and current term [%d]", args.Term, rf.currentTerm)
	if rf.currentTerm < args.Term {
		reply.VoteGranted = true
	}
	if rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// check if candidate's log is at least as up-to-date as rf's
		lastLog := rf.getLastLogEntry()
		reply.VoteGranted = args.Term > lastLog.Term || (args.Term == lastLog.Term && args.LastLogIndex > lastLog.Index)
	}

	if reply.VoteGranted {
		rf.follow(args.CandidateId, args.Term)
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

type AppendEntriesArgs struct {
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries 		[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term 		int
	Success		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// rule 1:
	if rf.currentTerm > args.Term {
		// leader's term is old
		reply.Success = false
		return
	}

	// rule 2:
	prevLogEntry := rf.getLogEntry(args.PrevLogIndex)
	if prevLogEntry == nil || prevLogEntry.Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// heartbeat
	entries := args.Entries
	if entries == nil {
		reply.Success = true
		return
	}

	// rule 3, 4
	rf.doAppendEntries(entries)
	rf.follow(args.LeaderId, args.Term)
	reply.Success = true

	// rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, GetLastLogEntry(entries).Index)
	}
	go rf.applySync()
}

func min(lhs int, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
// Start
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	// Your code here (2B).
	term, isLeader := rf.GetState()
	if isLeader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		index = rf.getLastLogEntry().Index + 1
		rf.log = append(rf.log, LogEntry{command, term, index})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}
	return index, term, isLeader
}

// Kill
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <- rf.electionTimer.C :
			rf.mu.Lock()
			// maybe old state is candidate
			rf.updateState(Candidate)
			rf.mu.Unlock()
		case <- rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadcastHeartbeat()
				rf.heartbeatTimer.Reset(HeartbeatInterval)
			} else {
				rf.heartbeatTimer.Stop()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) updateState(state State) {
	oldState := rf.state
	atomic.StoreInt32((*int32)(&rf.state), int32(state))
	switch state {
	case Leader:
		// old state not leader
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = rf.getLastLogEntry().Index + 1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randomElectionTimeout(ElectionTimeoutLower, ElectionTimeoutUpper))
	case Candidate:
		rf.heartbeatTimer.Stop()
		rf.startElection()
		rf.electionTimer.Reset(randomElectionTimeout(ElectionTimeoutLower, ElectionTimeoutUpper))
	}
	rf.logWriter.Printf("update state from [%s] to [%s]", oldState.String(), rf.state.String())
}

func (rf *Raft) startElection() {
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.logWriter.Printf("start election with term[%d]", rf.currentTerm)
	lastLog := rf.getLastLogEntry()
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLog.Index,
		lastLog.Term,
	}
	var nVoteGranted int32
	var nVotNotGranted int32
	var electFinished int32 	// finished 1, not 0
	nVoteGranted = 1
	electFinished = 0
	if len(rf.peers) == 1 {
		rf.updateState(Leader)
		return
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			if atomic.LoadInt32(&electFinished) == 1 ||
				atomic.LoadInt32((*int32)(&rf.state)) == int32(Leader) {
				return
			}
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}
			if reply.VoteGranted {
				atomic.AddInt32(&nVoteGranted, 1)
			} else {
				atomic.AddInt32(&nVotNotGranted, 1)
			}

			if atomic.LoadInt32(&electFinished) == 1 || atomic.LoadInt32((*int32)(&rf.state)) == int32(Leader) {
				return
			}
			nVote := int32(len(rf.peers))
			nMinLeaderVote := int32(len(rf.peers))/2 + 1
			if atomic.LoadInt32(&nVoteGranted) >= nMinLeaderVote {
				atomic.StoreInt32(&electFinished, 1)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// ensure update leader exactly once
				rf.updateState(Leader)
				return
			}
			if nVote - atomic.LoadInt32(&nVotNotGranted) < nMinLeaderVote {
				atomic.StoreInt32(&electFinished, 1)
			}
		}(server)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for server := range rf.peers {
		if rf.me == server {
			continue
		}
		lastLogIndex := rf.getLastLogEntry().Index
		prevLogIndex := rf.nextIndex[server] - 1
		args := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			prevLogIndex,
			rf.getLogEntry(prevLogIndex).Term,
			rf.getLogEntries(prevLogIndex+1, lastLogIndex),
			rf.commitIndex,
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			if !rf.sendAppendEntries(server, &args, &reply) {
				return
			}
			if reply.Success == false {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term <= rf.currentTerm {
					if rf.nextIndex[server] - 1 == prevLogIndex {
						// nextIndex is not changed (valid), back
						// TODO : can optimize
						rf.nextIndex[server] = prevLogIndex - 1
					}
				} else {
					rf.follow(-1, reply.Term)
				}
				return
			}
		}(server)
	}
}

func (rf *Raft) follow(id int, term int) {
	rf.votedFor = id
	rf.currentTerm = term
	rf.updateState(Follower)
}

func (rf *Raft) getLastLogEntry() *LogEntry {
	// need locked; return pointer avoid copy
	return GetLastLogEntry(rf.log)
}

func GetLastLogEntry(log []LogEntry) *LogEntry {
	length := len(log)
	if length == 0 {
		return nil
	}
	return &log[length-1]
}

func (rf *Raft) getLogEntry(index int) *LogEntry{
	return GetLogEntry(rf.log, index)
}

func GetLogEntry(log []LogEntry, index int) *LogEntry {
	idxInLog := index - log[0].Index
	if idxInLog < 0  || idxInLog >= len(log){
		return nil
	}
	return &log[idxInLog]
}

func (rf *Raft) getLogEntries(from int, to int) []LogEntry {
	// [from, to] => [begin, end)
	begin := from - rf.log[0].Index
	end := to - rf.log[0].Index + 1
	if from > to {
		return nil
	}
	entries := make([]LogEntry, end - begin)
	copy(entries, rf.log[begin:end])
	return entries
}

func (rf *Raft) deleteLogEntries(index int) {
	// delete log entries from index
	idxInLog := index - rf.log[0].Index
	rf.log = rf.log[0:idxInLog]
}

func (rf *Raft) doAppendEntries(entries []LogEntry) {
	// TODO add some codes here
	if len(entries) == 0 {
		return
	}

	for i := range entries {
		index, term := entries[i].Index, entries[i].Term
		if logEntry := rf.getLogEntry(index); logEntry == nil || logEntry.Term != term {
			rf.deleteLogEntries(index)
			rf.log = append(rf.log, entries[i:]...)
			return
		}
	}
}

func (rf *Raft) applySync() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
		applyMsg := ApplyMsg{}
		applyMsg.Command = rf.getLogEntry(index)
		applyMsg.CommandIndex = index
		applyMsg.CommandValid = true
		rf.applyCh <- applyMsg
		rf.lastApplied = index
	}
}

func randomElectionTimeout(lower time.Duration, upper time.Duration) time.Duration {
	base := lower.Milliseconds()
	delta := upper.Milliseconds() - lower.Milliseconds()
	ret := base + rand.Int63n(delta)
	return time.Duration(ret) * time.Millisecond
}

// Make
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
	rf.applyCh = applyCh
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.electionTimer = time.NewTimer(randomElectionTimeout(ElectionTimeoutLower, ElectionTimeoutUpper))
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.heartbeatTimer.Stop()
	rf.logWriter = log.Logger{}

	rf.log = make([]LogEntry, 1)
	rf.log[0].Index = 0

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	logFile, _ := os.Create("log" + strconv.Itoa(rf.me))
	rf.logWriter.SetOutput(logFile)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
