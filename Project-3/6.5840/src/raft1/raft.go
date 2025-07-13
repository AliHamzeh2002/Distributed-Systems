package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type State = int

const (
	Follower State = iota
	Candidate
	Leader
)

const NotVoted = -1

const HeartBeatDelay = 120 // milliseconds
const InitialTerm = 0

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *tester.Persister   // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	applyCh     chan raftapi.ApplyMsg
	applierCond *sync.Cond

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// these are not in article, i dont know if should be persistent (i think not)
	gotPulse  bool
	state     State
	voteCount int

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// for snapshotting
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte // for caching the snapshot
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var vote int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&term) != nil ||
		d.Decode(&vote) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return // error reading persist data
	}

	rf.currentTerm = term
	rf.votedFor = vote
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	// **reset volatile state to snapshot point**
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

func (rf *Raft) applySnapshot() {
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.lastIncludedIndex || index > rf.commitIndex || index > rf.lastApplied {
		panic("I THOUGHT Raft would never be asked to snapshot an index that is less than lastIncludedIndex or greater than commitIndex or lastApplied")
	}

	// if index is 10 and lastIncludedIndex is 5,
	// and len(rf.log) is 15, then we need to keep
	// log entries 11-15
	arrayIndex := index - rf.lastIncludedIndex
	term := rf.log[arrayIndex].Term

	newLog := make([]LogEntry, len(rf.log)-(arrayIndex))
	newLog[0] = LogEntry{Term: term, Command: nil} // initial empty log entry
	if arrayIndex+1 < len(rf.log) {
		copy(newLog[1:], rf.log[arrayIndex+1:])
	}

	rf.log = newLog

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	rf.snapshot = snapshot
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) isCandidateLogsUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastIndex, myLastTerm := rf.getLastLogIndexAndTerm()

	if lastLogTerm > myLastTerm {
		return true
	}

	if lastLogTerm == myLastTerm && lastLogIndex >= myLastIndex {
		return true
	}

	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm

	if rf.votedFor != NotVoted && rf.votedFor != args.CandidateId { //TODO : check
		reply.VoteGranted = false
		return
	}

	isUpToDate := rf.isCandidateLogsUpToDate(args.LastLogIndex, args.LastLogTerm)

	if !isUpToDate || args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
}

type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // term in the conflicting entry (if any)
	ConflictIndex int  // index of first entry with that term (if any)
	XLen          int  // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//If AppendEntries RPC received from new leader: convert to
	// follower
	rf.state = Follower
	rf.gotPulse = true
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = true

	// our snapshot is further than lastIncludedIndex 
	// so we send -1 so the leader send its snapshot to make us match
	if args.PrevLogIndex - rf.lastIncludedIndex < 0{
		reply.XLen = -1
		return
	}

	// Reply false if log doesn’t contain an entry
	// at prevLogIndex whose term matches prevLogTerm (§5.3)
	if !rf.isLogMatching(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		reply.XLen = len(rf.log) + rf.lastIncludedIndex // Amirali:TODO we should change this
		reply.ConflictIndex, reply.ConflictTerm = rf.findConflictData(args.PrevLogIndex)
		return
	}

	// If an existing entry conflicts with a new one
	// (same index but different terms), delete the
	// existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	startIndex := args.PrevLogIndex + 1
	arrayIndex := startIndex - rf.lastIncludedIndex
	i := 0
	for ; i < len(args.Entries); i++ {
		if arrayIndex+i >= len(rf.log) {
			break
		}
		if rf.log[arrayIndex+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:arrayIndex+i]
			break
		}
	}
	rf.log = append(rf.log, args.Entries[i:]...)

	//If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		rf.applierCond.Signal()
	}

}

func (rf *Raft) findConflictData(prevLogIndex int) (int, int) {
	if prevLogIndex-rf.lastIncludedIndex >= len(rf.log) {
		return -1, -1
	}
	conflictTerm := rf.log[prevLogIndex-rf.lastIncludedIndex].Term
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term == conflictTerm {
			return i + rf.lastIncludedIndex, conflictTerm
		}
	}
	return -1, -1
}

func (rf *Raft) isLogMatching(index int, term int) bool {
	if index < 0 || index-rf.lastIncludedIndex >= len(rf.log) {
		return false
	}
	return rf.log[index-rf.lastIncludedIndex].Term == term
}

// func (rf *Raft) applyEntries(){
// 	rf.mu.Lock()
// 	commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
// 	entries := make([]LogEntry, commitIndex-lastApplied)
// 	copy(entries, rf.log[lastApplied+1:commitIndex+1])
// 	startIndex := lastApplied + 1
// 	rf.lastApplied = commitIndex
// 	rf.mu.Unlock()

// 	for i, entry := range entries{
// 		rf.applyCh <- raftapi.ApplyMsg{
// 			CommandValid: true,
// 			Command:      entry.Command,
// 			CommandIndex: startIndex + i,
// 		}
// 	}
// }

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applierCond.Wait()
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied-rf.lastIncludedIndex+1:commitIndex-rf.lastIncludedIndex+1])
		startIndex := lastApplied + 1
		rf.lastApplied = commitIndex
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIndex + i,
			}
		}

	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	//fmt.Println("entered", server, rf.me)
	//defer fmt.Println("returned")
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.updateTerm(reply.Term)

	if rf.state != Leader || rf.killed() {
		return
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = len(args.Entries) + args.PrevLogIndex + 1
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		for i := rf.commitIndex + 1; i < rf.lastIncludedIndex+len(rf.log); i++ {
			count := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[i-rf.lastIncludedIndex].Term == rf.currentTerm {
				rf.commitIndex = i
			}
		}
		rf.applierCond.Signal()
	} else if args.Term >= reply.Term {
		if reply.XLen <= args.PrevLogIndex {
			rf.nextIndex[server] = reply.XLen // Amirali:TODO we should change this
		} else if rf.hasTerm(reply.ConflictTerm) {
			rf.nextIndex[server] = rf.findLastEntryIndex(reply.ConflictTerm) + 1
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}

		if rf.nextIndex[server] <= rf.lastIncludedIndex {

			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Snapshot:          rf.snapshot,
			}

			go rf.sendInstallSnapshot(server, args)
			return
		}

		prevLogIndex, prevLogTerm := rf.nextIndex[server]-1, rf.log[rf.nextIndex[server]-rf.lastIncludedIndex-1].Term
		sendEntries := make([]LogEntry, len(rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:]))
		copy(sendEntries, rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:])

		newArgs := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      sendEntries,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendAppendEntries(server, newArgs)
	}
}

func (rf *Raft) hasTerm(term int) bool {
	for _, entry := range rf.log {
		if entry.Term == term {
			return true
		}
	}
	return false
}

func (rf *Raft) findLastEntryIndex(term int) int {
	lastEntryIndex := -1
	for i, entry := range rf.log {
		if entry.Term == term {
			lastEntryIndex = i
		}
	}

	return lastEntryIndex + rf.lastIncludedIndex
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	term, isLeader := rf.currentTerm, rf.state == Leader
	if !isLeader {
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})

	index := len(rf.log) + rf.lastIncludedIndex - 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Snapshot:          rf.snapshot,
			}
			go rf.sendInstallSnapshot(i, args)
			continue
		}

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.log[rf.nextIndex[i]-rf.lastIncludedIndex-1].Term
		sendEntries := make([]LogEntry, len(rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:]))
		copy(sendEntries, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:])

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      sendEntries,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}

		go rf.sendAppendEntries(i, args)

	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		needElection := (rf.state != Leader) && !rf.gotPulse
		rf.gotPulse = false
		rf.mu.Unlock()

		if needElection {
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 350 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.state = Candidate
	rf.persist()

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.prepareAndSendRequestVote(i)
	}
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = NotVoted
		rf.state = Follower
		rf.persist()
	}
}

func (rf *Raft) prepareAndSendRequestVote(server int) {
	rf.mu.Lock()

	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	reply := &RequestVoteReply{}

	if !rf.sendRequestVote(server, args, reply) {
		return // RPC failed, don't do anything
	}

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(reply.Term)

	if rf.state != Candidate {
		return
	}

	if reply.VoteGranted && reply.Term == rf.currentTerm {
		rf.voteCount++

		if rf.voteCount > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex, _ := rf.getLastLogIndexAndTerm()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	go rf.heartbeatTicker()
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.sendHeartbeat()

		rf.mu.Unlock()

		time.Sleep(time.Duration(HeartBeatDelay) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Snapshot:          rf.snapshot,
			}
			go rf.sendInstallSnapshot(i, args)
			continue
		}

		prevLogIndex, prevLogTerm := rf.nextIndex[i]-1, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex-1].Term

		sendEntries := make([]LogEntry, len(rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:]))
		copy(sendEntries, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:])

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      sendEntries,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendAppendEntries(i, args)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applierCond = sync.NewCond(&rf.mu)
	rf.state = Follower // initial state
	rf.gotPulse = true  // to avoid starting an election immediately
	rf.currentTerm = InitialTerm
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// Your initialization code here (3A, 3B, 3C).
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil}) // initial empty log entry

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	snapshot := persister.ReadSnapshot()
	if len(snapshot) != 0 {
		rf.snapshot = make([]byte, len(snapshot))
		copy(rf.snapshot, snapshot)
		rf.persist()
		go rf.applySnapshot()
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // index of the last included entry in the snapshot
	LastIncludedTerm  int    // term of the last included entry in the snapshot
	Snapshot          []byte // raw bytes of the snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        return
    }

    if args.LastIncludedIndex <= rf.commitIndex {
        return
    }

    rf.updateTerm(args.Term)
    rf.state = Follower
    rf.gotPulse = true

    removeUntil := args.LastIncludedIndex - rf.lastIncludedIndex
    if removeUntil >= len(rf.log) {
        // Snapshot covers entire log or beyond; discard log and start fresh.
        rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
    } else if removeUntil > 0 {
        // Snapshot covers part of log; keep entries after LastIncludedIndex.
        newLog := make([]LogEntry, len(rf.log)-removeUntil)
        newLog[0] = LogEntry{Term: args.LastIncludedTerm, Command: nil}
        copy(newLog[1:], rf.log[removeUntil+1:])
        rf.log = newLog
    } // else: snapshot aligns with current log start, no change needed

    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm
    rf.snapshot = make([]byte, len(args.Snapshot))
    copy(rf.snapshot, args.Snapshot)
    rf.persist()

    rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
    rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

    go rf.applySnapshot()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok || rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(reply.Term)

	// TODO: i think here we might send AppendEntries
}
