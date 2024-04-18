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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
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

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	applyCh      chan ApplyMsg
	status       Status    // Follower Candidate Leader
	votedTime    time.Time //last voted ts
	votedSupport int       // recevie vote num
	// Updated on stable storage before responding to RPCs
	currentTerm int
	votedFor    int //candidateId that received vote in currentterm (or null if none
	logs        []LogEntry

	commitIndex int //index of highest log entry known to be committed (initialized to 0
	lastApplied int //index of highest log entry applied to state machine (initialized to 0

	//Volatile state on leaders (Reinitialized after election)
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int
	//for each server, index of highest log entry known to be replicated on server(initialized to 0
	matchIndex []int
}

type LogEntry struct {
	Command interface{}
	Term    int //  entry was received by leader, first index is 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one)
	LeaderCommit int        //leader’s commitIndex
}
type AppendEntriesReply struct {
	Term      int  //currentTerm, for leader to update itself
	Success   bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int
}

//=============================== 选举部分 =====================================

func (rf *Raft) candidateRequestVote() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logs),
				LastLogTerm:  0, //TODO
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			//Note: 只要还是 Candidate, rpc返回后Term只可能不变或增大(非法)
			//除非一种情况: Candidate->Follwer(term减小)->Candidate
			if rf.status != Candidate || rf.currentTerm != args.Term {
				// fmt.Println("收到了上个term拉选票的回复, 忽略")
				rf.mu.Unlock()
				return
			}
			// rf.currentTerm == args.Term
			if reply.Term > args.Term {
				rf.status = Follower
				rf.votedFor = -1
				rf.votedSupport = 0
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				rf.votedSupport += 1
				if rf.votedSupport > len(rf.peers)/2 {
					fmt.Println("选主成功：", rf.me, " Term: ", rf.currentTerm)
					rf.status = Leader
					rf.votedFor = -1
					rf.votedSupport = 0
					rf.votedTime = time.Now()
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.logs) + 1 //TODO
					}
					rf.matchIndex = make([]int, len(rf.peers))
					rf.matchIndex[rf.me] = len(rf.logs)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 收到新任期的拉票就重置，如果是 Candidate 需回归 Follower 状态
	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.votedSupport = 0
		rf.currentTerm = args.Term
		rf.status = Follower
	}
	// 比较 raft term 的优先级最高
	if rf.currentTerm > args.Term {
		return
	}
	// candidate’s log is at least as up-to-date as receiver’s log
	// 既然candidate term不落后, 那么其log的term是否更新? 若term相同则的index是否更新?
	if len(rf.logs) > 0 {
		lastLogTerm := rf.logs[len(rf.logs)-1].Term
		if lastLogTerm > args.LastLogTerm || lastLogTerm == args.LastLogTerm && len(rf.logs) > args.LastLogIndex {
			return
		}
	}
	// votedFor is null or candidateId
	if rf.votedFor != -1 || rf.votedFor == args.CandidateId {
		return
	}
	// 满足投票条件
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.votedTime = time.Now()
	reply.VoteGranted = true
}

//=============================== 日志增量部分 =====================================

// leader 根据 nextIndex 无限重发未同步日志，附带心跳作用
// 1.自己过时了，转为 follower
// 2.更新 commitIndex 和 nextIndex
func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				LeaderCommit: rf.commitIndex,
			}
			if len(rf.logs) >= rf.nextIndex[server] {
				args.Entries = rf.logs[rf.nextIndex[server]-1:]
				args.PrevLogIndex = rf.nextIndex[server] - 1 // rf.nextIndex[server] > 0
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
				}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			// fmt.Printf("[TIKER-SendHeart-Rf(%v)-To(%v)] args:%+v,curStatus%v\n", rf.me, server, args, rf.status)
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.status != Leader {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.status = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.votedSupport = 0
					rf.votedTime = time.Now()
					return
				}
				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 每次收到一个follower的accept, 遍历一遍所有follower是否超半数同意
					for index := len(rf.logs); index > rf.commitIndex; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if rf.me == i {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}
						if sum > len(rf.peers)/2 {
							rf.commitIndex = index
							break
						}
					}

				} else { // 返回为冲突
					if reply.NextIndex != -1 {
						rf.nextIndex[server] = reply.NextIndex
					}
				}
			}
		}(index)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Printf("[	AppendEntries--Return-Rf(%v)] arg:%+v, reply:%+v\n", rf.me, args, reply)

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.NextIndex = -1
	if args.Term < rf.currentTerm {
		return
	}
	rf.status = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.votedSupport = 0
	rf.votedTime = time.Now()

	// 自身的快照Index比发过来的prevLogIndex还大
	if rf.lastApplied > args.PrevLogIndex {
		reply.NextIndex = rf.lastApplied + 1
		return
	}
	// log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.logs) < args.PrevLogIndex || (args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.NextIndex = rf.lastApplied + 1 //TODO
		return
	}
	reply.Success = true
	//If an existing entry conflicts with a new one (same index but different terms)
	//delete the existing entry and all that follow it
	rf.logs = append(rf.logs[:args.PrevLogIndex], args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
	}
}

//=============================== 定时任务 =====================================
func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 50) // 35
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) electionTicker() {
	rand.Seed(time.Now().UnixNano())
	for !rf.killed() {
		curTime := time.Now()
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(150)+150)) // 100 75

		rf.mu.Lock()
		if rf.votedTime.Before(curTime) && rf.status != Leader {
			rf.status = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.votedSupport = 1

			fmt.Printf("[++++ elect ++++] :Rf[%v] send a election\n", rf.me)
			rf.candidateRequestVote()
			rf.votedTime = time.Now()
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) committedTicker() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 15)
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			messages = append(messages, ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.logs[rf.lastApplied-1].Command,
			})
			//fmt.Printf("[	AppendEntries func-rf(%v)	] commitLog  \n", rf.me)
		}
		rf.mu.Unlock()
		for _, applyMsg := range messages {
			rf.applyCh <- applyMsg
		}
	}
}

//=============================== 对外接口 =====================================

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = Follower
	rf.votedSupport = 0
	rf.votedTime = time.Now()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logs = make([]LogEntry, 0)
	// rf.logs = append(rf.logs, LogEntry{}) // index = len(cur_logs)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTicker()
	go rf.appendTicker()
	go rf.committedTicker()

	return rf
}

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.status != Leader {
		return -1, -1, false
	}
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
	return len(rf.logs), rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	fmt.Printf("[Kill] Rf(%v)", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
