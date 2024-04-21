package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

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
	// 2D中用于传入快照点
	lastIncludeIndex int
	lastIncludeTerm  int
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
// called after [currentTerm, votedFor, logs] change
//
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	//fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	return data
}
func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
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

type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期
	LeaderId         int    // 请求方的LeaderId
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
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
				LastLogIndex: rf.getLastIndex(),
				LastLogTerm:  rf.getLastTerm(),
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			fmt.Printf("[Return-Rf(%v)] arg:%+v, reply:%+v\n", rf.me, args, reply)
			rf.mu.Lock()
			//Note: 只要还是 Candidate, rpc返回后Term只可能不变或增大(非法)
			//除非一种情况: Candidate->Follwer(term减小)->Candidate
			if rf.status != Candidate || rf.currentTerm != args.Term {
				// fmt.Println("收到了上个term拉选票的回复, 忽略")
				rf.mu.Unlock()
				return
			}
			// 以下 rf.currentTerm == args.Term 成立
			if reply.Term > args.Term {
				fmt.Printf("[RF(%v)]  ->  follwer\n", rf.me)
				rf.status = Follower
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				rf.votedSupport += 1
				if rf.votedSupport > len(rf.peers)/2 {
					fmt.Printf("选主成功：%v  Term %v\n", rf.me, rf.currentTerm)
					rf.status = Leader
					//TODO send heartbeats
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.getLastIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					rf.matchIndex[rf.me] = rf.getLastIndex()
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 比较 raft term 的优先级最高
	if rf.currentTerm > args.Term || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// 收到新任期的拉票就重置，如果是 Candidate 需回归 Follower 状态
	if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.status = Follower
		fmt.Println("重置选票")
	}
	// candidate’s log is at least as up-to-date as receiver’s log
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.votedTime = time.Now()
	reply.Term, reply.VoteGranted = rf.currentTerm, true
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
			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}
			if rf.getLastIndex() >= rf.nextIndex[server] {
				args.Entries = rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex-1:]
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
					fmt.Printf("[Rf(%v)] ---> follwer\n", rf.me)
					rf.status = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					// rf.votedTime = time.Now() //remove?
					return
				}
				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 每次收到一个follower的accept, 遍历一遍所有follower是否超半数同意
					//												TODO: index > rf.lastIncludeIndex+1
					for index := rf.getLastIndex(); index > rf.commitIndex; index-- {
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
						if sum > len(rf.peers)/2 && rf.restoreLogTerm(index) == rf.currentTerm {
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
	defer rf.persist()
	// defer fmt.Printf("[	AppendEntries--Return-Rf(%v)] arg:%+v, reply:%+v\n", rf.me, args, reply)

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.NextIndex = -1
	if rf.currentTerm > args.Term {
		return
	}
	rf.status = Follower
	rf.currentTerm = args.Term
	if rf.currentTerm < args.Term {
		rf.votedFor = -1
	}
	rf.votedTime = time.Now()
	// 自身的快照Index比发过来的prevLogIndex还大
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	// log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.NextIndex = rf.getLastIndex()
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastIncludeIndex; index-- {
				if rf.restoreLogTerm(index) != tempTerm {
					reply.NextIndex = index + 1
					break
				}
			}
			return
		}
	}
	reply.Success = true
	//If an existing entry conflicts with a new one (same index but different terms)
	//delete the existing entry and all that follow it
	//TODO 不一定每次都强行截断
	rf.logs = append(rf.logs[:args.PrevLogIndex-rf.lastIncludeIndex], args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
}

//----------------------------------------------日志压缩(快照）部分---------------------------------------------------------
func (rf *Raft) leaderSendSnapShot(server int) {

	rf.mu.Lock()
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendSnapShot(server, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.status != Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	// 如果返回的term比自己大说明自身数据已经不合适了
	if reply.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm, rf.votedFor = reply.Term, -1
		rf.persist()
		// rf.votedTime = time.Now()
		rf.mu.Unlock()
		return
	}
	rf.matchIndex[server] = args.LastIncludeIndex
	rf.nextIndex[server] = args.LastIncludeIndex + 1
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.status = Follower
	rf.votedFor = -1
	rf.persist()
	rf.votedTime = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.logs = tempLog

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg

}

// 截断index前(含)的
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("[Snapshot] commintIndex", rf.commitIndex)
	if rf.lastIncludeIndex >= index || index > rf.commitIndex { //apllyied idx?
		return
	}
	// 更新快照日志
	sLogs := make([]LogEntry, 0)
	for i := index + 1; i <= rf.getLastIndex(); i++ { // 逻辑index
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	//fmt.Printf("[Snapshot-Rf(%v)]rf.commitIndex:%v,index:%v\n", rf.me, rf.commitIndex, index)
	// 更新快照下标/任期
	// index <= commitIndex <= getLastIndex(logs_len+ include_len)
	if index == rf.getLastIndex() { //全部snapshot
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}
	rf.lastIncludeIndex = index
	rf.logs = sLogs

	if index > rf.lastApplied {
		fmt.Println("\nQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ")
		rf.lastApplied = index
	}

	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

//=============================== 定时任务 =====================================
func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 100) // 35
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
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(150)+300)) // 100 75

		rf.mu.Lock()
		if rf.votedTime.Before(curTime) && rf.status != Leader {
			rf.status = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.votedSupport = 1
			rf.persist()
			fmt.Printf("[++++ elect ++++]: Rf[%v] send a election\n", rf.me)
			rf.votedTime = time.Now()
			rf.candidateRequestVote()
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
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			messages = append(messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Command,
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
	rf.lastIncludeTerm = 0
	rf.lastIncludeIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logs = make([]LogEntry, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 同步快照信息 TODO
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}
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
	rf.persist()
	return len(rf.logs), rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
