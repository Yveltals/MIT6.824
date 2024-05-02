package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key     string
	Value   string
	Command string
	ClerkId int64
	SeqId   int
	Server  int
}

type KVState struct {
	CKs       map[int64]int
	DataSouce map[string]string
}

type ClerkOps struct {
	seqId       int // clerk current seq id
	getCh       chan Op
	putAppendCh chan Op
	msgUniqueId int // rpc waiting msg uid
}

func (ck *ClerkOps) GetCh(command string) chan Op {
	switch command {
	case "Put":
		return ck.putAppendCh
	case "Append":
		return ck.putAppendCh
	default:
		return ck.getCh
	}
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	dataSource  map[string]string
	clerkOpsMap map[int64]*ClerkOps // clerk id to ClerkOps struct
	applyMsgCh  chan raft.ApplyMsg
	persister   *raft.Persister
}

func (kv *KVServer) WaitApplyMsgByCh(ch chan Op, ck *ClerkOps) (Op, Err) {
	startTerm, _ := kv.rf.GetState()
	timer := time.NewTimer(1000 * time.Millisecond)
	for {
		select {
		case Msg := <-ch:
			return Msg, OK
		case <-timer.C:
			curTerm, isLeader := kv.rf.GetState()
			if curTerm != startTerm || !isLeader {
				kv.mu.Lock()
				ck.msgUniqueId = 0
				kv.mu.Unlock()
				return Op{}, ErrWrongLeader
			}
			timer.Reset(1000 * time.Millisecond)
		}
	}
}

func (kv *KVServer) NotifyApplyMsgByCh(ch chan Op, Msg Op) {
	// if notify timeout, then we ignore, because client probably send request to anthor server
	timer := time.NewTimer(200 * time.Millisecond)
	select {
	case ch <- Msg:
		return
	case <-timer.C:
		DPrintf("[KVServer-%d] NotifyApplyMsgByCh Msg=%v, timeout", kv.me, Msg)
		return
	}
}

func (kv *KVServer) GetCk(ckId int64) *ClerkOps {
	ck, found := kv.clerkOpsMap[ckId]
	if !found {
		ck = new(ClerkOps)
		ck.seqId = 0
		ck.getCh = make(chan Op)
		ck.putAppendCh = make(chan Op)
		kv.clerkOpsMap[ckId] = ck
		DPrintf("[KVServer-%d] Init ck %d", kv.me, ckId)
	}
	return kv.clerkOpsMap[ckId]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// step 1 : start a command, check kv is leader, wait raft to commit command
	kv.mu.Lock()
	ck := kv.GetCk(args.ClerkId)
	DPrintf("[KVServer-%d] Received Req Get %v", kv.me, args)
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:     args.Key,
		Command: "Get",
		ClerkId: args.ClerkId,
		SeqId:   args.SeqId,
		Server:  kv.me,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		ck.msgUniqueId = 0
		kv.mu.Unlock()
		return
	}
	DPrintf("[KVServer-%d] Received Req Get %v, waiting logIndex=%d", kv.me, args, logIndex)
	ck.msgUniqueId = logIndex
	kv.mu.Unlock()
	// step 2 : parse op struct
	getMsg, err := kv.WaitApplyMsgByCh(ck.getCh, ck)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KVServer-%d] Received Msg [Get] 	 args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqId, getMsg)
	reply.Err = err
	if err != OK {
		// leadership change, return ErrWrongLeader
		return
	}

	_, foundData := kv.dataSource[getMsg.Key]
	if !foundData {
		reply.Err = ErrNoKey
		return
	} else {
		reply.Value = kv.dataSource[getMsg.Key]
		DPrintf("[KVServer-%d] Excute Get %s is %s", kv.me, getMsg.Key, reply.Value)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// step 1 : start a command, wait raft to commit command
	kv.mu.Lock()
	ck := kv.GetCk(args.ClerkId)
	// already process
	if ck.seqId > args.SeqId {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	DPrintf("[KVServer-%d] Received Req PutAppend %v, SeqId=%d ", kv.me, args, args.SeqId)
	// start a command
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:     args.Key,
		Value:   args.Value,
		Command: args.Op,
		ClerkId: args.ClerkId,
		SeqId:   args.SeqId,
		Server:  kv.me,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ck.msgUniqueId = logIndex
	DPrintf("[KVServer-%d] Received Req PutAppend %v, waiting logIndex=%d", kv.me, args, logIndex)
	kv.mu.Unlock()
	// step 2 : wait the channel
	reply.Err = OK
	Msg, err := kv.WaitApplyMsgByCh(ck.putAppendCh, ck)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KVServer-%d] Recived Msg [PutAppend] from ck.putAppendCh args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqId, Msg)
	reply.Err = err
	if err != OK {
		DPrintf("[KVServer-%d] leader change args=%v, SeqId=%d", kv.me, args, args.SeqId)
		return
	}
}

func (kv *KVServer) processMsg() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.SnapshotValid {
			kv.readKVState(applyMsg.Snapshot)
			continue
		}
		Msg := applyMsg.Command.(Op)
		DPrintf("[KVServer-%d] Received Msg from channel. Msg=%v", kv.me, applyMsg)

		kv.mu.Lock()
		ck := kv.GetCk(Msg.ClerkId)
		// not now process this log
		if Msg.SeqId > ck.seqId {
			DPrintf("[KVServer-%d] Ignore Msg %v, Msg.Index > ck.index=%d", kv.me, applyMsg, ck.seqId)
			kv.mu.Unlock()
			continue
		}

		// check need snapshot or not
		_, isLeader := kv.rf.GetState()
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize()/4 >= kv.maxraftstate {
			DPrintf("[KVServer-%d] size=%d, maxsize=%d, DoSnapshot %v", kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, applyMsg)
			kv.saveKVState(applyMsg.CommandIndex - 1)
		}

		// check need notify or not
		needNotify := ck.msgUniqueId == applyMsg.CommandIndex
		if Msg.Server == kv.me && isLeader && needNotify {
			ck.msgUniqueId = 0
			DPrintf("[KVServer-%d] Process Msg %v finish, ready send to ck.Ch, SeqId=%d isLeader=%v", kv.me, applyMsg, ck.seqId, isLeader)
			kv.NotifyApplyMsgByCh(ck.GetCh(Msg.Command), Msg)
			DPrintf("[KVServer-%d] Process Msg %v Send to Rpc handler finish SeqId=%d isLeader=%v", kv.me, applyMsg, ck.seqId, isLeader)
		}

		if Msg.SeqId < ck.seqId {
			DPrintf("[KVServer-%d] Ignore Msg %v,  Msg.SeqId < ck.seqId", kv.me, applyMsg)
			kv.mu.Unlock()
			continue
		}

		switch Msg.Command {
		case "Put":
			kv.dataSource[Msg.Key] = Msg.Value
			DPrintf("[KVServer-%d] Excute CkId=%d Put Msg=%v, kvdata=%v", kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
		case "Append":
			DPrintf("[KVServer-%d] Excute CkId=%d Append Msg=%v kvdata=%v", kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
			kv.dataSource[Msg.Key] += Msg.Value
		case "Get":
			DPrintf("[KVServer-%d] Excute CkId=%d Get Msg=%v kvdata=%v", kv.me, Msg.ClerkId, applyMsg, kv.dataSource)
		}
		ck.seqId = Msg.SeqId + 1
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("%d Received Kill Command, logsize=%d, kv data=%v", kv.me, kv.persister.RaftStateSize(), kv.dataSource)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readKVState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	DPrintf("[KVServer-%d] read size=%d", kv.me, len(data))
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	cks := make(map[int64]int)
	dataSource := make(map[string]string)
	//var commitIndex int
	if d.Decode(&cks) != nil || d.Decode(&dataSource) != nil {
		DPrintf("[readKVState] decode failed ...")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		for ckId, seqId := range cks {
			ck := kv.GetCk(ckId)
			ck.seqId = seqId
		}
		kv.dataSource = dataSource
		DPrintf("[KVServer-%d] readKVState clerkOpsMap=%v dataSource=%v", kv.me, kv.clerkOpsMap, kv.dataSource)
	}
}

func (kv *KVServer) saveKVState(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	cks := make(map[int64]int)
	for ckId, ck := range kv.clerkOpsMap {
		cks[ckId] = ck.seqId
	}
	e.Encode(cks)
	e.Encode(kv.dataSource)
	kv.rf.Snapshot(index, w.Bytes())
	DPrintf("[KVServer-%d] Size=%d", kv.me, kv.persister.RaftStateSize())
}

// read kv state and raft snapshot
func (kv *KVServer) readState() {
	kv.readKVState(kv.persister.ReadSnapshot())
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg, 1000) // for test3A TestSpeed
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mu.Lock()
	DPrintf("Start KVServer-%d", me)

	kv.dataSource = make(map[string]string)
	kv.clerkOpsMap = make(map[int64]*ClerkOps)
	kv.applyMsgCh = make(chan raft.ApplyMsg, 1000)
	kv.persister = persister
	kv.mu.Unlock()
	kv.readState()
	go kv.processMsg()
	return kv
}
