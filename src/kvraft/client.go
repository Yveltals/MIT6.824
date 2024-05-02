package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	ckId     int64
	seqId    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) allocSeqId() int {
	ck.seqId += 1
	return ck.seqId
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ckId = nrand()
	ck.seqId = -1
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:     key,
		ClerkId: ck.ckId,
		SeqId:   ck.allocSeqId(),
	}
	reply := GetReply{}
	server := ck.leaderId
	for {
		DPrintf("[Clerk-%d] call [Get] request key=%s , SeqId=%d, server=%d", ck.ckId, key, args.SeqId, server%len(ck.servers))
		ok := ck.SendGet(server%len(ck.servers), &args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				server += 1
				DPrintf("[Clerk-%d] ErrWrongLeader, retry server=%d, args=%v", ck.ckId, server%len(ck.servers), args)
				continue
			}
			ck.leaderId = server
			DPrintf("[Clerk-%d] call [Get] response server=%d reply=%v, args=%v", ck.ckId, server%len(ck.servers), reply, args)
			break
		} else {
			server += 1
		}
		time.Sleep(50 * time.Millisecond)
	}

	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkId: ck.ckId,
		SeqId:   ck.allocSeqId(),
	}
	reply := PutAppendReply{}
	server := ck.leaderId
	for {
		DPrintf("[Clerk-%d] call [PutAppend] request key=%s value=%s op=%s, seq=%d, server=%d", ck.ckId, key, value, op, args.SeqId, server%len(ck.servers))
		ok := ck.SendPutAppend(server%len(ck.servers), &args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				server += 1
				time.Sleep(50 * time.Millisecond)
				DPrintf("[Clerk-%d] call [PutAppend] faild, try next server id =%d ... retry args=%v", ck.ckId, server, args)
				continue
			}
			ck.leaderId = server
			DPrintf("[Clerk-%d] call [PutAppend] response server=%d, ... reply = %v, args=%v", ck.ckId, server%len(ck.servers), reply, args)
			break
		} else {
			// Send Request failed ... retry
			server += 1
			DPrintf("[Clerk][PutAppend] %d faild, call result=false, try next server id =%d ... retry reply=%v", ck.ckId, server, reply)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) SendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) SendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
