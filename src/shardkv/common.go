package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	ClerkId int64
	SeqId   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	ClerkId int64
	SeqId   int
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestMoveShard struct {
	Shards     []int             // shards to join
	Data       map[string]string // kv data of shards
	ConfigNum  int
	RequestMap map[int64]int // filter repeat request
	ClerkId    int64
	SeqId      int
}

type ReplyMoveShard struct {
	Err
}
