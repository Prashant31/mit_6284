package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ops         map[int]chan Op
	data        map[string]string
	lastApplied map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	ok, op := kv.waitForOp(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	reply.Value = op.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	ok, _ := kv.waitForOp(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

func (kv *KVServer) waitForOp(op Op) (bool, Op) {
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false, op
	}
	kv.mu.Lock()
	opChan, ok := kv.ops[idx]
	if !ok {
		opChan = make(chan Op, 1)
		kv.ops[idx] = opChan
	}
	kv.mu.Unlock()
	select {
	case appliedOP := <-opChan:
		return kv.isSameOp(op, appliedOP), appliedOP
	case <-time.After(600 * time.Millisecond):
		return false, op
	}
}

func (kv *KVServer) isSameOp(issued Op, applied Op) bool {
	return issued.ClientId == applied.ClientId &&
		issued.RequestId == applied.RequestId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) StartApplier() {
	for {
		applyMsg := <-kv.applyCh
		if !applyMsg.CommandValid {
			continue
		}

		index := applyMsg.CommandIndex
		op := applyMsg.Command.(Op)

		kv.mu.Lock()
		if op.Type == "Get" {
			op.Value = kv.data[op.Key]
		} else {
			lastIndex, ok := kv.lastApplied[op.ClientId]
			// !ok means we have not seen this request before and hence we just apply it

			// op.RequestId > lastIndex : This is important check
			// This happens when the raft leader have replicated the log entry but died before reponding to the client
			// Now the new ly elected raft leader has applied that to the state machine
			// But client assumes the server died/timedout etc and send the same request back
			// Now the stage machine should just ignore this entry from the log and should not apply to state machine
			if !ok || op.RequestId > lastIndex {
				if op.Type == "Put" {
					kv.data[op.Key] = op.Value
				} else {
					kv.data[op.Key] = kv.data[op.Key] + op.Value
				}
				kv.lastApplied[op.ClientId] = op.RequestId
			}
		}
		// []ops Acts a storage for the operation
		// In case of duplicate requests we just take the ops from the already computed value and return
		opCh, ok := kv.ops[index]
		if !ok {
			opCh = make(chan Op, 1)
			kv.ops[index] = opCh
		}
		opCh <- op
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.ops = make(map[int]chan Op)
	kv.lastApplied = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.StartApplier()
	return kv
}
