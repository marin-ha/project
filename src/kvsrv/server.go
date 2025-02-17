package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	cache map[string]string

	// Your definitions here.
	idempotentMap map[string]requestResult
}

type requestResult struct{
	requestId int64
	result string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.cache[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if exist, ret := kv.idempotent(args); exist{
		reply.Value = ret
		return
	}
	kv.cache[args.Key]  = args.Value
	kv.idempotentMap[args.ClientKey] = requestResult{
		requestId: args.RequestKey,
		result: reply.Value,
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if exist, ret := kv.idempotent(args); exist{
		reply.Value = ret
		return
	}
	reply.Value =kv.cache[args.Key]
	kv.cache[args.Key] = fmt.Sprintf("%s%s", reply.Value, args.Value)
	kv.idempotentMap[args.ClientKey] = requestResult{
		requestId: args.RequestKey,
		result: reply.Value,
	}
}

func (kv *KVServer) idempotent(args *PutAppendArgs) (bool, string){
	if reqrst, ok := kv.idempotentMap[args.ClientKey]; ok && reqrst.requestId == args.RequestKey{
		return true, reqrst.result
	}
	return false, "" 
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.cache = make(map[string]string)
	kv.idempotentMap =make(map[string]requestResult)
	// You may need initialization code here.
	return kv
}
