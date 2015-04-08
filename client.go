package pbservice

import "viewservice"
import "net/rpc"
import "sync"
import "fmt"
import "time"
import "crypto/rand"
import "math/big"


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	view viewservice.View
	mu sync.Mutex
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	go func() {
		time.Sleep(3 * time.Second)
		ck.UpdateCache()
	}()
	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	// fmt.Println("starting GET RPC from", ck.vs.Primary(), key)
	callSucceeded := false
	args, reply := GetArgs{Key: key}, GetReply{}
	for callSucceeded != true {
		ck.mu.Lock()
		primary := ck.view.Primary
		ck.mu.Unlock()
		callSucceeded = call(primary, "PBServer.Get", args, &reply)
		if callSucceeded == false {
			ck.UpdateCache()
		}
	}


	return reply.Value
}


func (ck *Clerk) UpdateCache() {
	primary := ck.vs.Primary()
	// fmt.Println("updating cache from primary:", primary)
	// fmt.Println("current view:", ck.view)
	reply := ViewReply{}
	call(primary, "PBServer.GetView", &GetArgs{}, &reply)
	ck.mu.Lock()
	ck.view = reply.View
	ck.mu.Unlock()
	// fmt.Println("cache updated")
	// fmt.Println("updated view:", ck.view)
}
//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// fmt.Println("starting PUT RPC: ", key, value)
	reqId := nrand()
	callSucceeded := false
	ck.mu.Lock()
	primary := ck.view.Primary
	ck.mu.Unlock()
	tries := 0
	for callSucceeded == false {
		fmt.Println("trying reqId:", reqId, "attempt:", tries)
		tries++
		args := PutAppendArgs{Key: key, Value: value, Op: op, ReqId: reqId}
		reply := PutAppendReply{}
		callSucceeded = call(primary, "PBServer.PutAppend", args, &reply)
		if callSucceeded == false {
			ck.UpdateCache()
			ck.mu.Lock()
			primary = ck.view.Primary
			ck.mu.Unlock()
		}
	}
	return
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
