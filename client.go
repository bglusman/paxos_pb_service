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
	fmt.Println("dialing", srv)
	c, errx := rpc.Dial("unix", srv)
	fmt.Println("errx",errx)
	if errx != nil {
		return false
	}
	defer c.Close()
	fmt.Println("call calling ")
	err := c.Call(rpcname, args, reply)
	fmt.Println("call returning", err)
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
	for callSucceeded != true || reply.Err == ErrWrongServer {
		ck.mu.Lock()
		primary := ck.view.Primary
		ck.mu.Unlock()
		callSucceeded = call(primary, "PBServer.Get", args, &reply)
		if callSucceeded == false {
			ck.UpdateCache()
		}



		for reply.Err == "" && callSucceeded == true {fmt.Println("sleeping get"); time.Sleep(50 * time.Millisecond )}
	}

	fmt.Println("GET returning")
	return reply.Value
}


func (ck *Clerk) UpdateCache() {
	ck.mu.Lock()
	//primary := ck.vs.Primary()
	// fmt.Println("updating cache from primary:", primary)
	// fmt.Println("current view:", ck.view)
	reply := viewservice.PingReply{}
	fmt.Println("get view rpc start, asking info from", ck.vs.Server())
	updateSuccessful := call(ck.vs.Server(), "ViewServer.GetView", &viewservice.PingReply{}, &reply)

	if !updateSuccessful{
		fmt.Println("can't contact primary")
		return
	}
	fmt.Println("get view rpc return", reply)

	ck.view = reply.View
	ck.mu.Unlock()
	fmt.Println("cache updated")
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
	tries2 := 0
	reply := PutAppendReply{}

	for tries2 < 20 && (callSucceeded == false || reply.Err == ErrWrongServer) {
		fmt.Println("call status", callSucceeded, "reply", reply)
		fmt.Println("trying reqId:", reqId, "attempt:", tries2)

		args := PutAppendArgs{Key: key, Value: value, Op: op, ReqId: reqId}
		callSucceeded = call(primary, "PBServer.PutAppend", args, &reply)
		fmt.Println("called, returned", callSucceeded, reply, tries2)
		if callSucceeded == false || reply.Err == ErrWrongServer {
			ck.UpdateCache()
			ck.mu.Lock()
			primary = ck.view.Primary
			fmt.Println("updating primary to", primary)
			ck.mu.Unlock()
		}
		tries := 0

		for reply.Err == "" && callSucceeded == true && tries < 20{fmt.Println(tries); time.Sleep(50 * time.Millisecond); tries++}
		tries2++

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
