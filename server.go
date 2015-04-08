package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view       viewservice.View
	role       string
	data 			 map[string]string
	reqs       map[int64]bool
	dataMu     sync.Mutex
}

func (pb *PBServer) GetView(args *GetArgs, reply *ViewReply) error {
	fmt.Println("getting view")
	pb.mu.Lock()
	reply.View = pb.view
	fmt.Println("sending out view", pb.view)
	pb.mu.Unlock()
	fmt.Println("view set")
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// fmt.Println("GET in server: ", args)
	pb.mu.Lock()
	role 			:= pb.role
	pb.mu.Unlock()
	if role != "primary" {
			reply.Err = ErrWrongServer
		} else {
			var ok bool
			pb.dataMu.Lock()
			reply.Value, ok = pb.data[args.Key]
			pb.dataMu.Unlock()
			if !ok {
				reply.Err = ErrNoKey
			}
		}


	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// fmt.Println("PUT in server")
	pb.mu.Lock()
	role 			:= pb.role
	processed := pb.reqs[args.ReqId]
	pb.reqs[args.ReqId] = true
	pb.mu.Unlock()
	if processed == false {
		if args.Op == "Put" && role == "primary" || role == "backup" && args.Backup {
				pb.data[args.Key] = args.Value
		} else if args.Op == "Append" && role == "primary" || role == "backup" && args.Backup {
				pb.dataMu.Lock()
				str, OK := pb.data[args.Key]
				pb.dataMu.Unlock()
				if !OK {
					str = ""
				}
				pb.dataMu.Lock()
				pb.data[args.Key] = str + args.Value
				pb.dataMu.Unlock()
		} else {
			reply.Err = ErrWrongServer
		}
		if reply.Err != ErrWrongServer  && role == "primary" {
			args.Backup = true
			backupErr := false
			for backupErr != true && pb.view.Backup != "" {
				backupErr = call(pb.view.Backup, "PBServer.PutAppend", args, &reply)
			}
		}
	}
	return nil
}

func (pb *PBServer) DataClone(data map[string]string, reply *PutAppendReply) error {
	pb.data = data
	reply.Err = OK
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// fmt.Println("Clerk: ", pb.vs)
	oldView := pb.view
	// oldRole := pb.role
	pb.view, _ = pb.vs.Ping(pb.view.Viewnum)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.vs.Me() == pb.view.Primary {
		pb.role = "primary"
	} else if pb.vs.Me() == pb.view.Backup {
		pb.role = "backup"
	} else {
		pb.role = "idle"
	}
	if pb.role == "primary" && oldView.Backup != pb.view.Backup {
		reply := PutAppendReply{}
		call(pb.view.Backup, "PBServer.DataClone", pb.data, &reply)
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.reqs = make(map[int64]bool)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
