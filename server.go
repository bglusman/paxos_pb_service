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
	view     viewservice.View
	role     string
	data     map[string]string
	reqs     map[int64]bool
	dataMu   sync.Mutex
	putQueue chan PutRequest
}

func (pb *PBServer) GetView(args *GetArgs, reply *ViewReply) error {
	// fmt.Println("getting view")
	pb.mu.Lock()
	reply.View = pb.view
	// fmt.Println("sending out view", pb.view)
	pb.mu.Unlock()
	// fmt.Println("view set")
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// fmt.Println("GET in server: ", args)
	// fmt.Println("get of data:", args.Key, "by:", pb.me)
	pb.mu.Lock()
	role := pb.role
	pb.mu.Unlock()
	if role != "primary" {
		// fmt.Println(role, pb.me, "returning wrong server to get request")
		reply.Err = ErrWrongServer
	} else {
		var ok bool
		pb.mu.Lock()
		reply.Value, ok = pb.data[args.Key]
		pb.mu.Unlock()
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
		}
	}

	return nil
}
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// fmt.Println("server-side PutAppend entered for", pb.me)
	// 	fmt.Println("queuing req:", args.ReqId);
	// 	pb.putQueue <- PutRequest{Args: *args, Reply: *reply}
	// 	return nil
	// }

	// func (pb *PBServer) ProcessPutQueue() {
	// 	// fmt.Println("PUT in server")
	// 	fmt.Println("starting to process queue")
	// 	for request := range pb.putQueue {
	// fmt.Println("request in queue:", request)
	// args, reply := request.Args, request.Reply
	pb.mu.Lock()
	role := pb.role
	processed := pb.reqs[args.ReqId]

	if processed == false {

		if args.Backup {
			// fmt.Println("back up of data:", args, "by:", pb.me)
		}
		// pb.dataMu.Lock()
		if args.Op == "Put" && (role == "primary" || (role == "backup" && args.Backup)) {
			// pb.dataMu.Lock()
			pb.data[args.Key] = args.Value
			// pb.dataMu.Unlock()
		} else if args.Op == "Append" && (role == "primary" || (role == "backup" && args.Backup)) {
			// pb.dataMu.Lock()
			str, OK := pb.data[args.Key]
			// pb.dataMu.Unlock()
			if !OK {
				str = ""
			}
			// pb.dataMu.Lock()
			pb.data[args.Key] = str + args.Value

			// fmt.Println("appended:", pb.data[args.Key], "on key:", args.Key)
			// pb.dataMu.Unlock()
		} else {
			// fmt.Println(role, pb.me, "returning wrong server to put request id", args.ReqId)
			// fmt.Println("server thinks it is in", pb.view.Viewnum, "and request is from", args.Viewnum)
			reply.Err = ErrWrongServer
		}
		// pb.dataMu.Lock()
		if reply.Err != ErrWrongServer && role == "primary" {
			// pb.mu.Lock()
			args.Backup = true
			backupCallSucceeded := false

			backup := pb.view.Backup
			// pb.mu.Unlock()
			backupTries := 0
			backupReply := PutAppendReply{}

			for backupCallSucceeded != true && backup != "" && backupTries < 20 {
				// fmt.Println("backing up data:", args, "by:", pb.me)
				backupCallSucceeded = call(backup, "PBServer.PutAppend", args, &backupReply)
				if !backupCallSucceeded {
					// pb.mu.Lock()
					backup = pb.view.Backup
					// pb.mu.Unlock()
				}
				backupTries++
			}
			// wait for backup reply before moving on
			// if backup replies wrong server, then someone might have the wrong view
			// return error to client to force call to UpdateCache()
			waitForReply := 0
			for backupReply.Err == "" && backupCallSucceeded == true && waitForReply < 20 {
				time.Sleep(50 * time.Millisecond)
				waitForReply++
			}
			if backupReply.Err == ErrWrongServer {
				// force the backup to talk to the viewservice
				// fmt.Println("SENDING BACKUP TO BAD SERVER")
				dummy := GetReply{}
				// not really sure how to do an RPC without these
				call(backup, "PBServer.Tick", &dummy, &dummy)
				reply.Err = ErrWrongServer
			}
		}
		// pb.mu.Lock()
		pb.reqs[args.ReqId] = true
		if reply.Err == "" {
			reply.Err = OK
		}
		// pb.mu.Unlock()
		// pb.dataMu.Unlock()
	}
	pb.mu.Unlock()

	return nil
}

func (pb *PBServer) DataClone(data *BackupData, reply *PutAppendReply) error {
	pb.data = data.Data
	pb.reqs = data.Reqs
	reply.Err = OK
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) Tick(data *GetReply, reply *GetReply) error {
	pb.tick()
	return nil
}

func (pb *PBServer) tick() {
	// fmt.Println("Clerk: ", pb.vs)
	pb.mu.Lock()
	oldView := pb.view
	// oldRole := pb.role
	pb.view, _ = pb.vs.Ping(pb.view.Viewnum)

	if pb.vs.Me() == pb.view.Primary {
		pb.role = "primary"
	} else if pb.vs.Me() == pb.view.Backup {
		pb.role = "backup"
	} else {
		pb.role = "idle"
	}
	if pb.role == "primary" && oldView.Backup != pb.view.Backup {
		// var success bool
		reply := PutAppendReply{}
		// for success != true {
		call(pb.view.Backup, "PBServer.DataClone", BackupData{Data: pb.data, Reqs: pb.reqs}, &reply)
		// }
	}
	pb.mu.Unlock()
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
