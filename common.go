package pbservice

import "viewservice"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type BackupData struct {
	Data map[string]string
	Reqs map[int64]bool
}

type PutRequest struct {
	Args  PutAppendArgs
	Reply PutAppendReply
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op     string
	ReqId  int64
	Backup bool
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Viewnum uint
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Viewnum uint
}

type ViewReply struct {
	View viewservice.View
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
