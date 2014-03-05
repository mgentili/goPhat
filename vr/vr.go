package vr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
)

const (
	F         = 1
	NREPLICAS = 2 * F // doesn't count the master as a replica
)

var replicaAddress = []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}

var clients [NREPLICAS]*rpc.Client

var rstate ReplicaState
var mstate MasterState

// a replica's possible states
const (
	Normal = iota
	Recovery
	ViewChange
)

type Command string

var phatlog []string

type ReplicaState struct {
	View          uint
	OpNumber      uint
	CommitNumber  uint
	ReplicaNumber uint
	Status        int
}

type MasterState struct {
	A int
	// bit vector of what replicas have replied
	Replies uint64
}

type PrepareArgs struct {
	View         uint
	Command      interface{}
	OpNumber     uint
	CommitNumber uint
}

type PrepareReply struct {
	View          uint
	OpNumber      uint
	ReplicaNumber uint
}

type CommitArgs struct {
	View         uint
	CommitNumber uint
}

func assert(b bool) {
	if !b {
		log.Fatal("assertion failed")
	}
}

func wrongView() error {
	return errors.New("view numbers don't match")
}

func addLog(command interface{}) {
	phatlog = append(phatlog, command.(string))
	//    phatlog.add(command)
}

func doCommit(cn uint) {
	if cn <= rstate.CommitNumber {
		return
	} else if cn > rstate.CommitNumber+1 {
		log.Printf("Commits are ahead of us\n")
	}
	//    rstate.Commit()
	log.Printf("'commiting'")
	rstate.CommitNumber++
}

// RPCs
type Replica struct{}

func (t *Replica) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	if rstate.Status != Normal {
		// TODO: ideally we should just not respond or something in this case
		return errors.New("not in normal mode")
	}

	if args.View != rstate.View {
		return wrongView()
	}

	if args.OpNumber != rstate.OpNumber-1 {
		// TODO: we should probably sleep or something til this is true??
		return errors.New("op numbers out of sync")
	}

	rstate.OpNumber++
	addLog(args.Command)

	reply.View = rstate.View
	reply.OpNumber = rstate.OpNumber
	reply.ReplicaNumber = rstate.ReplicaNumber

	return nil
}

func (t *Replica) Commit(args *CommitArgs, reply *int) error {
	if args.View != rstate.View {
		return wrongView()
	}

	doCommit(args.CommitNumber)

	return nil
}

func (rstate *ReplicaState) IsMaster() bool {
	return rstate.View%(NREPLICAS+1) == rstate.ReplicaNumber
}

func (mstate *MasterState) Reset() {
	mstate.A = 0
	mstate.Replies = 0
}

func masterInit() {
	j := 0
	var i uint = 0
	for ; i < NREPLICAS+1; i++ {
		if i == rstate.ReplicaNumber {
			continue
		}
		c, err := rpc.Dial("tcp", replicaAddress[i])
		if err != nil {
			// couldn't connect to some replica, try to just keep going
			// (could probably handle this better...)
			fmt.Println(err)
		}
		clients[j] = c
		j++
	}
}

func replicaInit() net.Listener {
	rpc.Register(new(Replica))
	ln, err := net.Listen("tcp", replicaAddress[rstate.ReplicaNumber])
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return ln
}

// TODO: might not need to do this, e.g. if we handle client and server rpcs all on the same port
func replicaRun(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		rpc.ServeConn(c)
	}
}

func goVR(command interface{}) {
	assert(rstate.IsMaster() /*&& holdLease()*/)

	// FIXME: right now we enforce that the last operation has been committed before starting a new one
	assert(rstate.OpNumber == rstate.CommitNumber)

	mstate.Reset()

	rstate.OpNumber++
	addLog(command)

	args := PrepareArgs{rstate.View, command, rstate.OpNumber, rstate.CommitNumber}
	replyConstructor := func() interface{} { return new(PrepareReply) }
	sendAndRecv(NREPLICAS, "Replica.Prepare", args, replyConstructor, func(reply interface{}) bool {
		return handlePrepareOK(reply.(*PrepareReply))
	})
}

func handlePrepareOK(reply *PrepareReply) bool {
	if reply.View != rstate.View {
		return false
	}

	if reply.OpNumber != rstate.OpNumber {
		return false
	}

	if ((1 << reply.ReplicaNumber) & mstate.Replies) != 0 {
		return false
	}

	mstate.Replies |= 1 << reply.ReplicaNumber
	mstate.A++

	// we've implicitly gotten a response from ourself already
	if mstate.A != F {
		return mstate.A >= F
	}

	// we've now gotten a majority
	doCommit(rstate.CommitNumber)

	args := CommitArgs{rstate.View, rstate.CommitNumber}
	// TODO: technically only need to do this when we don't get another request from the client for a while
	go sendAndRecv(NREPLICAS, "Replica.Commit", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

	return true
}

func sendAndRecv(N int, msg string, args interface{}, newReply func() interface{}, handler func(reply interface{}) bool) {

	type ClientCall struct {
		call *rpc.Call
		// need to track which client so we can resend as needed
		client *rpc.Client
	}

	callChan := make(chan ClientCall)

	sendOne := func(client *rpc.Client) {
		call := client.Go(msg, args, newReply(), nil)
		go func(c ClientCall) {
			// wait for this call to complete
			<-c.call.Done
			// and now send it to the master channel
			callChan <- c
		}(ClientCall{call, client})
	}

	for i := 0; i < N; i++ {
		sendOne(clients[i])
	}

	for i := 0; i < N; {
		clientCall := <-callChan
		call := clientCall.call
		if call.Error != nil {
			// for now just resend failed messages indefinitely
			log.Println("sendAndRecv message error")
			sendOne(clientCall.client)
			continue
		}
		if handler(call.Reply) {
			break
		}

		i++
	}
}
