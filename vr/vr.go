package vr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

const (
	F           = 1
	NREPLICAS   = 2 * F // doesn't count the master as a replica
	LEASE       = 5000 * time.Millisecond
	MAX_RENEWAL = LEASE / 2
)

// a replica's possible states
const (
	Normal = iota
	Recovery
	ViewChange
)

type Command string

type Replica struct{
    Rstate ReplicaState
    Mstate MasterState
    Vcstate ViewChangeState
	// list of replica addresses, in sorted order
	Config []string
    Clients [NREPLICAS + 1]*rpc.Client
    //TEMP
    Phatlog []string
}


type ReplicaState struct {
	View           uint
	OpNumber       uint
	CommitNumber   uint
	ReplicaNumber  uint
	Status         int
	NormalView     uint
	ViewChangeMsgs uint
	Timer          *time.Timer
}

type MasterState struct {
	A int
	// bit vector of what replicas have replied
	Replies uint64

	Timer            *time.Timer
	Heartbeats       int
	HeartbeatReplies uint64
}

type ViewChangeState struct {
	DoViewChangeMsgs [NREPLICAS + 1]DoViewChangeArgs
	DoViewReplies    uint64
	StartViewReplies uint64
	StartViews       uint
	DoViews          uint
	NormalView       uint
}

type DoViewChangeArgs struct {
	View          uint
	ReplicaNumber uint
	Log           []string
	NormalView    uint
	OpNumber      uint
	CommitNumber  uint
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

// Go doesn't have assertions...
func assert(b bool) {
	if !b {
		log.Fatal("assertion failed")
	}
}

func wrongView() error {
	return errors.New("view numbers don't match")
}

func (r *Replica) addLog(command interface{}) {
	r.Phatlog = append(r.Phatlog, command.(string))
    r.Debug("adding command to log")
	//    phatlog.add(command)
}

func (r *Replica) Debug(format string, args ...interface{}) {
	str := fmt.Sprintf("Replica %d: %s", r.Rstate.ReplicaNumber, format)
	log.Printf(str, args...)
}

func (r *Replica) doCommit(cn uint) {
	if cn <= r.Rstate.CommitNumber {
		r.Debug("Ignoring commit %d, already commited up to %d", cn, r.Rstate.CommitNumber)
		return
	} else if cn > r.Rstate.OpNumber {
		r.Debug("need to do state transfer. only at op %d in log but got commit for %d\n", r.Rstate.OpNumber, cn)
		return
	} else if cn > r.Rstate.CommitNumber+1 {
		r.Debug("need to do extra commits")
		// we're behind (but have all the log entries, so don't need to state
		// transfer), so catch up by committing up to the current commit
		for i := r.Rstate.CommitNumber + 1; i < cn; i++ {
			r.doCommit(i)
		}
	}
	//db.Commit()
	r.Rstate.CommitNumber++
}

// RPCs
func (r *Replica) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	r.Debug("Got prepare %d\n", args.OpNumber)

	if args.View != r.Rstate.View {
		return wrongView()
	}

	r.Rstate.ExtendLease() // do we extend lease in non-normal mode??

	if r.Rstate.Status != Normal {
		// TODO: ideally we should just not respond or something in this case
		return errors.New("not in normal mode")
	}

	if args.OpNumber != r.Rstate.OpNumber+1 {
		// TODO: we should probably sleep or something til this is true??
		return fmt.Errorf("op numbers out of sync: got %d expected %d", args.OpNumber, r.Rstate.OpNumber+1)
	}

	r.Rstate.OpNumber++
	r.addLog(args.Command)

	reply.View = r.Rstate.View
	reply.OpNumber = r.Rstate.OpNumber
	reply.ReplicaNumber = r.Rstate.ReplicaNumber

	return nil
}

func (r *Replica) Commit(args *CommitArgs, reply *uint) error {
	if args.View != r.Rstate.View {
		return wrongView()
	}

	r.Rstate.ExtendLease()

	*reply = r.Rstate.ReplicaNumber

	r.doCommit(args.CommitNumber)

	return nil
}

func (r *Replica) IsMaster() bool {
	return r.Rstate.View%(NREPLICAS+1) == r.Rstate.ReplicaNumber
}

func (mstate *MasterState) Reset() {
	mstate.A = 0
	mstate.Replies = 0
    mstate.Heartbeats = 0
    mstate.HeartbeatReplies = 0
}

func (mstate *MasterState) ExtendNeedsRenewal() {
	mstate.Timer.Reset(MAX_RENEWAL)
}

func (r *Replica) Heartbeat(replica uint) {
    assert(r.IsMaster())

	if ((1 << replica) & r.Mstate.HeartbeatReplies) != 0 {
		return
	}

	r.Mstate.HeartbeatReplies |= 1 << replica
	r.Mstate.Heartbeats++

	// we've gotten a majority to renew our lease!
	if r.Mstate.Heartbeats == F {
		r.Mstate.Heartbeats = 0
		r.Mstate.HeartbeatReplies = 0
		// don't need to try renewing for a while
		r.Mstate.ExtendNeedsRenewal()
		// update our own lease
		r.Rstate.ExtendLease()
	}
}

func (rstate *ReplicaState) ExtendLease() {
	rstate.Timer.Reset(LEASE)
}

func (r *Replica) ReplicaTimeout() {
	if r.IsMaster() {
		r.Debug("we couldn't stay master :(\n")
		// can't handle read requests anymore
	}
	r.Debug("Timed out, trying view change")
	r.PrepareViewChange()
	// start counting again so we timeout if the new replica can't become master
	r.Rstate.ExtendLease()
}

func (r *Replica) MasterNeedsRenewal() {
	// TODO: we've been inactive for MAX_RENEWAL time, so we need to
	// send explicit commits to renew lease
}

func RunAsReplica(i uint, config []string) {
    r := new(Replica)
	r.Rstate.ReplicaNumber = i
	r.Config = config

	ln := r.ReplicaInit()

	go r.ReplicaRun(ln)
	if r.IsMaster() {
		r.MasterInit()
		for {
			r.RunVR("foo")
			r.RunVR("bar")
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (r *Replica) MasterInit() {
    assert(r.IsMaster())

    r.Mstate.Reset()
	r.Mstate.Timer = time.AfterFunc(MAX_RENEWAL, r.MasterNeedsRenewal)
}

func (r *Replica) ReplicaInit() net.Listener {
	ln, err := net.Listen("tcp", r.Config[r.Rstate.ReplicaNumber])
	if err != nil {
		fmt.Println(err)
		return nil
	}
	r.Rstate.Timer = time.AfterFunc(LEASE, r.ReplicaTimeout)
	return ln
}

// TODO: might not need to do this, e.g. if we handle client and server rpcs all on the same port
func (r *Replica) ReplicaRun(ln net.Listener) {
    newServer := rpc.NewServer()

    newServer.Register(r)

	for {
/*		c, err := ln.Accept()
		if err != nil {
			continue
		}

		rpc.ServeConn(c)
*/
        newServer.Accept(ln)
        r.Debug("finished an accept?")
	}
}

func (r *Replica) RunVR(command interface{}) {
	assert(r.IsMaster() /*&& holdLease()*/)

	// FIXME: right now we enforce that the last operation has been committed before starting a new one
	assert(r.Rstate.OpNumber == r.Rstate.CommitNumber)

	r.Mstate.Reset()

	r.Rstate.OpNumber++
	r.addLog(command)

	args := PrepareArgs{r.Rstate.View, command, r.Rstate.OpNumber, r.Rstate.CommitNumber}
	replyConstructor := func() interface{} { return new(PrepareReply) }
	r.sendAndRecv(NREPLICAS, "Replica.Prepare", args, replyConstructor, func(reply interface{}) bool {
		return r.handlePrepareOK(reply.(*PrepareReply))
	})
}

func (r *Replica) handlePrepareOK(reply *PrepareReply) bool {
	r.Debug("got response: %+v\n", reply)
	if reply.View != r.Rstate.View {
		return false
	}

	r.Heartbeat(reply.ReplicaNumber)

	if reply.OpNumber != r.Rstate.OpNumber {
		return false
	}

	if ((1 << reply.ReplicaNumber) & r.Mstate.Replies) != 0 {
		return false
	}

	r.Debug("got suitable response\n")

	r.Mstate.Replies |= 1 << reply.ReplicaNumber
	r.Mstate.A++

	r.Debug("new master state: %v\n", r.Mstate)

	// we've implicitly gotten a response from ourself already
	if r.Mstate.A != F {
		return r.Mstate.A >= F
	}

	// we've now gotten a majority
	r.doCommit(r.Rstate.CommitNumber + 1)

	args := CommitArgs{r.Rstate.View, r.Rstate.CommitNumber}
	// TODO: technically only need to do this when we don't get another request from the client for a while
	go r.sendAndRecv(NREPLICAS, "Replica.Commit", args,
		func() interface{} { return new(uint) },
		func(reply interface{}) bool { r.Heartbeat(*(reply.(*uint))); return false })

	return true
}

func (r *Replica) SendSync(repNum uint, msg string, args interface{}, reply interface{}) error {
	if r.Clients[repNum] == nil {
		err := r.ClientConnect(repNum)
		if err != nil {
			return err
		}
	}
	err := r.Clients[repNum].Call(msg, args, reply)
	return err
}

func (r *Replica) ClientConnect(repNum uint) error {
	assert(repNum != r.Rstate.ReplicaNumber)
	c, err := rpc.Dial("tcp", r.Config[repNum])

	if err != nil {
		r.Debug("error trying to connect to replica %d: %v", repNum, err)
	} else {
		r.Clients[repNum] = c
	}

	return err
}

/* Sends RPC to N other replicas
 * msg is the RPC call name
 * args is the argument struct
 * newReply is a constructor that returns a new object of the expected reply 
   type. This is a bit of a wart of Go, because you can't really pass types
   to a function, but we still need a way to keep making new reply objects
 * handler is a function that will be called and passed the resulting reply 
   for each reply that is received. It will be called until it returns true,
   which signals that enough replies have been received that sendAndRecv
   will return (e.g. a majority has been received).
 * Note, however, that the RPCs will generally be re-sent until N responses
 * are received, even when handler returns true. This is so all replicas
 * do eventually get the message, even once a majority has been reached
 * and other operations can continue
 */
func (r *Replica) sendAndRecv(N int, msg string, args interface{}, newReply func() interface{}, handler func(reply interface{}) bool) {

	type ClientCall struct {
		call *rpc.Call
		// need to track which client so we can resend as needed
		repNum uint
	}

	callChan := make(chan ClientCall)

	sendOne := func(repNum uint) {
		if r.Clients[repNum] == nil {
			err := r.ClientConnect(repNum)
			if err != nil {
				return
			}
		}
		client := r.Clients[repNum]
		call := client.Go(msg, args, newReply(), nil)
		go func(c ClientCall) {
			// wait for this call to complete
			<-c.call.Done
			// and now send it to the master channel
			callChan <- c
		}(ClientCall{call, repNum})
	}

	// send requests to the replicas
	i := 0 // == requests sent
	for repNum := uint(0); i < N && repNum < NREPLICAS+1; repNum++ {
		if repNum == r.Rstate.ReplicaNumber {
			continue
		}
		sendOne(repNum)
		i++
	}

	doneChan := make(chan int)

	go func() {
		callHandler := true
		// and now get the responses and retry if necessary
		for i := 0; i < N; {
			clientCall := <-callChan
			call := clientCall.call
			if call.Error != nil {
				// for now just resend failed messages indefinitely
				r.Debug("sendAndRecv message error: %v", call.Error)
				if call.Error == rpc.ErrShutdown {
					err := r.ClientConnect(clientCall.repNum)
					if err != nil {
						// for now, at least, we won't retry a second time if the connection is completely shutdown
                        i++
						continue
					}
				}
				sendOne(clientCall.repNum)
				continue
			}
			if callHandler && handler(call.Reply) {
				// signals doneChan so that sendAndRecv can exit
				// (and the master can continue to the next request)
				// we still continue and resend messages as neccesary, however
				doneChan <- 0
				callHandler = false
			}

			i++
		}
	}()

	<-doneChan
}
