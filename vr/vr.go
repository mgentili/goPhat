package vr

const (
    F = 1
    NREPLICAS = 2*F // doesn't count the master as a replica
)

var clients[NREPLICAS]*rpc.Client

var rstate ReplicaState
var mstate MasterState

type ReplicaState {
    View int
    OpNumber int
    CommitNumber int
    ReplicaNumber int
}

type MasterState {
    A int
    // bit vector of what replicas have replied
    Replies uint64
}

type PrepareArgs struct {
    View int
    Command interface{}
    OpNumber int
    CommitNumber int
}

type PrepareReply struct {
    View int
    OpNumber int
    ReplicaNumber int
}

type CommitArgs struct {
    View int
    CommitNumber int
}

// RPCs
type Replica struct{}

func wrongView() error {
    return os.NewError("view numbers don't match")
}

func (t *Replica) Prepare(args *PrepareArgs, reply *PrepareReply) error {
    if args.View != rstate.View {
        return wrongView()
    }

    if args.OpNumber != rstate.OpNumber-1 {
        // TODO: we should probably sleep or something til this is true??
        return os.NewError("op numbers out of sync")
    }

    rstate.OpNumber++
    phatlog.add(args.Command)

    reply.View = rstate.View
    reply.OpNumber = rstate.OpNumber
    reply.ReplicaNumber = rstate.ReplicaNumber

    return nil
}

func (t *Replica) Commit(args *CommitArgs, reply *int) error {
    if args.View != rstate.View {
        return wrongView()
    }

    rstate.Commit()
    rstate.CommitNumber++
    
    return nil
}

func (rstate *ReplicaState) IsMaster() bool {
    return rstate.View % (NREPLICAS+1) == rstate.ReplicaNumber
}

func (mstate *MasterState) Reset() {
    mstate.A = 0
    mstate.Replies = 0
}

func masterInit() {
    j := 0
    for i := 0; i < NREPLICAS+1; i++ {
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
    assert(rstate.IsMaster() /*&& holdLease()*/);

    // FIXME: right now we enforce that the last operation has been committed before starting a new one
    assert(rstate.OpNumber == rstate.CommitNumber);
    
    mstate.Reset()
    rstate.OpNumber++
    
    phatlog.add(command)

    args := PrepareArgs{ rstate.View, command, rstate.OpNumber, rstate.CommitNumber }
    replyConstructor := func() { return new(PrepareReply) }
    sendAndRecv(NREPLICAS, "Replica.Prepare", args, replyConstructor, func(reply interface{}) bool {
        return handlePrepareOK(reply.(*PrepareReply));
    });
}

func handlePrepareOK(reply *PrepareReply) bool {
    if reply.View != rstate.View {
        return false
    }
    
    if reply.OpNumber != rstate.OpNumber {
        return false
    }

    if (1 << reply.ReplicaNumber) & mstate.Replies {
        return false
    }

    mstate.Replies |= 1 << reply.ReplicaNumber
    mstate.A++

    // we've implicitly gotten a response from ourself already
    if mstate.A != F {
        return mstate.A >= F+1
    }

    // we've now gotten a majority
    rstate.Commit()
    rstate.CommitNumber++

    args := CommitArgs{ rstate.View, rstate.CommitNumber }
    // TODO: technically only need to do this when we don't get another request from the client for a while
    go sendAndEnsure(NREPLICAS, "Replica.Commit", args)

    return true
}

func sendAndEnsure(N int, msg string, args interface{}) {
    var calls []*rpc.Call
    for i := 0; i < N; i++ {
        call := clients[i].Go(msg, args, nil, nil)
        calls = append(calls, call)
    }

    for {
        <-call.Done
        if call.Error != nil {
            log.Printf("sendAndEnsure message error")
        }
    }
}

func sendAndRecv(N int, msg string, args interface{}, newReply func()(interface{}), handler func(reply interface{})(bool)) {
    var calls []*rpc.Call
    for i := 0; i < N; i++ {
        call := clients[i].Go(msg, args, newReply(), nil)
        calls = append(calls, call)
    }

    for _, call := range calls {
        <-call.Done
        if call.Error != nil {
            // TODO: it's possible for a majority of messages to fail and then we need to retry them
            log.Fatal("sendAndRecv message error")
        }
        if handler(call.Reply) {
            break
    }


}