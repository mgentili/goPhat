package vr

const (
    F = 1
    NREPLICAS = 2*F // doesn't count the master as a replica
)

type ReplicaState {
    ViewNumber int
    OpNumber int
    CommitNumber int
    ReplicaNumber int
}

type MasterState {
    A int
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

func goVR(command interface{}) {
    assert(rstate.IsMaster() /*&& holdLease()*/);
    
    rstate.OpNumber++
    
    phatlog.add(command)

    args := PrepareArgs{ rstate.ViewNumber, command, rstate.OpNumber, rstate.CommitNumber }
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