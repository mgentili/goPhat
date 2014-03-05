package vr

/*
Lots of issues here:
Log needs to be actually added
Do not like the global DoViewChangeArgs slice right below
StartViewChange sends RPCs of itself - I am not sure if that works as is (kind of doubt it)
In DoViewChange settings the new variables correctly when we can be missing indexes in the slice, how to do that?
*/

//Need a slice of DoViewChange args (somewhere)
var DVCArgs [NREPLICAS]DoViewChangeArgs

type StartViewChangeArgs struct {
	View          int
	ReplicaNumber int
}

type DoViewChangeArgs struct {
	View          int
	ReplicaNumber int
	Log []string
	NormalView   int //last time the view was "normal"
	OpNumber     int
	CommitNumber int
}

type StartViewArgs struct {
	View int
	OpNumber     int
    Log []string
	CommitNumber int
}

//A replica notices that a viewchange is needed - starts off the messages
func PrepareViewChange() {
	rstate.Status = ViewChange
	rstate.View++

	args := StartViewChangeArgs{rstate.View, rstate.ReplicaNumber}

	sendAndRecv(NREPLICAS, "Replica.StartViewChange", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

}

//viewchange RPCs
func (t *Replica) StartViewChange(args *StartViewChangeArgs, reply *EmptyReply) error {
    //This view is already ahead of the proposed one
    if r.View > args.View {
        return nil
    }

    if r.View < args.View {
        rstate.View = args.View
        rstate.Status = ViewChange
    }

    rstate.ViewChangeMsgs++ //when this equals NREPLICAS we send DoViewChange

	//send StartViewChange messages to all replicas
	replyConstructor := func() { return new(EmptyReply) }

	sendAndRecv(NREPLICAS, "Replica.StartViewChange", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

	//We have a majority for StartViewChange msgs -- send DoViewChange to
	//new master -- rstate.View % NREPLICAS+1 is assumed the master..
	if rstate.ViewChangeMsgs == F {
		args := DoViewChangeArgs{rstate.View, rstate.ReplicaNumber, phatlog, rstate.View, rstate.OpNumber, rstate.CommitNumber}
		replyConstructor := func() { return new(EmptyReply) }


		call := clients[rstate.View%(NREPLICAS+1)].Go("Replica.DoViewChange", args, replyConstructor, nil)

	}

}

func (t *Replica) DoViewChange(args *DoViewChangeArgs, reply *EmptyReply) error {
	mstate.ViewChangeMsgs++ //recieved a DoViewChange message
	DVCArgs[args.ReplicaNumber] = args

	//We have recived enough DoViewChange messages
	if mstate.ViewChangeMsgs == F+1 {

		//Sets new log with largest reply
		//Sets OpNumber with topmost entry in new logw
		//Sets CommitNumber to max of all DVCArgs seen

		args := StartViewArgs{rstate.View /*log*/, rstate.OpNumber, rstate.CommitNumber}
		replyConstructor := func() { return new(EmptyReply) }

		//Confused by the syntax, basically we do not care about return value
		sendAndRecv(NREPLICAS, "Replica.StartView", args, replyConstructor, func(reply interface{}) bool {
			return reply.(*EmptyReply)
		})

	}

}

func (t *Replica) StartView(args *DoViewChangeArgs, reply *EmptyReply) error {
	//sets the log
	rstate.OpNumber = args.OpNumber
	rstate.CommitNumber = args.CommitNumber
}
