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

//Not sure if this is necesary - just to mirror syntax
type EmptyReply struct {
}

type StartViewChangeArgs struct {
	View          int
	ReplicaNumber int
}

type DoViewChangeArgs struct {
	View          int
	ReplicaNumber int
	//l log
	NormalView   int //last time the view was "normal"
	OpNumber     int
	CommitNumber int
}

type StartViewArgs struct {
	View int
	//l log
	OpNumber     int
	CommitNumber int
}

//A replica notices that a viewchange is needed - starts off the messages
func PrepareViewChange() {
	rstate.Status = ViewChange
	rstate.View++

	//send StartViewChanges to all replicas
	//Confused by the syntax, basically we do not care about return value
	sendAndRecv(NREPLICAS, "Replica.StartViewChange", args, replyConstructor, func(reply interface{}) bool {
		return reply.(*EmptyReply)
	})

}

//viewchange RPCs
func (t *Replica) StartViewChange(args *StartViewChangeArgs, reply *EmptyReply) error {
	if args.View <= rstate.View {
		return os.NewError("Replica already at same or newer view")
	}

	rstate.View = args.View
	rstate.Status = ViewChange
	rstate.ViewChangeMsgs++ //when this equals NREPLICAS we send DoViewChange

	//send StartViewChange messages to everyone
	args := StartViewChangeArgs{rstate.View, rstate.ReplicaNumber}
	replyConstructor := func() { return new(EmptyReply) }

	//TODO:INFINITE RECURSION - do not sent to self??
	//Confused by the syntax, basically we do not care about return value
	sendAndRecv(NREPLICAS, "Replica.StartViewChange", args, replyConstructor, func(reply interface{}) bool {
		return reply.(*EmptyReply)
	})

	//We have a majority for StartViewChange msgs -- send DoViewChange to
	//new master -- rstate.View % NREPLICAS+1 is assumed the master..
	if rstate.ViewChangeMsgs == F {
		args := DoViewChangeArgs{rstate.View, rstate.ReplicaNumber /*log l*/, rstate.View, rstate.OpNumber, rstate.CommitNumber}
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
