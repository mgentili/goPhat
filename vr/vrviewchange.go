package vr

/*
Do not like the global DoViewChangeArgs slice right below
*/

//Need a slice of DoViewChange args (somewhere)
var DVCArgs []DoViewChangeArgs

type StartViewChangeArgs struct {
	View          uint
	ReplicaNumber uint
}

type DoViewChangeArgs struct {
	View          uint
	ReplicaNumber uint
	Log           []string
	NormalView    uint //last time the view was "normal"
	OpNumber      uint
	CommitNumber  uint
}

type StartViewArgs struct {
	View         uint
	OpNumber     uint
	Log          []string
	CommitNumber uint
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
func (t *Replica) StartViewChange(args *StartViewChangeArgs, reply *int) error {
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

		//TODO:Verify this line is right!!
		call := clients[rstate.View%(NREPLICAS+1)].Go("Replica.DoViewChange", args, interface{})

	}

	return nil
}

func (t *Replica) DoViewChange(args *DoViewChangeArgs, reply *int) error {
	mstate.ViewChangeMsgs++ //recieved a DoViewChange message
	DVCArgs = DVCArgs.append(DVCArgs, args)

	//We have recived enough DoViewChange messages
	if mstate.ViewChangeMsgs == F+1 {
		var maxNormalView uint = 0
		var maxId uint = 0
		var tmpOpNumber uint = 0
		var maxCommit uint = 0

		//finds optimal log
		for i = 0; i < len(DVCArgs); i++ {
			if DVCArgs[i].NormalView > maxNormalView {
				maxNormalView = DVCArgs[i].NormalView
				maxId = i
				tmpOpNumber = DVCArgs[i].OpNumber
			} else if DVCArgs[i].NormalView == maxNormalView {
				if DVCArgs[i].OpNumber > tmpOpNumber {
					maxId = i
					tmpOpNumber = DVCArgs[i].OpNumber
				}
			}
		}

		//finds largest commit number
		for i = 0; i < len(DVCArgs); i++ {
			if DVCArgs[i].CommitNumber > maxCommit {
				maxCommit = DVCArgs[i].CommitNumber
			}
		}

        rstate.View = DVCArgs[0].View
		phatlog = DVCArgs[maxId].Log
		rstate.OpNumber = tmpOpNumber //I believe this is right
		rstate.CommitNumber = maxCommit

		args := StartViewArgs{rstate.View, phatlog, rstate.OpNumber, rstate.CommitNumber}
		sendAndRecv(NREPLICAS, "Replica.StartView", args,
			func() interface{} { return nil },
			func(r interface{}) bool { return false })
	}
	return nil
}

func (t *Replica) StartView(args *DoViewChangeArgs, reply *int) error {
	phatlog = args.Log
	rstate.OpNumber = args.OpNumber
	rstate.CommitNumber = args.CommitNumber

	return nil
}
