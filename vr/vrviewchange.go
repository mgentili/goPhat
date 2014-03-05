package vr

//dummy init state
var newMasterArgs = NewMasterArgs{0, 0, phatlog, 0, 0}

type NewMasterArgs struct {
	View          uint
	MaxNormalView uint
	Log           []string
	OpNumber      uint
	CommitNumber  uint
}

type StartViewChangeArgs struct {
	View          uint
	ReplicaNumber uint
}

type DoViewChangeArgs struct {
	View          uint
	ReplicaNumber uint
	Log           []string
	NormalView    uint
	OpNumber      uint
	CommitNumber  uint
}

type StartViewArgs struct {
	View         uint
	Log          []string
	OpNumber     uint
	CommitNumber uint
}

//A replica notices that a viewchange is needed - starts off the messages
func PrepareViewChange() {
	log.Printf("Replica %d preparing a View Change\n", rstate.ReplicaNumber)

	rstate.Status = ViewChange
	rstate.View++

	args := StartViewChangeArgs{rstate.View, rstate.ReplicaNumber}

	go sendAndRecv(NREPLICAS, "Replica.StartViewChange", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

}

//viewchange RPCs
func (t *Replica) StartViewChange(args *StartViewChangeArgs, reply *int) error {
	log.Printf("Replica %d got StartViewChange\n", rstate.ReplicaNumber)

	//This view is already ahead of the proposed one or we already are in viewchange
	if rstate.View > args.View || rstate.Status == ViewChange {
		return nil
	}

	if rstate.View < args.View {
		rstate.NormalView = rstate.View //last known normal View
		rstate.View = args.View
		rstate.Status = ViewChange
	}

	SVCargs := StartViewChangeArgs{rstate.View, rstate.ReplicaNumber}

	//send StartViewChange messages to all replicas
	go sendAndRecv(NREPLICAS, "Replica.StartViewChange", SVCargs,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

	log.Printf("Replica %d sent DoViewChange to %d\n", rstate.ReplicaNumber, rstate.View%(NREPLICAS+1))

	//new master -- rstate.View % NREPLICAS+1 is assumed the master..
	DVCargs := DoViewChangeArgs{rstate.View, rstate.ReplicaNumber, phatlog, rstate.NormalView, rstate.OpNumber, rstate.CommitNumber}

	//TODO:Verify this line is right!!
	clients[rstate.View%(NREPLICAS+1)].Go("Replica.DoViewChange", DVCargs, nil, nil)

	return nil
}

func (t *Replica) DoViewChange(args *DoViewChangeArgs, reply *int) error {
	log.Printf("Replica %d got DoViewChange from %d\n", rstate.ReplicaNumber, args.ReplicaNumber)

	mstate.ViewChangeMsgs++ //recieved a DoViewChange message

	newMasterArgs.View = args.View
	if args.NormalView > newMasterArgs.MaxNormalView || (args.NormalView == newMasterArgs.MaxNormalView && args.OpNumber > newMasterArgs.OpNumber) {
		newMasterArgs.MaxNormalView = args.NormalView
		newMasterArgs.Log = args.Log
		newMasterArgs.OpNumber = args.OpNumber
	}

	if args.CommitNumber > newMasterArgs.CommitNumber {
		newMasterArgs.CommitNumber = args.CommitNumber
	}
	//We have recived enough DoViewChange messages
	if mstate.ViewChangeMsgs == F+1 {
		rstate.View = newMasterArgs.View
		phatlog = newMasterArgs.Log
		rstate.OpNumber = newMasterArgs.OpNumber
		rstate.CommitNumber = newMasterArgs.CommitNumber

		//send the StartView messages to all replicas
		SVargs := StartViewArgs{rstate.View, phatlog, rstate.OpNumber, rstate.CommitNumber}
		go sendAndRecv(NREPLICAS, "Replica.StartView", SVargs,
			func() interface{} { return nil },
			func(r interface{}) bool { return false })

		mstate.ViewChangeMsgs = 0
	}
	return nil
}

func (t *Replica) StartView(args *DoViewChangeArgs, reply *int) error {
	phatlog = args.Log
	rstate.OpNumber = args.OpNumber
	rstate.CommitNumber = args.CommitNumber

	return nil
}
