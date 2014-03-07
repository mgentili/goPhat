package vr

import (
	"log"
)

var vcstate ViewChangeState

type StartViewChangeArgs struct {
	View          uint
	ReplicaNumber uint
}

type StartViewArgs struct {
	View         uint
	Log          []string
	OpNumber     uint
	CommitNumber uint
}

func logVCState(state string) {
	log.Printf("Replica %d at state %s\n", rstate.ReplicaNumber, state)
	log.Printf("StartViewChangeReplies: %d\n", vcstate.StartViewReplies)
	log.Printf("---\n")
}

//A replica notices that a viewchange is needed - starts off the messages
func PrepareViewChange() {
	logVCState("PrepareViewChange")
	rstate.Status = ViewChange
	rstate.View++

	args := StartViewChangeArgs{rstate.View, rstate.ReplicaNumber}

	go sendAndRecv(NREPLICAS, "Replica.StartViewChange", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

}

//viewchange RPCs
func (t *Replica) StartViewChange(args *StartViewChangeArgs, reply *int) error {
	logVCState("StartViewChange")

	//This view is already ahead of the proposed one
	if rstate.View > args.View {
		return nil
	}

	//already recieved a message from this replica
	if ((1 << args.ReplicaNumber) & vcstate.StartViewReplies) != 0 {
		return nil
	}

	vcstate.StartViewReplies |= 1 << args.ReplicaNumber
	vcstate.StartViews++

	//first time we have seen this viewchange message
	if rstate.View < args.View {
		vcstate.NormalView = rstate.View //last known normal View
		rstate.View = args.View
		rstate.Status = ViewChange

		SVCargs := StartViewChangeArgs{rstate.View, rstate.ReplicaNumber}

		//send StartViewChange messages to all replicas
		go sendAndRecv(NREPLICAS, "Replica.StartViewChange", SVCargs,
			func() interface{} { return nil },
			func(r interface{}) bool { return false })
	}

	if vcstate.StartViews == F {
		logVCState("Sending DoViewChange")

		DVCargs := DoViewChangeArgs{rstate.View, rstate.ReplicaNumber, phatlog, vcstate.NormalView, rstate.OpNumber, rstate.CommitNumber}

		// only send DoViewChange if we're not the new master (can't actually send a message to ourself)
		if !rstate.IsMaster() {
			clients[rstate.View%(NREPLICAS+1)].Call("Replica.DoViewChange", DVCargs, nil)
		}
	}

	return nil
}

func (t *Replica) DoViewChange(args *DoViewChangeArgs, reply *int) error {
	logVCState("DoViewChange")

	//already recieved a message from this replica
	if ((1 << args.ReplicaNumber) & vcstate.DoViewReplies) != 0 {
		return nil
	}

	vcstate.DoViewReplies |= 1 << args.ReplicaNumber
	vcstate.DoViews++
	vcstate.DoViewChangeMsgs[args.ReplicaNumber] = *args

	//We have recived enough DoViewChange messages
	if vcstate.DoViews == F+1 {
		//TODO: implement
		calcMasterView()

		//send the StartView messages to all replicas
		SVargs := StartViewArgs{rstate.View, phatlog, rstate.OpNumber, rstate.CommitNumber}
		sendAndRecv(NREPLICAS, "Replica.StartView", SVargs,
			func() interface{} { return nil },
			func(r interface{}) bool { return false })

	}
	return nil
}

func (t *Replica) StartView(args *DoViewChangeArgs, reply *int) error {
	logVCState("StartView")

	phatlog = args.Log
	rstate.OpNumber = args.OpNumber
	rstate.CommitNumber = args.CommitNumber

	return nil
}

func calcMasterView() {
	rstate.View = vcstate.DoViewChangeMsgs[0].View

	/*
	   for i  := 0; i < NREPLICAS+1; i++ {
	       if i != rstate.ReplicaNumber {

	       }
	   }
	*/

}
