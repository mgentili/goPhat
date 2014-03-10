package vr

import (
	"log"
)

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

func (r *Replica) logVcstate(state string) {
	log.Printf("Replica %d at state %s\n", r.Rstate.ReplicaNumber, state)
	log.Printf("StartViewChangeRepliesCount: %d\n", r.Vcstate.StartViewReplies)
	log.Printf("StartViewChangeRepliesIdxs: %d\n", r.Vcstate.StartViewReplies)
	log.Printf("DoViewChangeRepliesCount: %d\n", r.Vcstate.StartViewReplies)
	log.Printf("DoViewChangeRepliesIdxs: %d\n", r.Vcstate.StartViewReplies)

	log.Printf("---\n")
}

//A replica notices that a viewchange is needed - starts off the messages
func (r *Replica) PrepareViewChange() {
	r.logVcstate("PrepareViewChange")
	r.Rstate.Status = ViewChange
	r.Rstate.View++

	args := StartViewChangeArgs{r.Rstate.View, r.Rstate.ReplicaNumber}

	go r.sendAndRecv(NREPLICAS, "RPCReplica.StartViewChange", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

}

//viewchange RPCs
func (t *RPCReplica) StartViewChange(args *StartViewChangeArgs, reply *int) error {
	r := t.R
	r.logVcstate("StartViewChange")

	//This view is already ahead of the proposed one
	if r.Rstate.View > args.View {
		return nil
	}

	//already recieved a message from this replica
	if ((1 << args.ReplicaNumber) & r.Vcstate.StartViewReplies) != 0 {
		return nil
	}

	r.Vcstate.StartViewReplies |= 1 << args.ReplicaNumber
	r.Vcstate.StartViews++

	//first time we have seen this viewchange message
	if r.Rstate.View < args.View {
		r.Vcstate.NormalView = r.Rstate.View //last known normal View
		r.Rstate.View = args.View
		r.Rstate.Status = ViewChange

		SVCargs := StartViewChangeArgs{r.Rstate.View, r.Rstate.ReplicaNumber}

		//send StartViewChange messages to all replicas
		go r.sendAndRecv(NREPLICAS, "RPCReplica.StartViewChange", SVCargs,
			func() interface{} { return nil },
			func(r interface{}) bool { return false })
	}

	if r.Vcstate.StartViews == F {
		r.logVcstate("Sending DoViewChange")

		DVCargs := DoViewChangeArgs{r.Rstate.View, r.Rstate.ReplicaNumber, r.Phatlog, r.Vcstate.NormalView, r.Rstate.OpNumber, r.Rstate.CommitNumber}

		// only send DoViewChange if we're not the new master (can't actually send a message to ourself)
		if !r.IsMaster() {
			r.Clients[r.Rstate.View%(NREPLICAS+1)].Call("RPCReplica.DoViewChange", DVCargs, nil)
		}
	}

	return nil
}

func (t *RPCReplica) DoViewChange(args *DoViewChangeArgs, reply *int) error {
	r := t.R
	r.logVcstate("DoViewChange")

	//already recieved a message from this replica
	if ((1 << args.ReplicaNumber) & r.Vcstate.DoViewReplies) != 0 {
		return nil
	}

	r.Vcstate.DoViewReplies |= 1 << args.ReplicaNumber
	r.Vcstate.DoViews++
	r.Vcstate.DoViewChangeMsgs[args.ReplicaNumber] = *args

	//We have recived enough DoViewChange messages
	if r.Vcstate.DoViews == F+1 {
		r.logVcstate("PrepareStartView")

		//TODO: implement
		r.calcMasterView()

		//send the StartView messages to all replicas
		SVargs := StartViewArgs{r.Rstate.View, r.Phatlog, r.Rstate.OpNumber, r.Rstate.CommitNumber}
		r.sendAndRecv(NREPLICAS, "RPCReplica.StartView", SVargs,
			func() interface{} { return nil },
			func(r interface{}) bool { return false })

	}
	return nil
}

func (t *RPCReplica) StartView(args *DoViewChangeArgs, reply *int) error {
	r := t.R
	r.logVcstate("StartView")

	r.Phatlog = args.Log
	r.Rstate.OpNumber = args.OpNumber
	r.Rstate.CommitNumber = args.CommitNumber

	return nil
}

func (r *Replica) calcMasterView() {
	r.Rstate.View = r.Vcstate.DoViewChangeMsgs[0].View

	/*
	   for i  := 0; i < NREPLICAS+1; i++ {
	       if i != r.Rstate.ReplicaNumber {

	       }
	   }
	*/

}
