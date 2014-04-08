package vr

import (
	"sort"
	"time"
)

// handle a given replica's heartbeat response
func (r *Replica) Heartbeat(replica uint, newTime time.Time) {
	assert(r.IsMaster())

	r.Mstate.Heartbeats[replica] = newTime

	sortedTimes := SortTimes(r.Mstate.Heartbeats)

	oldestMajority := len(sortedTimes) - F
	if oldestMajority < 0 {
		// not enough heartbeats yet to have a lease
		return
	}
	leaseExpiry := sortedTimes[oldestMajority].Add(-MAX_CLOCK_DRIFT)
	r.Mstate.ExtendNeedsRenewal(leaseExpiry)
	r.Rstate.ExtendLease(leaseExpiry)
}

func (mstate *MasterState) ExtendNeedsRenewal(newTime time.Time) {
	mstate.Timer.Reset(newTime.Sub(time.Now()) / RENEW_FACTOR)
}

func (rstate *ReplicaState) ExtendLease(newTime time.Time) {
	rstate.Timer.Reset(newTime.Sub(time.Now()))
}

func (r *Replica) ReplicaTimeout() {
	if r.IsMaster() {
		r.Debug(STATUS, "we couldn't stay master :(,ViewNum:%d\n", r.Rstate.View)
		// TODO: can't handle read requests anymore
	}
	r.Debug(STATUS, "Timed out, trying view change")
	r.PrepareViewChange()
	// start counting again so we timeout if the new replica can't become master
	r.Rstate.ExtendLease(time.Now().Add(LEASE))
}

func (r *Replica) MasterNeedsRenewal() {
	if r.IsShutdown {
		return
	}
	r.sendCommitMsgs()
}

type ByTime []time.Time

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].Before(a[j]) }

func SortTimes(times map[uint]time.Time) []time.Time {
	vals := make([]time.Time, len(times))
	i := 0
	for _, v := range times {
		vals[i] = v
		i++
	}
	sort.Sort(ByTime(vals))
	return vals
}
