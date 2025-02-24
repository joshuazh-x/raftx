package raftx

import (
	"encoding/binary"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	EntryTypeSubterm = 0xFF
)

type subtermInfo struct {
	subterm         uint64
	replicationSets [2]ReplicationSet
}

type raftx struct {
	id      uint64
	term    uint64
	subterm *subtermInfo
	cs      *raftpb.ConfState
	tracker *peerTracker
	cfg     Config
	msgs    []raftpb.Message
	logger  raft.Logger
}

func (r *raftx) maybeStartNewSubterm(rn *raft.RawNode) bool {
	if rn.BasicStatus().SoftState.RaftState != raft.StateLeader {
		return false
	}

	rs := [2]ReplicationSet{
		r.tracker.getReplicationSet(r.subterm.replicationSets[0], true),
		r.tracker.getReplicationSet(r.subterm.replicationSets[1], false),
	}

	if rs[0].Equals(r.subterm.replicationSets[0]) && rs[1].Equals(r.subterm.replicationSets[1]) {
		return false
	}

	r.logger.Infof("%x changes replication set to %s, %s", r.id, r.subterm.replicationSets[0].String(), r.subterm.replicationSets[1].String())

	d := make([]byte, 8)
	binary.BigEndian.PutUint64(d, r.subterm.subterm)
	subtermEntry := raftpb.Entry{
		Type: EntryTypeSubterm,
		Data: d,
	}

	if err := rn.Step(raftpb.Message{
		Type:    raftpb.MsgProp,
		Entries: []raftpb.Entry{subtermEntry},
		From:    r.id,
	}); err != nil {
		return false
	}

	subterm := &subtermInfo{
		replicationSets: rs,
		subterm:         r.subterm.subterm + 1,
	}
	r.subterm = subterm

	r.logger.Infof("%x starts new subterm. Term: %d, Subterm: %d", r.id, r.term, r.subterm.subterm)
	return true
}

func (r *raftx) applyConfChange(rn *raft.RawNode, cs *raftpb.ConfState) {
	r.tracker.applyConfChange(cs)

	r.maybeStartNewSubterm(rn)
}

func (r *raftx) becameLeader(rn *raft.RawNode, term uint64) {
	r.term = term
	r.tracker = newPeerTracker(r.cs, r.cfg.StaleNodeThreshold)
	r.subterm = &subtermInfo{
		subterm:         0,
		replicationSets: r.tracker.getDefaultReplicationSets(),
	}
}

func (r *raftx) tick() {
	r.tracker.tick()
}

func (r *raftx) sendAppendToWitness(index uint64) {
	for _, rs := range r.subterm.replicationSets {
		for v := range rs {
			if r.tracker.isWitness(v) {
				m := raftpb.Message{
					Type: raftpb.MsgApp,
					To:   v,
					From: r.id,
					Term: r.term,
				}
				r.msgs = append(r.msgs, m)
			}

		}
	}
}

func (r *raftx) maybeSendAppendToWitness(rn *raft.RawNode) {
	ci := r.tracker.committedOnWitnessAck(r.subterm.replicationSets, rn)
	rn.Step()
}
