package raftx

import (
	"math"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type ReplicationSetVoterPriority int

const (
	StaleOutsideVoter ReplicationSetVoterPriority = 1
	StaleWitness      ReplicationSetVoterPriority = 2
	StaleNonWitness   ReplicationSetVoterPriority = 3
	HealthyWitness    ReplicationSetVoterPriority = 4
	Nothing           ReplicationSetVoterPriority = 5
	HealthyNonWitness ReplicationSetVoterPriority = 6
)

type peer struct {
	lastSeenAt uint64
	isWitness  bool
}

type peerTracker struct {
	ticks          uint64
	peers          map[uint64]*peer
	voters         quorum.JointConfig
	staleThreshold uint64
}

func newPeerTracker(cs *raftpb.ConfState, staleThreshold uint64) *peerTracker {
	t := &peerTracker{
		peers:          map[uint64]*peer{},
		voters:         quorum.JointConfig{},
		staleThreshold: staleThreshold,
	}

	t.applyConfChange(cs)

	return t
}

func (t *peerTracker) applyConfChange(cs *raftpb.ConfState) {
	peers := map[uint64]*peer{}
	voters := quorum.JointConfig{}
	for i, c := range [2][]uint64{cs.Voters, cs.VotersOutgoing} {
		voters[i] = map[uint64]struct{}{}
		for _, v := range c {
			voters[i][v] = struct{}{}
			if _, ok := peers[v]; !ok {
				if p, ok := t.peers[v]; ok {
					peers[v] = p
				} else {
					peers[v] = &peer{lastSeenAt: 0, isWitness: false}
				}
			}
		}
	}

	t.peers = peers
	t.voters = voters
}

func (t *peerTracker) tick() {
	t.ticks++
}

func (t *peerTracker) observe(id uint64) {
	if p, ok := t.peers[id]; ok {
		p.lastSeenAt = t.ticks
	}
}

func (t *peerTracker) isStale(id uint64) bool {
	if p, ok := t.peers[id]; ok {
		return t.ticks-p.lastSeenAt > t.staleThreshold
	}

	return true
}

func (t *peerTracker) setWitness(id uint64) bool {
	if p, ok := t.peers[id]; ok {
		p.isWitness = true
		return true
	}

	return false
}

func (t *peerTracker) isWitness(id uint64) bool {
	return t.peers[id].isWitness
}

func (t *peerTracker) getReplicationSetVoterPriority(id uint64, r ReplicationSet) ReplicationSetVoterPriority {
	p, ok := t.peers[id]
	if !ok {
		return Nothing
	}

	if t.ticks-p.lastSeenAt > t.staleThreshold {
		if _, inside := r[id]; inside {
			if p.isWitness {
				return StaleWitness
			}
			return StaleNonWitness
		} else {
			return StaleOutsideVoter
		}
	} else {
		if p.isWitness {
			return HealthyWitness
		}
		return HealthyNonWitness
	}
}

func (t *peerTracker) getReplicationSet(current ReplicationSet, incoming bool) ReplicationSet {
	voters := t.voters[0]
	if !incoming {
		voters = t.voters[1]
	}

	var excluded uint64 = 0
	excludedPriority := Nothing
	set := ReplicationSet{}
	for id := range voters {
		priority := t.getReplicationSetVoterPriority(id, current)
		if priority == Nothing {
			continue
		}
		if priority >= excludedPriority {
			set[id] = struct{}{}
		} else {
			if excludedPriority != Nothing {
				set[excluded] = struct{}{}
			}
			excluded = id
			excludedPriority = priority
		}
	}
	return set
}

func (t *peerTracker) getDefaultReplicationSets() [2]ReplicationSet {
	sets := [2]ReplicationSet{}
	for i, c := range t.voters {
		for id := range c {
			sets[i][id] = struct{}{}
		}
	}

	return sets
}

type replicationSetMatchAckIndexer struct {
	rs      ReplicationSet
	pm      tracker.ProgressMap
	witness uint64
}

func newReplicationSetMatchAckIndexer(rs ReplicationSet, peers map[uint64]*peer, pm tracker.ProgressMap) *replicationSetMatchAckIndexer {
	witness := uint64(0)
	for v := range rs {
		if peer, ok := peers[v]; ok && peer.isWitness {
			witness = v
			break
		}
	}

	return &replicationSetMatchAckIndexer{
		rs:      rs,
		pm:      pm,
		witness: witness,
	}
}

func (ai replicationSetMatchAckIndexer) AckedIndex(id uint64) (quorum.Index, bool) {
	if _, ok := ai.rs[id]; !ok {
		return 0, false
	}
	pr, ok := ai.pm[id]
	if !ok {
		return 0, false
	}

	if id == ai.witness {
		return quorum.Index(math.MaxUint64), true
	}
	return quorum.Index(pr.Match), true
}

func (t *peerTracker) committedOnWitnessAck(rs [2]ReplicationSet, rn *raft.RawNode) uint64 {
	pm := tracker.ProgressMap{}
	rn.WithProgress(func(id uint64, typ raft.ProgressType, pr tracker.Progress) {
		pm[id] = &pr
	})

	idx0 := t.voters[0].CommittedIndex(newReplicationSetMatchAckIndexer(rs[0], t.peers, pm))
	idx1 := t.voters[1].CommittedIndex(newReplicationSetMatchAckIndexer(rs[1], t.peers, pm))
	if idx0 < idx1 {
		return uint64(idx0)
	}
	return uint64(idx1)
}
