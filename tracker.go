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
	StaleWitness      ReplicationSetVoterPriority = 1
	StaleNonWitness   ReplicationSetVoterPriority = 2
	HealthyWitness    ReplicationSetVoterPriority = 3
	Nothing           ReplicationSetVoterPriority = 4
	HealthyNonWitness ReplicationSetVoterPriority = 5
)

type peer struct {
	lastSeenAt uint64
	isWitness  bool
}

type peerTracker struct {
	ticks          uint64
	peers          map[uint64]*peer
	voters         quorum.JointConfig
	replicationSet JointReplicationSet
	staleThreshold uint64
}

func newPeerTracker(cs *raftpb.ConfState, staleThreshold uint64) *peerTracker {
	t := &peerTracker{
		ticks:          0,
		peers:          map[uint64]*peer{},
		voters:         quorum.JointConfig{},
		staleThreshold: staleThreshold,
	}

	t.applyConfChange(cs)
	t.adjustReplicationSet()

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

func (t *peerTracker) setWitness(id uint64) bool {
	if p, ok := t.peers[id]; ok {
		p.isWitness = true
		return true
	}

	return false
}

func (t *peerTracker) isWitness(id uint64) bool {
	p, ok := t.peers[id]
	return ok && p.isWitness
}

func (t *peerTracker) isVoter(id uint64, incoming bool) bool {
	i := 0
	if !incoming {
		i = 1
	}
	_, ok := t.voters[i][id]
	return ok
}

func (t *peerTracker) isReplicationSetCandidate(id uint64) bool {
	for i, voters := range t.voters {
		if _, ok := voters[id]; ok && !t.replicationSet[i].Contains(id) {
			return true
		}
	}

	return false
}

func (t *peerTracker) getReplicationSetVoterPriority(id uint64) ReplicationSetVoterPriority {
	p, ok := t.peers[id]
	if !ok {
		return Nothing
	}

	if t.ticks-p.lastSeenAt > t.staleThreshold {
		if p.isWitness {
			return StaleWitness
		}
		return StaleNonWitness
	} else {
		if p.isWitness {
			return HealthyWitness
		}
		return HealthyNonWitness
	}
}

func (t *peerTracker) adjustReplicationSet() bool {
	newReplicationSet := JointReplicationSet{}
	for i, voters := range t.voters {
		var excluded uint64 = 0
		excludedPriority := Nothing
		newReplicationSet[i] = ReplicationSet{}
		for id := range voters {
			priority := t.getReplicationSetVoterPriority(id)
			if priority == Nothing {
				continue
			}
			_, wasIncluded := t.replicationSet[i][id]
			if priority > excludedPriority {
				newReplicationSet[i][id] = struct{}{}
			} else if priority < excludedPriority || (priority == excludedPriority && !wasIncluded) {
				if excludedPriority != Nothing {
					newReplicationSet[i][excluded] = struct{}{}
				}
				excluded = id
				excludedPriority = priority
			}
		}
	}

	return !t.replicationSet[0].Equals(newReplicationSet[0]) || !t.replicationSet[1].Equals(newReplicationSet[1])
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

func (t *peerTracker) committedPendingWitnessAck(rn *raft.RawNode) (uint64, []uint64) {
	pm := tracker.ProgressMap{}
	witnessMatch := map[uint64]uint64{}
	rn.WithProgress(func(id uint64, typ raft.ProgressType, pr tracker.Progress) {
		pm[id] = &pr
		if t.isWitness(id) {
			witnessMatch[id] = pr.Match
		}
	})

	idx0 := t.voters[0].CommittedIndex(newReplicationSetMatchAckIndexer(t.replicationSet[0], t.peers, pm))
	idx1 := t.voters[1].CommittedIndex(newReplicationSetMatchAckIndexer(t.replicationSet[1], t.peers, pm))

	idx := uint64(idx0)
	if idx0 > idx1 {
		idx = uint64(idx1)
	}

	witness := []uint64{}
	for id, match := range witnessMatch {
		if match < idx {
			witness = append(witness, id)
		}
	}

	return idx, witness
}

func (t *peerTracker) hasSubQuorumVotes(votes map[uint64]bool, incoming bool) bool {
	i := 0
	if !incoming {
		i = 1
	}
	voters := t.voters[i]
	extra := uint64(0)
	defer delete(votes, extra)
	for id := range voters {
		if _, voted := votes[id]; !voted {
			extra = id
			votes[extra] = true
			break
		}
	}
	return voters.VoteResult(votes) == quorum.VoteWon
}
