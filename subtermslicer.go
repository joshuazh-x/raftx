package raftx

import (
	"sort"
)

type SubtermInfo struct {
	Term    uint64
	Subterm uint64
	Index   uint64
}
type SubtermSlicer struct {
	subterms []SubtermInfo
}

func NewSubtermSlicer() *SubtermSlicer {
	return &SubtermSlicer{
		subterms: []SubtermInfo{},
	}
}

func (s *SubtermSlicer) AppendSubterm(term uint64, subterm uint64, index uint64) {
	pos := sort.Search(len(s.subterms), func(i int) bool {
		return index <= s.subterms[i].Index
	})
	if pos == len(s.subterms) {
		s.subterms = append(s.subterms, SubtermInfo{Term: term, Subterm: subterm, Index: index})
		return
	}

	s.subterms = append(s.subterms[:pos], SubtermInfo{Term: term, Subterm: subterm, Index: index})
}

func (s *SubtermSlicer) GetSubterm(index uint64) SubtermInfo {
	pos := sort.Search(len(s.subterms), func(i int) bool {
		return index < s.subterms[i].Index
	})

	if pos == 0 {
		panic("index out of range")
	}

	return s.subterms[pos-1]
}
