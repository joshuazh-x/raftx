package raftx

import (
	"sort"
)

type SubtermLocation struct {
	Subterm uint64
	Index   uint64
}
type SubtermSlicer struct {
	subterms map[uint64][]SubtermLocation
}

func NewSubtermSlicer() *SubtermSlicer {
	return &SubtermSlicer{
		subterms: map[uint64][]SubtermLocation{},
	}
}

func (s *SubtermSlicer) AddSubterm(term uint64, subterm uint64, index uint64) {
	slice, ok := s.subterms[term]
	if !ok {
		s.subterms[term] = []SubtermLocation{SubtermLocation{Subterm: subterm, Index: index}}
		return
	}

	pos := sort.Search(len(slice), func(i int) bool {
		return index < slice[i].Index
	})
	if pos == len(slice) {
		s.subterms[term] = append(slice, SubtermLocation{Subterm: subterm, Index: index})
		return
	}

	slice = append(slice, SubtermLocation{})
	copy(slice[pos+1:], slice[pos:])
	slice[pos] = SubtermLocation{Subterm: subterm, Index: index}
	s.subterms[term] = slice
}

func (s *SubtermSlicer) GetSubterm(term uint64, index uint64) uint64 {
	slice, ok := s.subterms[term]
	if !ok {
		return 0
	}
	pos := sort.Search(len(slice), func(i int) bool {
		return index < slice[i].Index
	})

	if pos == 0 {
		return 0
	}

	return slice[pos-1].Subterm
}
