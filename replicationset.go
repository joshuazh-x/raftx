package raftx

import (
	"strconv"
	"strings"
)

type ReplicationSet map[uint64]struct{}

func (r ReplicationSet) Equals(rs ReplicationSet) bool {
	if len(r) != len(rs) {
		return false
	}
	for k := range r {
		if _, ok := rs[k]; !ok {
			return false
		}
	}
	return true
}

func (r ReplicationSet) ToArray() []uint64 {
	result := make([]uint64, 0, len(r))
	for v := range r {
		result = append(result, v)
	}
	return result
}

func (r ReplicationSet) String() string {
	arr := r.ToArray()
	strArr := make([]string, len(arr))
	for i, v := range arr {
		strArr[i] = strconv.FormatUint(v, 16)
	}
	return strings.Join(strArr, ",")
}

func (r ReplicationSet) Contains(id uint64) bool {
	_, ok := r[id]
	return ok
}
