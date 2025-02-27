package raftx

import "encoding/binary"

func encodeUint64(v uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, v)
	return data
}

func decodeUint64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}
