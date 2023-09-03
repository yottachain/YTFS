package util

import "bytes"
import "math"

func BytesPlusPlusRecursionFun(data []byte) bool {
	var byteMax = []byte{math.MaxUint8}
	if len(data) == 0 {
		return false
	}
	for i := len(data) - 1; i >= 0; i-- {
		if !bytes.Equal(data[i:i+1], byteMax) {
			data[i]++
			break
		}
		if BytesPlusPlusRecursionFun(data[0:i]) {
			data[i] = 0
			break
		}
	}

	return true
}
