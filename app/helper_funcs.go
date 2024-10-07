package main

func zigzag_encode(n int) int {
	return (n << 1) ^ (n >> 31)
}

func zigzag_decode(n int) int {
	return (n >> 1) ^ -(n & 1)
}

func fromZigzag(buf []byte) (int64, []byte) {
	var z uint64
	var shift uint
	for buf[0]&0x80 != 0 {
		z |= uint64(buf[0]&0x7F) << shift
		buf = buf[1:] // Move to the next byte
		shift += 7
	}
	z |= uint64(buf[0]) << shift
	if z&1 != 0 {
		return int64(z>>1) ^ -1, buf[1:]
	}
	return int64(z >> 1), buf[1:]
}
