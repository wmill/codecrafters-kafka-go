package main

type FetchResponse struct {
	correlation_id   uint32
	throttle_time_ms uint32
	error_code       uint16
	session_id       uint32
	responses        []FetchResponseTopic
}

type FetchResponseTopic struct {
	topic_id   [16]byte
	partitions []FetchResponsePartition
}

type FetchResponsePartition struct {
	partition_index        uint32
	error_code             uint16
	high_watermark         uint64
	last_stable_offset     uint64
	log_start_offset       uint64
	aborted_transactions   []FetchResponseAbortedTransaction
	preferred_read_replica uint32
	records                []byte
}

type FetchResponseAbortedTransaction struct {
	producer_id  uint64
	first_offset uint64
}
