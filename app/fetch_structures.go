package main

import (
	"fmt"
	"strings"
)

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

type FetchRequest struct {
	GenericRequest
	max_wait_ms           uint32
	min_bytes             uint32
	max_bytes             uint32
	isolation_level       uint8
	session_id            uint32
	session_epoch         uint32
	topics                []FetchRequestTopic
	forgotten_topics_data []FetchRequestForgottenTopicsData
	rack_id               []byte
}

type FetchRequestTopic struct {
	topic_id   [16]byte
	partitions []FetchRequestPartition
}

type FetchRequestPartition struct {
	partition            uint32
	current_leader_epoch uint32
	fetch_offset         uint64
	last_fetched_epoch   uint32
	log_start_offset     uint64
	partition_max_bytes  uint32
}

type FetchRequestForgottenTopicsData struct {
	topic_id   [16]byte
	partitions []uint32
}

func (fr *FetchResponse) String() string {
	var sb strings.Builder
	sb.WriteString("FetchResponse{\n")
	sb.WriteString(fmt.Sprintf("  correlation_id: %d\n", fr.correlation_id))
	sb.WriteString(fmt.Sprintf("  throttle_time_ms: %d\n", fr.throttle_time_ms))
	sb.WriteString(fmt.Sprintf("  error_code: %d\n", fr.error_code))
	sb.WriteString(fmt.Sprintf("  session_id: %d\n", fr.session_id))
	sb.WriteString("  responses: [\n")
	for _, topic := range fr.responses {
		sb.WriteString(fmt.Sprintf("    %s\n", topic.String()))
	}
	sb.WriteString("  ]\n")
	sb.WriteString("}")
	return sb.String()
}

func (frt *FetchResponseTopic) String() string {
	return fmt.Sprintf("FetchResponseTopic{topic_id: %x, partitions: %v}", frt.topic_id, frt.partitions)
}

func (fr *FetchRequest) String() string {
	var sb strings.Builder
	sb.WriteString("FetchRequest{\n")
	sb.WriteString(fmt.Sprintf("  max_wait_ms: %d\n", fr.max_wait_ms))
	sb.WriteString(fmt.Sprintf("  min_bytes: %d\n", fr.min_bytes))
	sb.WriteString(fmt.Sprintf("  max_bytes: %d\n", fr.max_bytes))
	sb.WriteString(fmt.Sprintf("  isolation_level: %d\n", fr.isolation_level))
	sb.WriteString(fmt.Sprintf("  session_id: %d\n", fr.session_id))
	sb.WriteString(fmt.Sprintf("  session_epoch: %d\n", fr.session_epoch))
	sb.WriteString("  topics: [\n")
	for _, topic := range fr.topics {
		sb.WriteString(fmt.Sprintf("    %s\n", topic.String()))
	}
	sb.WriteString("  ]\n")
	sb.WriteString(fmt.Sprintf("  forgotten_topics_data: %v\n", fr.forgotten_topics_data))
	sb.WriteString(fmt.Sprintf("  rack_id: %s\n", string(fr.rack_id)))
	sb.WriteString("}")
	return sb.String()
}

func (frt *FetchRequestTopic) String() string {
	return fmt.Sprintf("FetchRequestTopic{topic_id: %x, partitions: %v}", frt.topic_id, frt.partitions)
}
