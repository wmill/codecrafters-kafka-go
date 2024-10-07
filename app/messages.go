package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
)

// Request Header v0 => request_api_key request_api_version correlation_id
//   request_api_key => INT16
//   request_api_version => INT16
//   correlation_id => INT32

/*
ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER
  error_code => INT16
  api_keys => api_key min_version max_version TAG_BUFFER
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
*/

type ApiKeys struct {
	api_key     uint16
	min_version uint16
	max_version uint16
}

type ApiVersionsResponse struct {
	correlation_id   uint32
	error_code       uint16
	api_keys         []ApiKeys
	throttle_time_ms uint32
}

func (a *ApiVersionsResponse) toBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, a.correlation_id)
	binary.Write(buf, binary.BigEndian, a.error_code)
	// array length needs to be sent as a uvarint, use AppendVarint
	lengthVarint := make([]byte, 0)
	lengthVarint = binary.AppendUvarint(lengthVarint, uint64(len(a.api_keys)+1))
	buf.Write(lengthVarint)
	for _, apiKey := range a.api_keys {
		binary.Write(buf, binary.BigEndian, apiKey.api_key)
		binary.Write(buf, binary.BigEndian, apiKey.min_version)
		binary.Write(buf, binary.BigEndian, apiKey.max_version)
		buf.Write([]byte{0x00}) // TAG_BUFFER
	}

	binary.Write(buf, binary.BigEndian, a.throttle_time_ms)
	buf.Write([]byte{0x00}) // TAG_BUFFER

	return buf.Bytes()
}

/*
ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER
  client_software_name => COMPACT_STRING
  client_software_version => COMPACT_STRING
*/

/*

Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset TAG_BUFFER
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS

*/

type FetchResponse struct {
	correlation_id   uint32
	throttle_time_ms uint32
	error_code       uint16
	session_id       uint32
	responses        []FetchResponseTopic
}

type FetchResponseTopic struct {
	topic_id   []byte
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

func (f *FetchResponse) toBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, f.correlation_id)
	binary.Write(buf, binary.BigEndian, f.throttle_time_ms)
	binary.Write(buf, binary.BigEndian, f.error_code)
	binary.Write(buf, binary.BigEndian, f.session_id)

	// array length needs to be sent as a uvarint, use AppendVarint
	lengthVarint := make([]byte, 0)
	// arrayLength := len(f.responses) + 1
	// if arrayLength == 1 {}

	lengthVarint = binary.AppendUvarint(lengthVarint, uint64(len(f.responses)+1))
	buf.Write(lengthVarint)

	for _, response := range f.responses {
		buf.Write(response.topic_id)
		// array length needs to be sent as a uvarint, use AppendVarint
		lengthVarint := make([]byte, 0)
		lengthVarint = binary.AppendUvarint(lengthVarint, uint64(len(f.responses)+1))
		buf.Write(lengthVarint)
		for _, partition := range response.partitions {
			binary.Write(buf, binary.BigEndian, partition.partition_index)
			binary.Write(buf, binary.BigEndian, partition.error_code)
			binary.Write(buf, binary.BigEndian, partition.high_watermark)
			binary.Write(buf, binary.BigEndian, partition.last_stable_offset)
			binary.Write(buf, binary.BigEndian, partition.log_start_offset)
			for _, aborted_transaction := range partition.aborted_transactions {
				binary.Write(buf, binary.BigEndian, aborted_transaction.producer_id)
				binary.Write(buf, binary.BigEndian, aborted_transaction.first_offset)
				buf.Write([]byte{0x00}) // TAG_BUFFER
			}
			binary.Write(buf, binary.BigEndian, partition.preferred_read_replica)
			buf.Write(partition.records)
			buf.Write([]byte{0x00}) // TAG_BUFFER
		}
		buf.Write([]byte{0x00}) // TAG_BUFFER
	}
	buf.Write([]byte{0x00}) // TAG_BUFFER
	buf.Write([]byte{0x00}) // TAG_BUFFER
	return buf.Bytes()
}

func createResponse(rawResponse []byte) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(rawResponse)))
	binary.Write(buf, binary.BigEndian, rawResponse)
	return buf.Bytes()
}

func parseRequest(conn net.Conn) {

	// read the length of the message
	lengthBuffer := make([]byte, 4)
	_, err := io.ReadFull(conn, lengthBuffer)

	if err != nil && err != io.EOF {
		fmt.Println("Error reading length of message")
		fmt.Println(err)
		return
	}

	length := binary.BigEndian.Uint32(lengthBuffer)

	messageBuffer := make([]byte, length)
	_, err = io.ReadFull(conn, messageBuffer)
	if err != nil {
		fmt.Println("Error reading message")
		fmt.Println(err)
		return
	}

	request_api_key := binary.BigEndian.Uint16(messageBuffer[0:2])
	request_api_version := binary.BigEndian.Uint16(messageBuffer[2:4])
	correlation_id := binary.BigEndian.Uint32(messageBuffer[4:8])
	// create a byte slice starting after the correlation id

	fmt.Println("Request API Key: ", request_api_key)
	fmt.Println("Request API Version: ", request_api_version)
	fmt.Println("Correlation ID: ", correlation_id)

	switch request_api_key {
	case API_VERSIONS:
		handleApiVersionsRequest(conn, messageBuffer)
	case FETCH:
		handleFetchRequest(conn, messageBuffer)
	}
}

func handleApiVersionsRequest(conn net.Conn, message []byte) {
	request_api_version := binary.BigEndian.Uint16(message[2:4])
	correlation_id := binary.BigEndian.Uint32(message[4:8])
	// todo parse out the agent strings and process the rest of request.

	message_error_code := NONE
	if request_api_version > 4 {
		message_error_code = UNSUPPORTED_VERSION
	}

	api_response := ApiVersionsResponse{
		correlation_id:   correlation_id,
		error_code:       uint16(message_error_code),
		throttle_time_ms: 0,
		api_keys: []ApiKeys{
			{API_VERSIONS, 0, 4},
			{FETCH, 0, 16},
		},
	}

	api_versions_response := api_response.toBytes()
	fmt.Println(hex.EncodeToString(api_versions_response))

	response := createResponse(api_versions_response)
	conn.Write(response)

}

func handleFetchRequest(conn net.Conn, message []byte) {
	//request_api_version := binary.BigEndian.Uint16(message[2:4])
	correlation_id := binary.BigEndian.Uint32(message[4:8])
	fmt.Println("Correlation ID (fetch): ", correlation_id)

	fetch_response := FetchResponse{
		correlation_id:   correlation_id,
		throttle_time_ms: 0,
		error_code:       NONE,
		session_id:       0,
		responses:        []FetchResponseTopic{},
	}

	fetch_response_bytes := fetch_response.toBytes()
	fmt.Println(hex.EncodeToString(fetch_response_bytes))

	response := createResponse(fetch_response_bytes)
	conn.Write(response)
}
