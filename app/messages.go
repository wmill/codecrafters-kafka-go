package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
)

/*

Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
  request_api_key => INT16
  request_api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING

Response Header v1 => correlation_id TAG_BUFFER
  correlation_id => INT32

*/

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

COMPACT_STRING is a length-prefixed string. The length is encoded as a uvarint followed by the UTF-8 encoded string.
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


NULLABLE_STRING => length-prefixed string
int16 length followed by string

Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
  request_api_key => INT16
  request_api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING

Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
*/

type GenericRequest struct {
	request_api_key     uint16
	request_api_version uint16
	correlation_id      uint32
	client_id           []byte
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
	// quick check will remove later
	correlation_id := binary.BigEndian.Uint32(message[4:8])
	fmt.Println("Correlation ID (fetch): ", correlation_id)

	fetch_request := createFetchRequestFromBytes(message)
	fmt.Println(fetch_request)

	// fetch_response := FetchResponse{
	// 	correlation_id:   correlation_id,
	// 	throttle_time_ms: 0,
	// 	error_code:       NONE,
	// 	session_id:       0,
	// 	responses:        []FetchResponseTopic{},
	// }

	fetch_response := createFetchResponseFromFetchRequest(fetch_request)
	fetch_response.error_code = UNKNOWN_TOPIC_ID

	fetch_response_bytes := fetch_response.toBytes()
	fmt.Printf("Fetch Response bytes: %s\n", hex.EncodeToString(fetch_response_bytes))
	fmt.Println("Fetch Response length: ", len(fetch_response_bytes))

	// FIXME trimming off the last 53 bytes to see if it fixes the issue
	// fetch_response_bytes = fetch_response_bytes[:len(fetch_response_bytes)-53]
	// fmt.Printf("Fetch Response bytes: %s\n", hex.EncodeToString(fetch_response_bytes))
	// fmt.Println("Fetch Response length: ", len(fetch_response_bytes))

	response := createResponse(fetch_response_bytes)
	conn.Write(response)
}
