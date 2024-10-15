package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
)

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
