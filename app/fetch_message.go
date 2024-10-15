
func createFetchRequestFromBytes(message []byte) FetchRequest {
	// parse the request

	printByteSliceAsGoCode(message)

	buf := bytes.NewBuffer(message)
	fr := FetchRequest{}

	fr.request_api_key = binary.BigEndian.Uint16(buf.Next(2))
	fr.request_api_version = binary.BigEndian.Uint16(buf.Next(2))
	fr.correlation_id = binary.BigEndian.Uint32(buf.Next(4))

	// client id is a nullable string not a compact string
	client_id_length := int16(binary.BigEndian.Uint16(buf.Next(2)))
	if client_id_length > 0 {
		fr.client_id = buf.Next(int(client_id_length))
	}

	// always the fucking tag buffer
	buf.Next(1) // TAG_BUFFER

	fr.max_wait_ms = binary.BigEndian.Uint32(buf.Next(4))
	fr.min_bytes = binary.BigEndian.Uint32(buf.Next(4))
	fr.max_bytes = binary.BigEndian.Uint32(buf.Next(4))
	fr.isolation_level = buf.Next(1)[0]
	fr.session_id = binary.BigEndian.Uint32(buf.Next(4))
	fr.session_epoch = binary.BigEndian.Uint32(buf.Next(4))

	topic_length, topic_length_bytes := binary.Uvarint(buf.Bytes())

	if topic_length_bytes < 0 {
		fmt.Println("Error parsing topic_length")
		return fr
	}
	buf.Next(topic_length_bytes)
	for i := 0; i < int(topic_length)-1; i++ {
		frt := FetchRequestTopic{}
		// topic_id_length, topic_id_length_bytes := binary.Uvarint(buf.Bytes())

		// if topic_id_length_bytes < 0 {
		// 	fmt.Println("Error parsing topic_id_length")
		// 	return fr
		// }
		// buf.Next(topic_id_length_bytes)

		var topicID [16]byte
		copy(topicID[:], buf.Next(16))
		frt.topic_id = topicID
		fmt.Println("Topic ID: ", frt.topic_id)
		partition_length, partition_length_bytes := binary.Uvarint(buf.Bytes())

		if partition_length_bytes < 0 {
			fmt.Println("Error parsing partition_length")
			return fr
		}

		buf.Next(partition_length_bytes)
		for j := 0; j < int(partition_length)-1; j++ {
			frp := FetchRequestPartition{}
			frp.partition = binary.BigEndian.Uint32(buf.Next(4))
			frp.current_leader_epoch = binary.BigEndian.Uint32(buf.Next(4))
			frp.fetch_offset = binary.BigEndian.Uint64(buf.Next(8))
			frp.last_fetched_epoch = binary.BigEndian.Uint32(buf.Next(4))
			frp.log_start_offset = binary.BigEndian.Uint64(buf.Next(8))
			frp.partition_max_bytes = binary.BigEndian.Uint32(buf.Next(4))
			frt.partitions = append(frt.partitions, frp)
			buf.Next(1) // TAG_BUFFER
		}
		fr.topics = append(fr.topics, frt)
		buf.Next(1) // TAG_BUFFER
	}

	forgotten_topics_length, forgotten_topics_length_bytes := binary.Uvarint(buf.Bytes())
	buf.Next(forgotten_topics_length_bytes)
	for i := 0; i < int(forgotten_topics_length)-1; i++ {
		frftd := FetchRequestForgottenTopicsData{}
		// topic_id_length, topic_id_length_bytes := binary.Uvarint(buf.Bytes())

		// if topic_id_length_bytes < 0 {
		// 	fmt.Println("Error parsing topic_id_length")
		// 	return fr
		// }
		// buf.Next(topic_id_length_bytes)
		// frftd.topic_id = buf.Next(int(topic_id_length))
		var topicID [16]byte
		copy(topicID[:], buf.Next(16))
		frftd.topic_id = topicID

		partition_length, partition_length_bytes := binary.Uvarint(buf.Bytes())

		if partition_length_bytes < 0 {
			fmt.Println("Error parsing partition_length")
			return fr
		}
		buf.Next(partition_length_bytes)
		for j := 0; j < int(partition_length)-1; j++ {
			partition, partition_bytes := binary.Uvarint(buf.Bytes())
			if partition_bytes < 0 {
				fmt.Println("Error parsing partition")
				return fr
			}
			buf.Next(partition_bytes)
			frftd.partitions = append(frftd.partitions, uint32(partition))
		}
		fr.forgotten_topics_data = append(fr.forgotten_topics_data, frftd)
		buf.Next(1) // TAG_BUFFER
	}

	rack_id_length, rack_id_length_bytes := binary.Uvarint(buf.Bytes())
	buf.Next(rack_id_length_bytes)
	fr.rack_id = buf.Next(int(rack_id_length))

	return fr
}

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

func (f *FetchResponse) toBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, f.correlation_id)
	buf.Write([]byte{0x00}) // TAG_BUFFER
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
		buf.Write(response.topic_id[:]) // Convert [16]byte to []byte

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

func createFetchResponseFromFetchRequest(fr FetchRequest) FetchResponse {
	frr := FetchResponse{
		correlation_id:   fr.correlation_id,
		throttle_time_ms: 0,
		error_code:       NONE,
		session_id:       0,
		responses: []FetchResponseTopic{
			{
				topic_id: fr.topics[0].topic_id,
				partitions: []FetchResponsePartition{{
					partition_index:        0,
					error_code:             0,
					high_watermark:         0,
					last_stable_offset:     0,
					log_start_offset:       0,
					aborted_transactions:   []FetchResponseAbortedTransaction{},
					preferred_read_replica: 0,
					records:                []byte{},
				}},
			},
		},
	}

	return frr
}