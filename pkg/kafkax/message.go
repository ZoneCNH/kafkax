package kafkax

import "time"

type Partition int32

type Offset int64

type Header struct {
	Key   string
	Value []byte
}

type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
	Partition Partition
	Offset    Offset
}

func (m Message) Clone() Message {
	cloned := m
	cloned.Key = append([]byte(nil), m.Key...)
	cloned.Value = append([]byte(nil), m.Value...)
	cloned.Headers = cloneHeaders(m.Headers)
	return cloned
}

func cloneHeaders(headers []Header) []Header {
	if len(headers) == 0 {
		return nil
	}
	cloned := make([]Header, len(headers))
	for i, header := range headers {
		cloned[i] = Header{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	}
	return cloned
}
