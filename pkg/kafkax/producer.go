package kafkax

import (
	"context"
	"time"
)

type Producer interface {
	Produce(context.Context, Message, ...ProduceOption) (ProduceResult, error)
	Close(context.Context) error
}

type ProduceResult struct {
	Topic     string
	Partition Partition
	Offset    Offset
	Timestamp time.Time
}

type ProduceOption func(*produceOptions)

type produceOptions struct {
	Headers []Header
}

func WithProduceHeaders(headers ...Header) ProduceOption {
	return func(options *produceOptions) {
		options.Headers = append(options.Headers, cloneHeaders(headers)...)
	}
}
