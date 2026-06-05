package kafkax

import (
	"context"
	"time"
)

type Producer interface {
	Send(context.Context, Message) (ProduceResult, error)
	SendBatch(context.Context, []Message) (BatchProduceResult, error)
	Flush(context.Context) error
	Close(context.Context) error
}

type ProduceResult struct {
	Topic     string
	Partition Partition
	Offset    Offset
	Timestamp time.Time
}

type BatchProduceResult struct {
	Results []ProduceResult
}

func (r BatchProduceResult) Clone() BatchProduceResult {
	return BatchProduceResult{Results: append([]ProduceResult(nil), r.Results...)}
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
