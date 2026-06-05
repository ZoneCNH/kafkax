package kafkax

import "context"

type Consumer interface {
	Run(context.Context, Handler) error
	Poll(context.Context) (RecordBatch, error)
	Commit(context.Context, ...Offset) error
	Pause(context.Context, ...TopicPartition) error
	Resume(context.Context, ...TopicPartition) error
	Close(context.Context) error
}

type Handler interface {
	Handle(context.Context, Record) error
}

type HandlerFunc func(context.Context, Record) error

func (fn HandlerFunc) Handle(ctx context.Context, record Record) error {
	return fn(ctx, record)
}

type Subscription struct {
	Topics      []string
	GroupID     string
	StartOffset OffsetResetPolicy
}

func (s Subscription) Clone() Subscription {
	s.Topics = append([]string(nil), s.Topics...)
	return s
}

type OffsetResetPolicy string

const (
	OffsetResetEarliest OffsetResetPolicy = "earliest"
	OffsetResetLatest   OffsetResetPolicy = "latest"
	OffsetResetNone     OffsetResetPolicy = "none"
)

type TopicPartition struct {
	Topic     string
	Partition Partition
}

type Record struct {
	Message
	GroupID string
}

func (r Record) Clone() Record {
	r.Message = r.Message.Clone()
	return r
}

type RecordBatch struct {
	Records []Record
}

func (b RecordBatch) Clone() RecordBatch {
	if len(b.Records) == 0 {
		return RecordBatch{}
	}
	cloned := RecordBatch{Records: make([]Record, len(b.Records))}
	for i, record := range b.Records {
		cloned.Records[i] = record.Clone()
	}
	return cloned
}

type ConsumerMessage = Record
