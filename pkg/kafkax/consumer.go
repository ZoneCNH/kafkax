package kafkax

import "context"

type Consumer interface {
	Subscribe(context.Context, Subscription) error
	Receive(context.Context) (ConsumerMessage, error)
	Commit(context.Context, ConsumerMessage) error
	Close(context.Context) error
}

type Subscription struct {
	Topics      []string
	GroupID     string
	StartOffset OffsetResetPolicy
}

type OffsetResetPolicy string

const (
	OffsetResetEarliest OffsetResetPolicy = "earliest"
	OffsetResetLatest   OffsetResetPolicy = "latest"
	OffsetResetNone     OffsetResetPolicy = "none"
)

type ConsumerMessage struct {
	Message
	GroupID string
}
