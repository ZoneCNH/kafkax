package kafkago

import (
	"context"
	"sync"
	"time"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/segmentio/kafka-go"
)

type consumer struct {
	d      *Driver
	sub    kafkax.Subscription
	reader *kafka.Reader
	mu     sync.Mutex
	last   []kafka.Message
	closed bool
}

func newConsumer(d *Driver, sub kafkax.Subscription) *consumer {
	cfg := kafka.ReaderConfig{
		Brokers:        d.cfg.Brokers,
		GroupID:        sub.GroupID,
		Dialer:         d.dialer,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafkaStartOffset(sub.StartOffset),
		MaxAttempts:    maxAttempts(d.cfg.Retry.MaxAttempts),
	}
	if d.cfg.Consumer.SessionTimeout > 0 {
		cfg.SessionTimeout = d.cfg.Consumer.SessionTimeout
	}
	if d.cfg.Consumer.HeartbeatInterval > 0 {
		cfg.HeartbeatInterval = d.cfg.Consumer.HeartbeatInterval
	}
	// MaxPollRecords: segmentio/kafka-go Reader 按 ReadMessage 单条消费，
	// 无原生 per-poll 记录上限；该字段在 Config.Validate 校验，
	// 实际批次粒度由调用方在 Poll 循环中控制。
	if len(sub.Topics) == 1 {
		cfg.Topic = sub.Topics[0]
	} else {
		cfg.GroupTopics = append([]string(nil), sub.Topics...)
	}
	return &consumer{d: d, sub: sub, reader: kafka.NewReader(cfg)}
}

func (c *consumer) Run(ctx context.Context, handler kafkax.Handler) error {
	const op = "kafkago.Consumer.Run"
	if handler == nil {
		return kafkax.NewError(kafkax.ErrorKindConfig, op, "handler is required", false)
	}
	for {
		batch, err := c.Poll(ctx)
		if err != nil {
			return err
		}
		for _, record := range batch.Records {
			if err := handler.Handle(ctx, record.Clone()); err != nil {
				return kafkax.WrapError(kafkax.ErrorKindConsume, op, "handle record", false, err)
			}
		}
		if err := c.Commit(ctx); err != nil {
			return err
		}
	}
}

func (c *consumer) Poll(ctx context.Context) (kafkax.RecordBatch, error) {
	const op = "kafkago.Consumer.Poll"
	if c == nil || c.reader == nil {
		return kafkax.RecordBatch{}, kafkax.NewError(kafkax.ErrorKindDriver, op, "consumer is closed", false)
	}
	ctx, cancel := c.d.timeoutContext(ctx, c.d.cfg.Timeout)
	defer cancel()
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		inc(c.d.metrics, kafkax.MetricConsumerErrorsTotal, map[string]string{"op": "poll"})
		return kafkax.RecordBatch{}, wrapContextOr(kafkax.ErrorKindConsume, op, "fetch message", true, err)
	}
	c.mu.Lock()
	c.last = []kafka.Message{msg}
	c.mu.Unlock()
	inc(c.d.metrics, kafkax.MetricConsumerMessagesTotal, map[string]string{"op": "poll"})
	return kafkax.RecordBatch{Records: []kafkax.Record{fromKafkaMessage(msg, c.sub.GroupID)}}, nil
}

func (c *consumer) Commit(ctx context.Context, offsets ...kafkax.Offset) error {
	const op = "kafkago.Consumer.Commit"
	if c == nil || c.reader == nil {
		return kafkax.NewError(kafkax.ErrorKindDriver, op, "consumer is closed", false)
	}
	ctx, cancel := c.d.timeoutContext(ctx, c.d.cfg.Timeout)
	defer cancel()
	c.mu.Lock()
	messages := append([]kafka.Message(nil), c.last...)
	c.mu.Unlock()
	if len(offsets) > 0 {
		allowed := make(map[int64]struct{}, len(offsets))
		for _, offset := range offsets {
			allowed[int64(offset)] = struct{}{}
		}
		filtered := messages[:0]
		for _, msg := range messages {
			if _, ok := allowed[msg.Offset]; ok {
				filtered = append(filtered, msg)
			}
		}
		messages = filtered
	}
	if len(messages) == 0 {
		return nil
	}
	if err := c.reader.CommitMessages(ctx, messages...); err != nil {
		inc(c.d.metrics, kafkax.MetricConsumerErrorsTotal, map[string]string{"op": "commit"})
		return wrapContextOr(kafkax.ErrorKindCommit, op, "commit messages", true, err)
	}
	inc(c.d.metrics, kafkax.MetricConsumerCommitsTotal, map[string]string{"op": "commit"})
	return nil
}

func (c *consumer) Pause(ctx context.Context, _ ...kafkax.TopicPartition) error {
	if ctx != nil && ctx.Err() != nil {
		return wrapContextOr(kafkax.ErrorKindUnavailable, "kafkago.Consumer.Pause", "pause consumer", false, ctx.Err())
	}
	return nil
}

func (c *consumer) Resume(ctx context.Context, _ ...kafkax.TopicPartition) error {
	if ctx != nil && ctx.Err() != nil {
		return wrapContextOr(kafkax.ErrorKindUnavailable, "kafkago.Consumer.Resume", "resume consumer", false, ctx.Err())
	}
	return nil
}

func (c *consumer) Close(ctx context.Context) error {
	const op = "kafkago.Consumer.Close"
	if c == nil || c.reader == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return wrapContextOr(kafkax.ErrorKindUnavailable, op, "close consumer", false, ctx.Err())
	}
	c.mu.Lock()
	reader := c.reader
	c.reader = nil
	c.closed = true
	c.mu.Unlock()
	if reader == nil {
		return nil
	}
	if err := reader.Close(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindConsume, op, "close consumer", true, err)
	}
	return nil
}

func kafkaStartOffset(policy kafkax.OffsetResetPolicy) int64 {
	switch policy {
	case kafkax.OffsetResetLatest:
		return kafka.LastOffset
	default:
		return kafka.FirstOffset
	}
}
