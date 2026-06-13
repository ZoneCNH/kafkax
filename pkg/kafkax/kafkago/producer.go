package kafkago

import (
	"context"
	"sync"
	"time"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/segmentio/kafka-go"
)

type producer struct {
	d      *Driver
	writer *kafka.Writer
	mu     sync.Mutex
	last   []kafka.Message
}

func newProducer(d *Driver) *producer {
	p := &producer{d: d}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(d.cfg.Brokers...),
		Transport:    d.transport,
		RequiredAcks: requiredAcks(d.cfg.Producer.RequiredAcks),
		BatchBytes:   int64(d.cfg.Producer.BatchBytes),
		MaxAttempts:  maxAttempts(d.cfg.Retry.MaxAttempts),
		WriteTimeout: d.cfg.Timeout,
		ReadTimeout:  d.cfg.Timeout,
		Completion: func(messages []kafka.Message, err error) {
			p.last = append(p.last[:0], messages...)
		},
	}
	if writer.BatchBytes <= 0 {
		writer.BatchBytes = 1048576
	}
	p.writer = writer
	return p
}

func (p *producer) Send(ctx context.Context, message kafkax.Message, opts ...kafkax.ProduceOption) (kafkax.ProduceResult, error) {
	result, err := p.SendBatch(ctx, []kafkax.Message{message}, opts...)
	if err != nil {
		return kafkax.ProduceResult{}, err
	}
	if len(result.Results) == 0 {
		return kafkax.ProduceResult{}, nil
	}
	return result.Results[0], nil
}

func (p *producer) SendBatch(ctx context.Context, messages []kafkax.Message, opts ...kafkax.ProduceOption) (kafkax.BatchProduceResult, error) {
	const op = "kafkago.Producer.SendBatch"
	if p == nil || p.writer == nil {
		return kafkax.BatchProduceResult{}, kafkax.NewError(kafkax.ErrorKindDriver, op, "producer is closed", false)
	}
	ctx, cancel := p.d.timeoutContext(ctx, p.d.cfg.Timeout)
	defer cancel()
	kafkaMessages := make([]kafka.Message, len(messages))
	for i, msg := range messages {
		if normalizeTopic(msg.Topic) == "" {
			return kafkax.BatchProduceResult{}, kafkax.NewError(kafkax.ErrorKindConfig, op, "message topic is required", false)
		}
		kafkaMessages[i] = toKafkaMessage(msg)
	}
	start := time.Now()
	p.mu.Lock()
	p.last = p.last[:0]
	err := p.writer.WriteMessages(ctx, kafkaMessages...)
	completed := append([]kafka.Message(nil), p.last...)
	p.mu.Unlock()
	if err != nil {
		inc(p.d.metrics, kafkax.MetricProducerErrorsTotal, map[string]string{"op": "send"})
		return kafkax.BatchProduceResult{}, wrapContextOr(kafkax.ErrorKindProduce, op, "write messages", true, err)
	}
	observe(p.d.metrics, kafkax.MetricProducerLatencySeconds, time.Since(start).Seconds(), map[string]string{"op": "send"})
	inc(p.d.metrics, kafkax.MetricProducerMessagesTotal, map[string]string{"op": "send"})
	if len(completed) == 0 {
		completed = kafkaMessages
	}
	results := make([]kafkax.ProduceResult, len(completed))
	for i, msg := range completed {
		results[i] = fromKafkaProduceResult(msg)
	}
	return kafkax.BatchProduceResult{Results: results}, nil
}

func (p *producer) Flush(ctx context.Context) error {
	if p == nil || p.writer == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return wrapContextOr(kafkax.ErrorKindUnavailable, "kafkago.Producer.Flush", "flush", false, ctx.Err())
	}
	return nil
}

func (p *producer) Close(ctx context.Context) error {
	const op = "kafkago.Producer.Close"
	if p == nil || p.writer == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return wrapContextOr(kafkax.ErrorKindUnavailable, op, "close producer", false, ctx.Err())
	}
	p.mu.Lock()
	writer := p.writer
	p.writer = nil
	p.mu.Unlock()
	if writer == nil {
		return nil
	}
	if err := writer.Close(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindProduce, op, "close producer", true, err)
	}
	return nil
}

func requiredAcks(value int) kafka.RequiredAcks {
	switch value {
	case 0:
		return kafka.RequireNone
	case 1:
		return kafka.RequireOne
	default:
		return kafka.RequireAll
	}
}

func maxAttempts(value int) int {
	if value > 0 {
		return value
	}
	return 3
}

func toKafkaMessage(msg kafkax.Message) kafka.Message {
	headers := make([]kafka.Header, len(msg.Headers))
	for i, header := range msg.Headers {
		headers[i] = kafka.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return kafka.Message{
		Topic:     msg.Topic,
		Key:       append([]byte(nil), msg.Key...),
		Value:     append([]byte(nil), msg.Value...),
		Headers:   headers,
		Time:      msg.Timestamp,
		Partition: int(msg.Partition),
		Offset:    int64(msg.Offset),
	}
}

func fromKafkaProduceResult(msg kafka.Message) kafkax.ProduceResult {
	return kafkax.ProduceResult{
		Topic:     msg.Topic,
		Partition: kafkax.Partition(msg.Partition),
		Offset:    kafkax.Offset(msg.Offset),
		Timestamp: msg.Time,
	}
}

func fromKafkaMessage(msg kafka.Message, group string) kafkax.Record {
	headers := make([]kafkax.Header, len(msg.Headers))
	for i, header := range msg.Headers {
		headers[i] = kafkax.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return kafkax.Record{Message: kafkax.Message{
		Topic:     msg.Topic,
		Key:       append([]byte(nil), msg.Key...),
		Value:     append([]byte(nil), msg.Value...),
		Headers:   headers,
		Timestamp: msg.Time,
		Partition: kafkax.Partition(msg.Partition),
		Offset:    kafkax.Offset(msg.Offset),
	}, GroupID: group}
}
