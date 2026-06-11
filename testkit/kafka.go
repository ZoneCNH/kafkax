package testkit

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
)

type KafkaFake struct {
	mu      sync.Mutex
	closed  bool
	records []kafkax.Record
	topics  map[string]kafkax.TopicSpec
}

func FakeKafka() *KafkaFake {
	return &KafkaFake{topics: make(map[string]kafkax.TopicSpec)}
}

func (f *KafkaFake) Producer(ctx context.Context) (kafkax.Producer, error) {
	if err := f.ready(ctx, "testkit.KafkaFake.Producer"); err != nil {
		return nil, err
	}
	return &fakeProducer{driver: f}, nil
}

func (f *KafkaFake) Consumer(ctx context.Context) (kafkax.Consumer, error) {
	if err := f.ready(ctx, "testkit.KafkaFake.Consumer"); err != nil {
		return nil, err
	}
	return &fakeConsumer{driver: f}, nil
}

func (f *KafkaFake) Admin(ctx context.Context) (kafkax.Admin, error) {
	if err := f.ready(ctx, "testkit.KafkaFake.Admin"); err != nil {
		return nil, err
	}
	return &fakeAdmin{driver: f}, nil
}

func FakeProducer(ctx context.Context) (kafkax.Producer, error) {
	return FakeKafka().Producer(ctx)
}

func FakeConsumer(ctx context.Context) (kafkax.Consumer, error) {
	return FakeKafka().Consumer(ctx)
}

func FakeAdmin(ctx context.Context) (kafkax.Admin, error) {
	return FakeKafka().Admin(ctx)
}

func GoldenRecord(topic string) kafkax.Message {
	return kafkax.Message{
		Topic: topic,
		Key:   []byte("golden-key"),
		Value: []byte("golden-value"),
		Headers: []kafkax.Header{
			{Key: "content-type", Value: []byte("application/json")},
		},
	}
}

func (f *KafkaFake) ready(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, op, "context closed", true, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return kafkax.NewError(kafkax.ErrorKindDriver, op, "fake kafka is closed", false)
	}
	return nil
}

type fakeProducer struct {
	driver *KafkaFake
	closed bool
}

func (p *fakeProducer) Send(ctx context.Context, message kafkax.Message, options ...kafkax.ProduceOption) (kafkax.ProduceResult, error) {
	batch, err := p.SendBatch(ctx, []kafkax.Message{message}, options...)
	if err != nil {
		return kafkax.ProduceResult{}, err
	}
	return batch.Results[0], nil
}

func (p *fakeProducer) SendBatch(ctx context.Context, messages []kafkax.Message, _ ...kafkax.ProduceOption) (kafkax.BatchProduceResult, error) {
	if err := ctx.Err(); err != nil {
		return kafkax.BatchProduceResult{}, kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeProducer.SendBatch", "context closed", true, err)
	}
	p.driver.mu.Lock()
	defer p.driver.mu.Unlock()
	if p.closed || p.driver.closed {
		return kafkax.BatchProduceResult{}, kafkax.NewError(kafkax.ErrorKindProduce, "testkit.fakeProducer.SendBatch", "producer is closed", false)
	}
	result := kafkax.BatchProduceResult{Results: make([]kafkax.ProduceResult, len(messages))}
	for i, message := range messages {
		cloned := message.Clone()
		cloned.Offset = kafkax.Offset(len(p.driver.records))
		if cloned.Timestamp.IsZero() {
			cloned.Timestamp = time.Now().UTC()
		}
		record := kafkax.Record{Message: cloned}
		p.driver.records = append(p.driver.records, record)
		result.Results[i] = kafkax.ProduceResult{
			Topic:     cloned.Topic,
			Partition: cloned.Partition,
			Offset:    cloned.Offset,
			Timestamp: cloned.Timestamp,
		}
	}
	return result, nil
}

func (p *fakeProducer) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeProducer.Flush", "context closed", true, err)
	}
	return nil
}

func (p *fakeProducer) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeProducer.Close", "context closed", true, err)
	}
	p.driver.mu.Lock()
	defer p.driver.mu.Unlock()
	p.closed = true
	return nil
}

type fakeConsumer struct {
	driver *KafkaFake
	closed bool
	paused map[kafkax.TopicPartition]bool
	next   int
}

func (c *fakeConsumer) Run(ctx context.Context, handler kafkax.Handler) error {
	for {
		batch, err := c.Poll(ctx)
		if err != nil {
			return err
		}
		if len(batch.Records) == 0 {
			return nil
		}
		for _, record := range batch.Records {
			if err := handler.Handle(ctx, record.Clone()); err != nil {
				return kafkax.WrapError(kafkax.ErrorKindConsume, "testkit.fakeConsumer.Run", "handler failed", false, err)
			}
		}
	}
}

func (c *fakeConsumer) Poll(ctx context.Context) (kafkax.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		return kafkax.RecordBatch{}, kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeConsumer.Poll", "context closed", true, err)
	}
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()
	if c.closed || c.driver.closed {
		return kafkax.RecordBatch{}, kafkax.NewError(kafkax.ErrorKindConsume, "testkit.fakeConsumer.Poll", "consumer is closed", false)
	}
	for c.next < len(c.driver.records) {
		record := c.driver.records[c.next].Clone()
		c.next++
		if c.paused != nil && c.paused[kafkax.TopicPartition{Topic: record.Topic, Partition: record.Partition}] {
			continue
		}
		return kafkax.RecordBatch{Records: []kafkax.Record{record}}, nil
	}
	return kafkax.RecordBatch{}, nil
}

func (c *fakeConsumer) Commit(ctx context.Context, _ ...kafkax.Offset) error {
	if err := ctx.Err(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeConsumer.Commit", "context closed", true, err)
	}
	return nil
}

func (c *fakeConsumer) Pause(ctx context.Context, partitions ...kafkax.TopicPartition) error {
	if err := ctx.Err(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeConsumer.Pause", "context closed", true, err)
	}
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()
	if c.paused == nil {
		c.paused = make(map[kafkax.TopicPartition]bool)
	}
	for _, partition := range partitions {
		c.paused[partition] = true
	}
	return nil
}

func (c *fakeConsumer) Resume(ctx context.Context, partitions ...kafkax.TopicPartition) error {
	if err := ctx.Err(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeConsumer.Resume", "context closed", true, err)
	}
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()
	for _, partition := range partitions {
		delete(c.paused, partition)
	}
	return nil
}

func (c *fakeConsumer) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeConsumer.Close", "context closed", true, err)
	}
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()
	c.closed = true
	return nil
}

type fakeAdmin struct {
	driver *KafkaFake
	closed bool
}

func (a *fakeAdmin) DescribeTopics(ctx context.Context, names ...string) ([]kafkax.TopicDescription, error) {
	if err := ctx.Err(); err != nil {
		return nil, kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeAdmin.DescribeTopics", "context closed", true, err)
	}
	a.driver.mu.Lock()
	defer a.driver.mu.Unlock()
	if a.closed || a.driver.closed {
		return nil, kafkax.NewError(kafkax.ErrorKindAdmin, "testkit.fakeAdmin.DescribeTopics", "admin is closed", false)
	}
	descriptions := make([]kafkax.TopicDescription, 0, len(names))
	for _, name := range names {
		spec, ok := a.driver.topics[name]
		if !ok {
			continue
		}
		descriptions = append(descriptions, describeTopic(spec))
	}
	return descriptions, nil
}

func (a *fakeAdmin) PlanTopics(ctx context.Context, specs ...kafkax.TopicSpec) (kafkax.TopicPlan, error) {
	if err := ctx.Err(); err != nil {
		return kafkax.TopicPlan{}, kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeAdmin.PlanTopics", "context closed", true, err)
	}
	a.driver.mu.Lock()
	defer a.driver.mu.Unlock()
	if a.closed || a.driver.closed {
		return kafkax.TopicPlan{}, kafkax.NewError(kafkax.ErrorKindAdmin, "testkit.fakeAdmin.PlanTopics", "admin is closed", false)
	}
	plan := kafkax.TopicPlan{Action: kafkax.TopicPlanNoop, Specs: cloneTopicSpecs(specs), DryRun: true}
	for _, spec := range specs {
		current, exists := a.driver.topics[spec.Name]
		if !exists {
			plan.Action = mergeTopicAction(plan.Action, kafkax.TopicPlanCreate)
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name, From: "", To: "create"})
			continue
		}
		if current.Partitions != spec.Partitions {
			plan.Action = mergeTopicAction(plan.Action, kafkax.TopicPlanUpdate)
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name + ".partitions", From: strconv.Itoa(current.Partitions), To: strconv.Itoa(spec.Partitions)})
		}
		if current.ReplicationFactor != spec.ReplicationFactor {
			plan.Action = mergeTopicAction(plan.Action, kafkax.TopicPlanUpdate)
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name + ".replication_factor", From: strconv.Itoa(current.ReplicationFactor), To: strconv.Itoa(spec.ReplicationFactor)})
		}
		if current.Retention != spec.Retention {
			plan.Action = mergeTopicAction(plan.Action, kafkax.TopicPlanUpdate)
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name + ".retention", From: current.Retention.String(), To: spec.Retention.String()})
		}
		if current.CleanupPolicy != spec.CleanupPolicy {
			plan.Action = mergeTopicAction(plan.Action, kafkax.TopicPlanUpdate)
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name + ".cleanup_policy", From: string(current.CleanupPolicy), To: string(spec.CleanupPolicy)})
		}
		if current.Compression != spec.Compression {
			plan.Action = mergeTopicAction(plan.Action, kafkax.TopicPlanUpdate)
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name + ".compression", From: string(current.Compression), To: string(spec.Compression)})
		}
		if current.MinInSyncReplicas != spec.MinInSyncReplicas {
			plan.Action = mergeTopicAction(plan.Action, kafkax.TopicPlanUpdate)
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name + ".min_in_sync_replicas", From: strconv.Itoa(current.MinInSyncReplicas), To: strconv.Itoa(spec.MinInSyncReplicas)})
		}
		if !reflect.DeepEqual(current.Config, spec.Config) {
			plan.Action = mergeTopicAction(plan.Action, kafkax.TopicPlanUpdate)
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name + ".config", From: fmt.Sprint(current.Config), To: fmt.Sprint(spec.Config)})
		}
	}
	return plan.Clone(), nil
}

func (a *fakeAdmin) ApplyTopics(ctx context.Context, plan kafkax.TopicPlan) (kafkax.TopicApplyResult, error) {
	if err := ctx.Err(); err != nil {
		return kafkax.TopicApplyResult{}, kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeAdmin.ApplyTopics", "context closed", true, err)
	}
	a.driver.mu.Lock()
	defer a.driver.mu.Unlock()
	if a.closed || a.driver.closed {
		return kafkax.TopicApplyResult{}, kafkax.NewError(kafkax.ErrorKindAdmin, "testkit.fakeAdmin.ApplyTopics", "admin is closed", false)
	}
	applied := make([]kafkax.TopicDescription, 0, len(plan.Specs))
	for _, spec := range plan.Specs {
		cloned := spec.Clone()
		a.driver.topics[cloned.Name] = cloned
		applied = append(applied, describeTopic(cloned))
	}
	return kafkax.TopicApplyResult{Applied: applied, DryRun: plan.DryRun}.Clone(), nil
}

func (a *fakeAdmin) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, "testkit.fakeAdmin.Close", "context closed", true, err)
	}
	a.driver.mu.Lock()
	defer a.driver.mu.Unlock()
	a.closed = true
	return nil
}

func cloneTopicSpecs(specs []kafkax.TopicSpec) []kafkax.TopicSpec {
	cloned := make([]kafkax.TopicSpec, len(specs))
	for i, spec := range specs {
		cloned[i] = spec.Clone()
	}
	return cloned
}

func describeTopic(spec kafkax.TopicSpec) kafkax.TopicDescription {
	return kafkax.TopicDescription{
		Name:              spec.Name,
		Partitions:        spec.Partitions,
		ReplicationFactor: spec.ReplicationFactor,
		Retention:         spec.Retention,
		CleanupPolicy:     spec.CleanupPolicy,
		Compression:       spec.Compression,
		MinInSyncReplicas: spec.MinInSyncReplicas,
		Config:            spec.Clone().Config,
	}
}

func mergeTopicAction(current kafkax.TopicPlanAction, next kafkax.TopicPlanAction) kafkax.TopicPlanAction {
	if current == kafkax.TopicPlanNoop {
		return next
	}
	return current
}
