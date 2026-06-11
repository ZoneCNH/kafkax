package testkit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
)

func TestKafkaFakesRoundTripGoldenRecord(t *testing.T) {
	ctx := context.Background()
	fakeKafka := FakeKafka()
	producer, err := fakeKafka.Producer(ctx)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	consumer, err := fakeKafka.Consumer(ctx)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	record := GoldenRecord("orders")
	if _, err := producer.Send(ctx, record); err != nil {
		t.Fatalf("send: %v", err)
	}
	batch, err := consumer.Poll(ctx)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("expected one record, got %#v", batch)
	}
	if got := string(batch.Records[0].Value); got != "golden-value" {
		t.Fatalf("unexpected record value: %q", got)
	}
}

func TestKafkaFakeProducerClonesRecords(t *testing.T) {
	ctx := context.Background()
	fakeKafka := FakeKafka()
	producer, err := fakeKafka.Producer(ctx)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	message := kafkax.Message{Topic: "orders", Key: []byte("key"), Value: []byte("value")}
	if _, err := producer.SendBatch(ctx, []kafkax.Message{message}); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	message.Value[0] = 'X'

	consumer, err := fakeKafka.Consumer(ctx)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	batch, err := consumer.Poll(ctx)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if got := string(batch.Records[0].Value); got != "value" {
		t.Fatalf("expected cloned record value, got %q", got)
	}
}

func TestKafkaFakeProducerHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	producer, err := FakeProducer(context.Background())
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	if _, err := producer.Send(ctx, kafkax.Message{Topic: "orders"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %T %[1]v", err)
	}
}

func TestKafkaFakeAdminPlansWithoutMutatingTopics(t *testing.T) {
	ctx := context.Background()
	admin, err := FakeAdmin(ctx)
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}

	spec := kafkax.TopicSpec{
		Name:              "orders",
		Partitions:        3,
		ReplicationFactor: 2,
		Retention:         time.Hour,
		CleanupPolicy:     kafkax.CleanupPolicyCompact,
		Compression:       kafkax.CompressionZSTD,
		MinInSyncReplicas: 2,
		Config:            map[string]string{"segment.bytes": "1048576"},
	}
	plan, err := admin.PlanTopics(ctx, spec)
	if err != nil {
		t.Fatalf("plan topics: %v", err)
	}
	if plan.Action != kafkax.TopicPlanCreate || len(plan.Changes) != 1 {
		t.Fatalf("unexpected plan: %#v", plan)
	}
	descriptions, err := admin.DescribeTopics(ctx, "orders")
	if err != nil {
		t.Fatalf("describe topics: %v", err)
	}
	if len(descriptions) != 0 {
		t.Fatalf("PlanTopics must not mutate topics, got %#v", descriptions)
	}

	result, err := admin.ApplyTopics(ctx, plan)
	if err != nil {
		t.Fatalf("apply topics: %v", err)
	}
	if len(result.Applied) != 1 || result.Applied[0].Name != "orders" {
		t.Fatalf("unexpected apply result: %#v", result)
	}

	descriptions, err = admin.DescribeTopics(ctx, "orders")
	if err != nil {
		t.Fatalf("describe topics after apply: %v", err)
	}
	if len(descriptions) != 1 {
		t.Fatalf("expected one topic description, got %#v", descriptions)
	}
	description := descriptions[0]
	if description.Partitions != spec.Partitions ||
		description.ReplicationFactor != spec.ReplicationFactor ||
		description.Retention != spec.Retention ||
		description.CleanupPolicy != spec.CleanupPolicy ||
		description.Compression != spec.Compression ||
		description.MinInSyncReplicas != spec.MinInSyncReplicas ||
		description.Config["segment.bytes"] != "1048576" {
		t.Fatalf("topic description drift:\nactual:   %#v\nexpected: %#v", description, spec)
	}

	updated := spec.Clone()
	updated.Partitions = 5
	updated.Retention = 2 * time.Hour
	updated.CleanupPolicy = kafkax.CleanupPolicyCompactDelete
	updated.Compression = kafkax.CompressionLZ4
	updated.MinInSyncReplicas = 3
	updated.Config["segment.bytes"] = "2097152"

	updatePlan, err := admin.PlanTopics(ctx, updated)
	if err != nil {
		t.Fatalf("plan topic update: %v", err)
	}
	if updatePlan.Action != kafkax.TopicPlanUpdate {
		t.Fatalf("expected update plan, got %#v", updatePlan)
	}
	expectedChanges := map[string]bool{
		"orders.partitions":           false,
		"orders.retention":            false,
		"orders.cleanup_policy":       false,
		"orders.compression":          false,
		"orders.min_in_sync_replicas": false,
		"orders.config":               false,
	}
	for _, change := range updatePlan.Changes {
		if _, ok := expectedChanges[change.Field]; ok {
			expectedChanges[change.Field] = true
		}
	}
	for field, seen := range expectedChanges {
		if !seen {
			t.Fatalf("missing update change %q in %#v", field, updatePlan.Changes)
		}
	}
}
