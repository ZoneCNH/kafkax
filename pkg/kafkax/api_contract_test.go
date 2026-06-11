package kafkax

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

var (
	_ Producer = (*fakeProducer)(nil)
	_ Consumer = (*fakeConsumer)(nil)
	_ Admin    = (*fakeAdmin)(nil)
)

func TestMessageCloneCopiesMutableFields(t *testing.T) {
	message := Message{
		Topic: "orders",
		Key:   []byte("key"),
		Value: []byte("value"),
		Headers: []Header{
			{Key: "trace", Value: []byte("abc")},
		},
	}

	cloned := message.Clone()
	message.Key[0] = 'K'
	message.Value[0] = 'V'
	message.Headers[0].Value[0] = 'x'

	if string(cloned.Key) != "key" {
		t.Fatalf("expected cloned key to be isolated, got %q", cloned.Key)
	}
	if string(cloned.Value) != "value" {
		t.Fatalf("expected cloned value to be isolated, got %q", cloned.Value)
	}
	if string(cloned.Headers[0].Value) != "abc" {
		t.Fatalf("expected cloned header to be isolated, got %q", cloned.Headers[0].Value)
	}
}

func TestTopicContractsCloneConfigMaps(t *testing.T) {
	spec := TopicSpec{Name: "orders", Partitions: 3, ReplicationFactor: 2, Config: map[string]string{"retention.ms": "60000"}}
	clonedSpec := spec.Clone()
	spec.Config["retention.ms"] = "1"
	if clonedSpec.Config["retention.ms"] != "60000" {
		t.Fatalf("expected cloned topic spec config to be isolated, got %#v", clonedSpec.Config)
	}

	description := TopicDescription{Name: "orders", Partitions: 3, ReplicationFactor: 2, Config: map[string]string{"cleanup.policy": "delete"}}
	clonedDescription := description.Clone()
	description.Config["cleanup.policy"] = "compact"
	if clonedDescription.Config["cleanup.policy"] != "delete" {
		t.Fatalf("expected cloned topic description config to be isolated, got %#v", clonedDescription.Config)
	}
}

func TestBatchProduceResultCloneCopiesResults(t *testing.T) {
	result := BatchProduceResult{
		Results: []ProduceResult{{Topic: "orders", Partition: 1, Offset: 42}},
	}

	cloned := result.Clone()
	result.Results[0].Topic = "payments"
	result.Results[0].Offset = 99

	if cloned.Results[0].Topic != "orders" {
		t.Fatalf("expected cloned result topic to be isolated, got %q", cloned.Results[0].Topic)
	}
	if cloned.Results[0].Offset != 42 {
		t.Fatalf("expected cloned result offset to be isolated, got %d", cloned.Results[0].Offset)
	}
}

func TestSubscriptionCloneCopiesTopics(t *testing.T) {
	subscription := Subscription{Topics: []string{"orders"}, GroupID: "workers", StartOffset: OffsetResetEarliest}

	cloned := subscription.Clone()
	subscription.Topics[0] = "payments"

	if cloned.Topics[0] != "orders" {
		t.Fatalf("expected cloned subscription topics to be isolated, got %#v", cloned.Topics)
	}
	if cloned.GroupID != "workers" || cloned.StartOffset != OffsetResetEarliest {
		t.Fatalf("expected cloned subscription metadata to be preserved, got %#v", cloned)
	}
}

func TestRecordContractsCloneMutableMessageFields(t *testing.T) {
	record := Record{
		Message: Message{
			Topic: "orders",
			Key:   []byte("key"),
			Value: []byte("value"),
			Headers: []Header{
				{Key: "trace", Value: []byte("abc")},
			},
		},
		GroupID: "workers",
	}
	batch := RecordBatch{Records: []Record{record}}

	clonedRecord := record.Clone()
	clonedBatch := batch.Clone()
	record.Key[0] = 'K'
	record.Value[0] = 'V'
	record.Headers[0].Value[0] = 'x'
	batch.Records[0].Key[0] = 'B'
	batch.Records[0].Headers[0].Value[0] = 'y'

	if string(clonedRecord.Key) != "key" || string(clonedRecord.Value) != "value" || string(clonedRecord.Headers[0].Value) != "abc" {
		t.Fatalf("expected cloned record message fields to be isolated, got %#v", clonedRecord)
	}
	if clonedRecord.GroupID != "workers" {
		t.Fatalf("expected cloned record group to be preserved, got %q", clonedRecord.GroupID)
	}
	if string(clonedBatch.Records[0].Key) != "key" || string(clonedBatch.Records[0].Headers[0].Value) != "abc" {
		t.Fatalf("expected cloned record batch fields to be isolated, got %#v", clonedBatch)
	}
}

func TestTopicPlanCloneCopiesPluralSpecs(t *testing.T) {
	plan := TopicPlan{
		Action: TopicPlanCreate,
		Spec:   TopicSpec{Name: "legacy", Config: map[string]string{"cleanup.policy": "delete"}},
		Specs: []TopicSpec{
			{Name: "orders", Config: map[string]string{"retention.ms": "60000"}},
			{Name: "payments", Config: map[string]string{"segment.bytes": "1024"}},
		},
		Changes: []TopicChange{{Field: "partitions", From: "1", To: "3"}},
	}

	cloned := plan.Clone()
	plan.Spec.Config["cleanup.policy"] = "compact"
	plan.Specs[0].Config["retention.ms"] = "1"
	plan.Changes[0].To = "6"

	if cloned.Spec.Config["cleanup.policy"] != "delete" {
		t.Fatalf("expected cloned singular spec config to be isolated, got %#v", cloned.Spec.Config)
	}
	if cloned.Specs[0].Config["retention.ms"] != "60000" {
		t.Fatalf("expected cloned plural spec config to be isolated, got %#v", cloned.Specs[0].Config)
	}
	if cloned.Changes[0].To != "3" {
		t.Fatalf("expected cloned changes to be isolated, got %#v", cloned.Changes)
	}
}

func TestTopicApplyResultCloneCopiesDescriptions(t *testing.T) {
	result := TopicApplyResult{
		Applied: []TopicDescription{{Name: "orders", Config: map[string]string{"cleanup.policy": "delete"}}},
		DryRun:  true,
	}

	cloned := result.Clone()
	result.Applied[0].Config["cleanup.policy"] = "compact"

	if cloned.Applied[0].Config["cleanup.policy"] != "delete" {
		t.Fatalf("expected cloned applied descriptions to be isolated, got %#v", cloned.Applied)
	}
	if !cloned.DryRun {
		t.Fatal("expected cloned dry-run flag to be preserved")
	}
}

func TestConfigSanitizeMasksKafkaSecrets(t *testing.T) {
	sanitized := Config{
		Name:    "kafkax",
		Brokers: []string{"localhost:9092"},
		Security: SecurityConfig{
			Protocol: SecurityProtocolSASL,
			Username: "app",
			Password: "plain-password",
			Token:    "plain-token",
		},
	}.Sanitize()

	if sanitized.Security.Password != "***" {
		t.Fatalf("expected password to be masked, got %q", sanitized.Security.Password)
	}
	if sanitized.Security.Token != "***" {
		t.Fatalf("expected token to be masked, got %q", sanitized.Security.Token)
	}
	if sanitized.Security.Username != "app" {
		t.Fatalf("expected username to be preserved, got %q", sanitized.Security.Username)
	}
	if sanitized.Brokers[0] != "localhost:9092" {
		t.Fatalf("expected broker to be preserved, got %#v", sanitized.Brokers)
	}
}

func TestConfigValidateRejectsNegativeKafkaDurations(t *testing.T) {
	for name, config := range map[string]Config{
		"consumer session timeout": {Name: "kafkax", Consumer: ConsumerConfig{SessionTimeout: -1}},
		"admin timeout":            {Name: "kafkax", Admin: AdminConfig{Timeout: -1}},
		"retry backoff":            {Name: "kafkax", Retry: RetryConfig{Backoff: -1}},
		"retry attempts":           {Name: "kafkax", Retry: RetryConfig{MaxAttempts: -1}},
		"producer required acks":   {Name: "kafkax", Producer: ProducerConfig{RequiredAcks: -1}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := config.Validate(); !IsKind(err, ErrorKindValidation) {
				t.Fatalf("expected validation error, got %T %[1]v", err)
			}
		})
	}
}

func TestPublicKafkaAPIExposesNoThirdPartyConcreteTypes(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf((*Producer)(nil)).Elem(),
		reflect.TypeOf((*Consumer)(nil)).Elem(),
		reflect.TypeOf((*Admin)(nil)).Elem(),
		reflect.TypeOf(Message{}),
		reflect.TypeOf(ProduceResult{}),
		reflect.TypeOf(BatchProduceResult{}),
		reflect.TypeOf(Subscription{}),
		reflect.TypeOf(TopicPartition{}),
		reflect.TypeOf(Record{}),
		reflect.TypeOf(RecordBatch{}),
		reflect.TypeOf(TopicSpec{}),
		reflect.TypeOf(TopicDescription{}),
		reflect.TypeOf(TopicPlan{}),
		reflect.TypeOf(TopicApplyResult{}),
		reflect.TypeOf(Config{}),
	} {
		assertNoThirdPartyKafkaType(t, typ, map[reflect.Type]bool{})
	}
}

type fakeProducer struct{}

func (*fakeProducer) Send(context.Context, Message, ...ProduceOption) (ProduceResult, error) {
	return ProduceResult{}, nil
}

func (*fakeProducer) SendBatch(context.Context, []Message, ...ProduceOption) (BatchProduceResult, error) {
	return BatchProduceResult{}, nil
}

func (*fakeProducer) Flush(context.Context) error {
	return nil
}

func (*fakeProducer) Close(context.Context) error {
	return nil
}

type fakeConsumer struct{}

func (*fakeConsumer) Run(context.Context, Handler) error {
	return nil
}

func (*fakeConsumer) Poll(context.Context) (RecordBatch, error) {
	return RecordBatch{}, nil
}

func (*fakeConsumer) Commit(context.Context, ...Offset) error {
	return nil
}

func (*fakeConsumer) Pause(context.Context, ...TopicPartition) error {
	return nil
}

func (*fakeConsumer) Resume(context.Context, ...TopicPartition) error {
	return nil
}

func (*fakeConsumer) Close(context.Context) error {
	return nil
}

type fakeAdmin struct{}

func (*fakeAdmin) DescribeTopics(context.Context, ...string) ([]TopicDescription, error) {
	return nil, nil
}

func (*fakeAdmin) PlanTopics(context.Context, ...TopicSpec) (TopicPlan, error) {
	return TopicPlan{}, nil
}

func (*fakeAdmin) ApplyTopics(context.Context, TopicPlan) (TopicApplyResult, error) {
	return TopicApplyResult{}, nil
}

func (*fakeAdmin) Close(context.Context) error {
	return nil
}

func assertNoThirdPartyKafkaType(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	if typ == nil || seen[typ] {
		return
	}
	seen[typ] = true

	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Chan {
		typ = typ.Elem()
	}
	pkgPath := typ.PkgPath()
	for _, forbidden := range []string{"segmentio/kafka-go", "confluentinc", "twmb/franz-go", "/kgo"} {
		if strings.Contains(pkgPath, forbidden) {
			t.Fatalf("public API exposes third-party Kafka type %s from %s", typ, pkgPath)
		}
	}

	switch typ.Kind() {
	case reflect.Interface:
		for i := 0; i < typ.NumMethod(); i++ {
			method := typ.Method(i)
			assertNoThirdPartyKafkaType(t, method.Type, seen)
		}
	case reflect.Func:
		for i := 0; i < typ.NumIn(); i++ {
			assertNoThirdPartyKafkaType(t, typ.In(i), seen)
		}
		for i := 0; i < typ.NumOut(); i++ {
			assertNoThirdPartyKafkaType(t, typ.Out(i), seen)
		}
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			assertNoThirdPartyKafkaType(t, typ.Field(i).Type, seen)
		}
	case reflect.Map:
		assertNoThirdPartyKafkaType(t, typ.Key(), seen)
		assertNoThirdPartyKafkaType(t, typ.Elem(), seen)
	}
}
