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
		reflect.TypeOf(TopicSpec{}),
		reflect.TypeOf(Config{}),
	} {
		assertNoThirdPartyKafkaType(t, typ, map[reflect.Type]bool{})
	}
}

type fakeProducer struct{}

func (*fakeProducer) Produce(context.Context, Message, ...ProduceOption) (ProduceResult, error) {
	return ProduceResult{}, nil
}

func (*fakeProducer) Close(context.Context) error {
	return nil
}

type fakeConsumer struct{}

func (*fakeConsumer) Subscribe(context.Context, Subscription) error {
	return nil
}

func (*fakeConsumer) Receive(context.Context) (ConsumerMessage, error) {
	return ConsumerMessage{}, nil
}

func (*fakeConsumer) Commit(context.Context, ConsumerMessage) error {
	return nil
}

func (*fakeConsumer) Close(context.Context) error {
	return nil
}

type fakeAdmin struct{}

func (*fakeAdmin) DescribeTopic(context.Context, string) (TopicDescription, error) {
	return TopicDescription{}, nil
}

func (*fakeAdmin) PlanTopic(context.Context, TopicSpec) (TopicPlan, error) {
	return TopicPlan{}, nil
}

func (*fakeAdmin) ApplyTopic(context.Context, TopicPlan) error {
	return nil
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
