package kafkax

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewRejectsInvalidConfig(t *testing.T) {
	metrics := &recordingMetrics{}

	_, err := New(context.Background(), Config{Timeout: time.Second}, WithMetrics(metrics))
	if err == nil {
		t.Fatal("expected invalid config to fail")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
	if !metrics.counterWithLabel(MetricClientErrorsTotal, "kind", string(ErrorKindValidation)) {
		t.Fatalf("expected validation error metric, got %#v", metrics.counters)
	}
}

func TestNewRejectsNilContext(t *testing.T) {
	metrics := &recordingMetrics{}

	_, err := New(nil, Config{Name: "kafkax"}, WithMetrics(metrics)) //nolint:staticcheck // verifies the defensive nil-context branch.
	if err == nil {
		t.Fatal("expected nil context to fail")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
	if !metrics.counterWithLabel(MetricClientErrorsTotal, "kind", string(ErrorKindValidation)) {
		t.Fatalf("expected validation error metric, got %#v", metrics.counters)
	}
}

func TestNewRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New(ctx, Config{Name: "kafkax"})
	if err == nil {
		t.Fatal("expected canceled context to fail")
	}
	if !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("expected unavailable error, got %T %[1]v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled cause, got %v", err)
	}
}

func TestNewRejectsExpiredContext(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := New(ctx, Config{Name: "kafkax"})
	if err == nil {
		t.Fatal("expected expired context to fail")
	}
	if !IsKind(err, ErrorKindTimeout) {
		t.Fatalf("expected timeout error, got %T %[1]v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline cause, got %v", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	metrics := &recordingMetrics{}
	client, err := New(context.Background(), Config{Name: "kafkax"}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if !metrics.hasCounter(MetricClientCreatedTotal) {
		t.Fatalf("expected client creation metric, got %#v", metrics.counters)
	}

	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if !metrics.hasCounter(MetricClientClosedTotal) {
		t.Fatalf("expected client close metric, got %#v", metrics.counters)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestClientExposesInjectedKafkaDrivers(t *testing.T) {
	producer := &fakeProducer{}
	consumer := &fakeConsumer{}
	admin := &fakeAdmin{}
	client, err := New(
		context.Background(),
		Config{Name: "kafkax", Consumer: ConsumerConfig{GroupID: "workers", StartOffset: OffsetResetEarliest}},
		WithProducer(producer),
		WithConsumer(consumer),
		WithAdmin(admin),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	gotProducer, err := client.Producer()
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	if gotProducer != producer {
		t.Fatalf("expected injected producer, got %#v", gotProducer)
	}
	gotConsumer, err := client.Consumer("", "orders")
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	if gotConsumer != consumer {
		t.Fatalf("expected injected consumer, got %#v", gotConsumer)
	}
	gotAdmin, err := client.Admin()
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	if gotAdmin != admin {
		t.Fatalf("expected injected admin, got %#v", gotAdmin)
	}
}

func TestClientConsumerFactoryReceivesClonedSubscription(t *testing.T) {
	var captured Subscription
	client, err := New(
		context.Background(),
		Config{Name: "kafkax", Consumer: ConsumerConfig{GroupID: "workers", StartOffset: OffsetResetLatest}},
		WithConsumerFactory(func(subscription Subscription) (Consumer, error) {
			captured = subscription.Clone()
			subscription.Topics[0] = "mutated"
			return &fakeConsumer{}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if _, err := client.Consumer("", "orders"); err != nil {
		t.Fatalf("consumer: %v", err)
	}
	if captured.GroupID != "workers" || captured.StartOffset != OffsetResetLatest {
		t.Fatalf("expected config defaults to be applied, got %#v", captured)
	}
	if got := captured.Topics[0]; got != "orders" {
		t.Fatalf("expected cloned topic to preserve caller value, got %q", got)
	}
}

func TestClientDriversRejectMissingOrClosedState(t *testing.T) {
	client, err := New(context.Background(), Config{Name: "kafkax"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if _, err := client.Producer(); !IsKind(err, ErrorKindDriver) {
		t.Fatalf("expected missing producer driver error, got %T %[1]v", err)
	}
	if _, err := client.Consumer("workers", "orders"); !IsKind(err, ErrorKindDriver) {
		t.Fatalf("expected missing consumer driver error, got %T %[1]v", err)
	}
	if _, err := client.Admin(); !IsKind(err, ErrorKindDriver) {
		t.Fatalf("expected missing admin driver error, got %T %[1]v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := client.Producer(); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected closed producer validation error, got %T %[1]v", err)
	}
}

func TestCloseRejectsNilClient(t *testing.T) {
	var client *Client

	err := client.Close(context.Background())
	if err == nil {
		t.Fatal("expected nil client to fail")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
}

func TestCloseRejectsNilContext(t *testing.T) {
	metrics := &recordingMetrics{}
	client, err := New(context.Background(), Config{Name: "kafkax"}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	err = client.Close(nil) //nolint:staticcheck // verifies the defensive nil-context branch.
	if err == nil {
		t.Fatal("expected nil close context to fail")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
	if !metrics.counterWithLabel(MetricClientErrorsTotal, "kind", string(ErrorKindValidation)) {
		t.Fatalf("expected validation error metric, got %#v", metrics.counters)
	}
}

func TestCloseRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := New(context.Background(), Config{Name: "kafkax"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	err = client.Close(ctx)
	if err == nil {
		t.Fatal("expected canceled close context to fail")
	}
	if !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("expected unavailable error, got %T %[1]v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled cause, got %v", err)
	}
}

func TestCloseRejectsExpiredContext(t *testing.T) {
	metrics := &recordingMetrics{}
	client, err := New(context.Background(), Config{Name: "kafkax"}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err = client.Close(ctx)
	if err == nil {
		t.Fatal("expected expired close context to fail")
	}
	if !IsKind(err, ErrorKindTimeout) {
		t.Fatalf("expected timeout error, got %T %[1]v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline cause, got %v", err)
	}
	if !metrics.counterWithLabel(MetricClientErrorsTotal, "kind", string(ErrorKindTimeout)) {
		t.Fatalf("expected timeout error metric, got %#v", metrics.counters)
	}
}

func TestCloseRejectsZeroValueClient(t *testing.T) {
	var client Client

	err := client.Close(context.Background())
	if err == nil {
		t.Fatal("expected zero-value client to fail")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
}
