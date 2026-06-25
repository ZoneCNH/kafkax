package kafkago

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestRequiredAcksDefaultsToAll(t *testing.T) {
	if got := requiredAcks(0); got != kafka.RequireAll {
		t.Fatalf("requiredAcks(0) = %v, want kafka.RequireAll", got)
	}
}

func TestRequiredAcksSupportsExplicitAll(t *testing.T) {
	if got := requiredAcks(-1); got != kafka.RequireAll {
		t.Fatalf("requiredAcks(-1) = %v, want kafka.RequireAll", got)
	}
}

func TestRequiredAcksSupportsLeaderOnly(t *testing.T) {
	if got := requiredAcks(1); got != kafka.RequireOne {
		t.Fatalf("requiredAcks(1) = %v, want kafka.RequireOne", got)
	}
}
