package kafkax

import (
	"testing"
	"time"
)

func TestConfigValidateRequiresName(t *testing.T) {
	err := Config{Timeout: time.Second}.Validate()
	if err == nil {
		t.Fatal("expected missing name to fail validation")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
}

func TestConfigValidateRejectsNegativeTimeout(t *testing.T) {
	err := Config{Name: "kafkax", Timeout: -time.Second}.Validate()
	if err == nil {
		t.Fatal("expected negative timeout to fail validation")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
}

func TestConfigSanitizeMasksSecret(t *testing.T) {
	sanitized := Config{Name: "kafkax", Timeout: time.Second, Secret: "plain-text"}.Sanitize()
	if sanitized.Secret != "***" {
		t.Fatalf("expected masked secret, got %q", sanitized.Secret)
	}
	if sanitized.Name != "kafkax" {
		t.Fatalf("expected name to be preserved, got %q", sanitized.Name)
	}
}

func TestConfigValidateRejectsNegativeHeartbeatInterval(t *testing.T) {
	err := Config{Name: "kafkax", Consumer: ConsumerConfig{HeartbeatInterval: -time.Second}}.Validate()
	if err == nil {
		t.Fatal("expected negative heartbeat interval to fail validation")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
}

func TestConfigValidateRejectsNegativeMaxPollRecords(t *testing.T) {
	err := Config{Name: "kafkax", Consumer: ConsumerConfig{MaxPollRecords: -1}}.Validate()
	if err == nil {
		t.Fatal("expected negative max poll records to fail validation")
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("expected validation error, got %T %[1]v", err)
	}
}

func TestConfigValidateAllowsAllInSyncReplicaAcks(t *testing.T) {
	if err := (Config{Name: "kafkax", Producer: ProducerConfig{RequiredAcks: -1}}).Validate(); err != nil {
		t.Fatalf("RequiredAcks=-1 should be valid for all in-sync replicas: %v", err)
	}
}
