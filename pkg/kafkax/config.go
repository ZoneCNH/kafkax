package kafkax

import (
	"errors"
	"time"

	"github.com/ZoneCNH/kafkax/internal/sanitize"
	"github.com/ZoneCNH/kafkax/internal/validation"
)

type Config struct {
	Name          string
	Timeout       time.Duration
	Secret        string
	Brokers       []string
	ClientID      string
	Security      SecurityConfig
	Producer      ProducerConfig
	Consumer      ConsumerConfig
	Admin         AdminConfig
	Retry         RetryConfig
	Observability ObservabilityConfig
}

type SanitizedConfig struct {
	Name          string
	Timeout       time.Duration
	Secret        string
	Brokers       []string
	ClientID      string
	Security      SecurityConfig
	Producer      ProducerConfig
	Consumer      ConsumerConfig
	Admin         AdminConfig
	Retry         RetryConfig
	Observability ObservabilityConfig
}

func (c Config) Validate() error {
	if err := validation.RequireNonEmpty("name", c.Name); err != nil {
		return validationError("Config.Validate", err.Error(), err)
	}
	if c.Timeout < 0 {
		err := errors.New("timeout must not be negative")
		return validationError("Config.Validate", err.Error(), err)
	}
	if c.Producer.RequiredAcks < -1 {
		err := errors.New("producer required acks must be -1, 0, or 1")
		return validationError("Config.Validate", err.Error(), err)
	}
	if c.Consumer.SessionTimeout < 0 {
		err := errors.New("consumer session timeout must not be negative")
		return validationError("Config.Validate", err.Error(), err)
	}
	if c.Consumer.HeartbeatInterval < 0 {
		err := errors.New("consumer heartbeat interval must not be negative")
		return validationError("Config.Validate", err.Error(), err)
	}
	if c.Consumer.MaxPollRecords < 0 {
		err := errors.New("consumer max poll records must not be negative")
		return validationError("Config.Validate", err.Error(), err)
	}
	if c.Admin.Timeout < 0 {
		err := errors.New("admin timeout must not be negative")
		return validationError("Config.Validate", err.Error(), err)
	}
	if c.Retry.MaxAttempts < 0 {
		err := errors.New("retry max attempts must not be negative")
		return validationError("Config.Validate", err.Error(), err)
	}
	if c.Retry.Backoff < 0 {
		err := errors.New("retry backoff must not be negative")
		return validationError("Config.Validate", err.Error(), err)
	}
	return nil
}

func (c Config) Sanitize() SanitizedConfig {
	return SanitizedConfig{
		Name:          c.Name,
		Timeout:       c.Timeout,
		Secret:        sanitize.Secret(c.Secret),
		Brokers:       append([]string(nil), c.Brokers...),
		ClientID:      c.ClientID,
		Security:      c.Security.sanitized(),
		Producer:      c.Producer,
		Consumer:      c.Consumer,
		Admin:         c.Admin,
		Retry:         c.Retry,
		Observability: c.Observability,
	}
}

type SecurityProtocol string

const (
	SecurityProtocolPlaintext SecurityProtocol = "plaintext"
	SecurityProtocolTLS       SecurityProtocol = "tls"
	SecurityProtocolSASL      SecurityProtocol = "sasl"
)

type SecurityConfig struct {
	Protocol SecurityProtocol
	Mechanism string
	Username string
	Password string
	Token    string
}

func (c SecurityConfig) sanitized() SecurityConfig {
	c.Password = sanitize.Secret(c.Password)
	c.Token = sanitize.Secret(c.Token)
	return c
}

type ProducerConfig struct {
	RequiredAcks int
	Idempotent   bool
	BatchBytes   int
}

type ConsumerConfig struct {
	GroupID           string
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	MaxPollRecords    int
	StartOffset       OffsetResetPolicy
}

type AdminConfig struct {
	Timeout time.Duration
	DryRun  bool
}

type RetryConfig struct {
	MaxAttempts int
	Backoff     time.Duration
}

type ObservabilityConfig struct {
	MetricsNamespace string
	HealthTopic      string
}
