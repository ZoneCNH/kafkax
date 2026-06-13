package kafkago

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	defaultTimeout = 10 * time.Second
	defaultClient  = "kafkax"
)

// Option configures the kafka-go production driver.
type Option func(*options)

type options struct {
	metrics kafkax.Metrics
	saslTLS bool
}

// WithMetrics records driver operations through the public metrics interface.
func WithMetrics(metrics kafkax.Metrics) Option {
	return func(o *options) {
		if metrics != nil {
			o.metrics = metrics
		}
	}
}

// WithSASLTLS enables TLS transport for SASL_SSL/SASL_TLS fixtures while keeping
// the public kafkax security protocol driver-neutral.
func WithSASLTLS(enabled bool) Option {
	return func(o *options) {
		o.saslTLS = enabled
	}
}

// Driver adapts github.com/segmentio/kafka-go to the driver-neutral public kafkax interfaces.
type Driver struct {
	cfg       kafkax.Config
	metrics   kafkax.Metrics
	dialer    *kafka.Dialer
	transport *kafka.Transport

	mu        sync.Mutex
	producer  *producer
	admin     *admin
	consumers []*consumer
}

// New creates a production Kafka driver without leaking concrete kafka-go types publicly.
func New(cfg kafkax.Config, opts ...Option) (*Driver, error) {
	const op = "kafkago.New"
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.ClientID == "" {
		cfg.ClientID = cfg.Name
	}
	if cfg.ClientID == "" {
		cfg.ClientID = defaultClient
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(cfg.Brokers) == 0 {
		return nil, kafkax.NewError(kafkax.ErrorKindConfig, op, "at least one broker is required", false)
	}

	settings := options{metrics: kafkax.NoopMetrics{}}
	for _, opt := range opts {
		opt(&settings)
	}

	var mechanism sasl.Mechanism
	if cfg.Security.Protocol == kafkax.SecurityProtocolSASL {
		var err error
		mechanism, err = saslMechanism(cfg.Security)
		if err != nil {
			return nil, err
		}
	}
	tlsConfig := tlsConfigFor(cfg.Security, settings)
	dialer := &kafka.Dialer{
		Timeout:       cfg.Timeout,
		ClientID:      cfg.ClientID,
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}
	transport := &kafka.Transport{
		DialTimeout: cfg.Timeout,
		IdleTimeout: 30 * time.Second,
		ClientID:    cfg.ClientID,
		TLS:         tlsConfig,
		SASL:        mechanism,
	}
	d := &Driver{cfg: cfg, metrics: settings.metrics, dialer: dialer, transport: transport}
	d.producer = newProducer(d)
	d.admin = newAdmin(d)
	return d, nil
}

// ClientOptions returns injected driver-neutral options for pkg/kafkax.New.
func (d *Driver) ClientOptions() []kafkax.Option {
	if d == nil {
		return nil
	}
	return []kafkax.Option{
		kafkax.WithMetrics(d.metrics),
		kafkax.WithProducer(d.Producer()),
		kafkax.WithConsumerFactory(d.Consumer),
		kafkax.WithAdmin(d.Admin()),
	}
}

func (d *Driver) Producer() kafkax.Producer { return d.producer }
func (d *Driver) Admin() kafkax.Admin       { return d.admin }

func (d *Driver) Consumer(sub kafkax.Subscription) (kafkax.Consumer, error) {
	const op = "kafkago.Driver.Consumer"
	if d == nil {
		return nil, kafkax.NewError(kafkax.ErrorKindDriver, op, "driver is nil", false)
	}
	if len(sub.Topics) == 0 {
		return nil, kafkax.NewError(kafkax.ErrorKindConfig, op, "at least one topic is required", false)
	}
	if sub.GroupID == "" {
		sub.GroupID = d.cfg.Consumer.GroupID
	}
	if sub.GroupID == "" {
		return nil, kafkax.NewError(kafkax.ErrorKindConfig, op, "consumer group is required", false)
	}
	if sub.StartOffset == "" {
		sub.StartOffset = d.cfg.Consumer.StartOffset
	}
	c := newConsumer(d, sub.Clone())
	d.mu.Lock()
	d.consumers = append(d.consumers, c)
	d.mu.Unlock()
	return c, nil
}

func (d *Driver) Close(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.Lock()
	producer := d.producer
	admin := d.admin
	consumers := append([]*consumer(nil), d.consumers...)
	d.mu.Unlock()
	var errs []error
	if producer != nil {
		errs = append(errs, producer.Close(ctx))
	}
	for _, c := range consumers {
		errs = append(errs, c.Close(ctx))
	}
	if admin != nil {
		errs = append(errs, admin.Close(ctx))
	}
	if d.transport != nil {
		d.transport.CloseIdleConnections()
	}
	return errors.Join(errs...)
}

func saslMechanism(sec kafkax.SecurityConfig) (sasl.Mechanism, error) {
	if sec.Username == "" || sec.Password == "" {
		return nil, kafkax.NewError(kafkax.ErrorKindConfig, "kafkago.saslMechanism", "SASL username and password are required", false)
	}
	return plain.Mechanism{Username: sec.Username, Password: sec.Password}, nil
}

func tlsConfigFor(sec kafkax.SecurityConfig, settings options) *tls.Config {
	if sec.Protocol == kafkax.SecurityProtocolTLS || (sec.Protocol == kafkax.SecurityProtocolSASL && settings.saslTLS) {
		return &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return nil
}

func (d *Driver) timeoutContext(ctx context.Context, fallback time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	if fallback <= 0 {
		fallback = d.cfg.Timeout
	}
	if fallback <= 0 {
		fallback = defaultTimeout
	}
	return context.WithTimeout(ctx, fallback)
}

func (d *Driver) dialBroker(ctx context.Context) (*kafka.Conn, error) {
	const op = "kafkago.dialBroker"
	if d == nil || d.dialer == nil || len(d.cfg.Brokers) == 0 {
		return nil, kafkax.NewError(kafkax.ErrorKindConnection, op, "broker configuration is missing", true)
	}
	conn, err := d.dialer.DialContext(ctx, "tcp", d.cfg.Brokers[0])
	if err != nil {
		return nil, kafkax.WrapError(kafkax.ErrorKindConnection, op, "connect to broker", true, err)
	}
	return conn, nil
}

func (d *Driver) dialController(ctx context.Context) (*kafka.Conn, error) {
	broker, err := d.dialBroker(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = broker.Close() }()
	controller, err := broker.Controller()
	if err != nil {
		return nil, kafkax.WrapError(kafkax.ErrorKindAdmin, "kafkago.dialController", "discover controller", true, err)
	}
	address := net.JoinHostPort(controller.Host, fmt.Sprint(controller.Port))
	conn, err := d.dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, kafkax.WrapError(kafkax.ErrorKindConnection, "kafkago.dialController", "connect to controller", true, err)
	}
	return conn, nil
}

func inc(metrics kafkax.Metrics, name string, labels map[string]string) {
	if metrics != nil {
		metrics.IncCounter(name, labels)
	}
}

func observe(metrics kafkax.Metrics, name string, value float64, labels map[string]string) {
	if metrics != nil {
		metrics.ObserveHistogram(name, value, labels)
	}
}

func wrapContextOr(kind kafkax.ErrorKind, op, message string, retryable bool, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return kafkax.WrapError(kafkax.ErrorKindTimeout, op, message, true, err)
	}
	if errors.Is(err, context.Canceled) {
		return kafkax.WrapError(kafkax.ErrorKindUnavailable, op, message, false, err)
	}
	return kafkax.WrapError(kind, op, message, retryable, err)
}

func normalizeTopic(topic string) string { return strings.TrimSpace(topic) }
