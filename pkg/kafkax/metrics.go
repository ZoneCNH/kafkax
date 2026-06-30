package kafkax

import obs "github.com/ZoneCNH/observex/pkg/observex"

// MetricClient* constants are aliases to observex shared constants so all
// foundation x-modules report identical cross-component metric names.
const (
	MetricClientCreatedTotal           = obs.MetricClientCreatedTotal
	MetricClientClosedTotal            = obs.MetricClientClosedTotal
	MetricClientErrorsTotal            = obs.MetricClientErrorsTotal
	MetricClientHealthStatus           = obs.MetricClientHealthStatus
	MetricClientHealthLatencyMS        = obs.MetricClientHealthLatencyMS
	MetricClientRequestsTotal          = obs.MetricClientRequestsTotal
	MetricClientRequestDurationSeconds = obs.MetricClientRequestDurationSeconds
	MetricClientRetriesTotal           = obs.MetricClientRetriesTotal
	MetricClientInflight               = obs.MetricClientInflight

	// kafkax-specific metric names.
	MetricProducerMessagesTotal  = "producer_messages_total"
	MetricProducerErrorsTotal    = "producer_errors_total"
	MetricProducerLatencySeconds = "producer_latency_seconds"
	MetricConsumerMessagesTotal  = "consumer_messages_total"
	MetricConsumerErrorsTotal    = "consumer_errors_total"
	MetricConsumerLag            = "consumer_lag"
	MetricConsumerCommitsTotal   = "consumer_commits_total"
	MetricAdminOperationsTotal   = "admin_operations_total"
	MetricAdminErrorsTotal       = "admin_errors_total"
)

// Metrics is the observability hook interface for kafkax clients.
// It is a 3-method subset of observex.Metrics; any observex.Metrics
// implementation satisfies this interface.
type Metrics interface {
	IncCounter(name string, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
	SetGauge(name string, value float64, labels map[string]string)
}

// NoopMetrics is an alias for observex.NoopMetrics; it satisfies Metrics and
// discards all observations. Aliasing removes the duplicate no-op body.
type NoopMetrics = obs.NoopMetrics
