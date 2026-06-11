package kafkax

const (
	MetricClientCreatedTotal           = "client_created_total"
	MetricClientClosedTotal            = "client_closed_total"
	MetricClientErrorsTotal            = "client_errors_total"
	MetricClientHealthStatus           = "client_health_status"
	MetricClientHealthLatencyMS        = "client_health_latency_ms"
	MetricClientRequestsTotal          = "client_requests_total"
	MetricClientRequestDurationSeconds = "client_request_duration_seconds"
	MetricClientRetriesTotal           = "client_retries_total"
	MetricClientInflight               = "client_inflight"
	MetricProducerMessagesTotal        = "producer_messages_total"
	MetricProducerErrorsTotal          = "producer_errors_total"
	MetricProducerLatencySeconds       = "producer_latency_seconds"
	MetricConsumerMessagesTotal        = "consumer_messages_total"
	MetricConsumerErrorsTotal          = "consumer_errors_total"
	MetricConsumerLag                  = "consumer_lag"
	MetricConsumerCommitsTotal         = "consumer_commits_total"
	MetricAdminOperationsTotal         = "admin_operations_total"
	MetricAdminErrorsTotal             = "admin_errors_total"
)

type Metrics interface {
	IncCounter(name string, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
	SetGauge(name string, value float64, labels map[string]string)
}

type NoopMetrics struct{}

func (NoopMetrics) IncCounter(name string, labels map[string]string) {}

func (NoopMetrics) ObserveHistogram(name string, value float64, labels map[string]string) {}

func (NoopMetrics) SetGauge(name string, value float64, labels map[string]string) {}
