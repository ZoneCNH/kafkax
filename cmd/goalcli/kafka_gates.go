package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/ZoneCNH/kafkax/pkg/kafkax/kafkago"
)

const kafkaBrokerFixtureEnv = "KAFKAX_BROKER_FIXTURE"

type kafkaContractRequirement struct {
	path    string
	markers []string
}

var kafkaContractRequirements = []kafkaContractRequirement{
	{
		path:    "contracts/l2-kafka-adapter.schema.json",
		markers: []string{"public_api", "contracts", "gates", "adoption_claim"},
	},
	{
		path:    "contracts/kafkax.config.schema.json",
		markers: []string{"brokers", "client_id", "security", "producer", "consumer", "admin"},
	},
	{
		path:    "contracts/kafkax.message.schema.json",
		markers: []string{"topic", "key", "value", "headers"},
	},
	{
		path:    "contracts/kafkax.topic.schema.json",
		markers: []string{"partitions", "replication_factor", "cleanup_policy", "config"},
	},
	{
		path:    "contracts/kafkax.metrics.schema.json",
		markers: []string{"metrics", "producer", "consumer", "admin", "lag", "dlq"},
	},
	{
		path: "docs/standard/l2-kafka-adapter.md",
		markers: []string{
			"kafka-contract",
			"kafka-integration",
			"kafka-fault-injection",
			"kafka-metrics-golden",
			"kafka-admin-golden",
		},
	},
	{
		path: "docs/standard/harness-gates.md",
		markers: []string{
			"kafka-contract",
			"kafka-integration",
			"kafka-fault-injection",
			"kafka-metrics-golden",
			"kafka-admin-golden",
		},
	},
	{
		path: ".agent/harness/harness.yaml",
		markers: []string{
			"kafka_contract",
			"GOWORK=off make kafka-contract",
			"kafka_integration",
			"GOWORK=off make kafka-integration",
			"kafka_fault_injection",
			"GOWORK=off make kafka-fault-injection",
			"kafka_metrics_golden",
			"GOWORK=off make kafka-metrics-golden",
			"kafka_admin_golden",
			"GOWORK=off make kafka-admin-golden",
		},
	},
	{
		path:    "contracts/contracts_test.go",
		markers: []string{"TestKafkaMessageContractMatchesPublicMessage", "TestKafkaTopicContractMatchesPublicTopicSpec"},
	},
	{
		path: "pkg/kafkax/api_contract_test.go",
		markers: []string{
			"TestPublicKafkaAPIExposesNoThirdPartyConcreteTypes",
			"segmentio/kafka-go",
			"confluentinc",
			"twmb/franz-go",
		},
	},
	{
		path: "testkit/kafka_test.go",
		markers: []string{
			"TestKafkaFakesRoundTripGoldenRecord",
			"TestKafkaFakeAdminPlansWithoutMutatingTopics",
		},
	},
}

var kafkaBrokerGatePurposes = map[string]string{
	"kafka-integration":     "broker-backed produce/consume integration evidence",
	"kafka-fault-injection": "broker-backed retry, cancellation, and failure-mode evidence",
	"kafka-metrics-golden":  "broker-backed metrics golden evidence",
	"kafka-admin-golden":    "broker-backed topic admin golden evidence",
}

func runKafkaContract(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("kafka-contract", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return emitReport(stdout, "kafka-contract", "failed", nil, []string{err.Error()})
	}
	if fs.NArg() != 0 {
		return emitReport(stdout, "kafka-contract", "failed", nil, []string{"unexpected positional arguments"})
	}

	var gaps []string
	for _, req := range kafkaContractRequirements {
		content, err := os.ReadFile(req.path)
		if err != nil {
			gaps = append(gaps, fmt.Sprintf("%s: %v", req.path, err))
			continue
		}
		body := string(content)
		for _, marker := range req.markers {
			if !strings.Contains(body, marker) {
				gaps = append(gaps, fmt.Sprintf("%s: missing marker %q", req.path, marker))
			}
		}
	}

	details := []string{
		"driver-neutral public API markers present",
		"Kafka schema contract markers present",
		"Kafka harness runtime gate markers present",
		"testkit and API guard test markers present",
	}
	if len(gaps) > 0 {
		return emitReport(stdout, "kafka-contract", "failed", details, gaps)
	}
	return emitReport(stdout, "kafka-contract", "passed", details, nil)
}

func runKafkaBrokerGate(command string, args []string, stdout, stderr io.Writer) int {
	purpose, ok := kafkaBrokerGatePurposes[command]
	if !ok {
		return emitReport(stdout, command, "failed", nil, []string{"unknown Kafka broker gate"})
	}

	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fixture := fs.String("broker-fixture", os.Getenv(kafkaBrokerFixtureEnv), "Kafka broker fixture descriptor")
	if err := fs.Parse(args); err != nil {
		return emitReport(stdout, command, "failed", nil, []string{err.Error()})
	}
	if fs.NArg() != 0 {
		return emitReport(stdout, command, "failed", nil, []string{"unexpected positional arguments"})
	}

	if strings.TrimSpace(*fixture) == "" {
		details := []string{
			"gate=" + command,
			"purpose=" + purpose,
			"broker_fixture=<unset>",
			"report_contract=broker_fixture_required",
		}
		gaps := []string{
			"broker-backed Kafka evidence is required before this gate can pass",
			"FakeKafka testkit evidence cannot satisfy broker-backed release evidence",
			kafkaBrokerFixtureEnv + " is not set and --broker-fixture was not provided",
		}
		return emitReport(stdout, command, "gap", details, gaps)
	}

	fx, err := loadKafkaBrokerFixture(*fixture)
	details := []string{
		"gate=" + command,
		"purpose=" + purpose,
		"broker_fixture=" + kafkaBrokerFixtureDetail(*fixture),
	}
	if err != nil {
		return emitReport(stdout, command, "failed", details, []string{"broker fixture could not be parsed: " + safeKafkaGateError(err, brokerFixture{raw: *fixture})})
	}
	details = append(details,
		fmt.Sprintf("broker_count=%d", len(fx.cfg.Brokers)),
		"security_protocol="+string(fx.cfg.Security.Protocol),
		"sasl_tls="+strconv.FormatBool(fx.saslTLS),
		"timeout="+fx.cfg.Timeout.String(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), fx.cfg.Timeout)
	defer cancel()
	metrics := newGateMetrics()
	if err := runKafkaBrokerGateEvidence(ctx, command, fx, metrics); err != nil {
		return emitReport(stdout, command, "failed", details, []string{safeKafkaGateError(err, fx)})
	}
	return emitReport(stdout, command, "passed", details, nil)
}

type brokerFixture struct {
	cfg     kafkax.Config
	saslTLS bool
	raw     string
	source  string
}

func loadKafkaBrokerFixture(value string) (fx brokerFixture, err error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return brokerFixture{}, errors.New("empty fixture")
	}
	vars := map[string]string{}
	source := "inline"
	if info, err := os.Stat(raw); err == nil && !info.IsDir() {
		file, err := os.Open(raw)
		if err != nil {
			return brokerFixture{raw: raw, source: "file"}, err
		}
		defer func() {
			if closeErr := file.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}()
		source = "file"
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			if mergeFixtureMarkdownLine(vars, scanner.Text()) {
				continue
			}
			mergeFixtureLine(vars, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return brokerFixture{raw: raw, source: source}, err
		}
	} else if u, err := url.Parse(raw); err == nil && u.Scheme != "" && u.Host != "" {
		mergeFixtureURL(vars, u)
	} else {
		for _, line := range strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == '\r' || r == ';' }) {
			if mergeFixtureMarkdownLine(vars, line) {
				continue
			}
			mergeFixtureLine(vars, line)
		}
		if len(vars) == 0 {
			mergeFixtureLine(vars, raw)
		}
	}

	brokers := splitBrokers(firstFixtureValue(vars,
		"KAFKA_BROKERS", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BROKER", "KAFKA_ADDR", "KAFKA_ADDRESS", "KAFKA_ENDPOINT", "KAFKA_URL", "KAFKA_DSN",
		"KAFKAX_BROKERS", "KAFKAX_BOOTSTRAP_SERVERS", "BOOTSTRAP_SERVERS", "REDPANDA_BROKERS", "REDPANDA_BOOTSTRAP_SERVERS",
	))
	if len(brokers) == 0 {
		return brokerFixture{raw: raw, source: source}, errors.New("no brokers found")
	}
	username := firstFixtureValue(vars, "KAFKA_USERNAME", "KAFKA_USER", "KAFKA_SASL_USERNAME", "KAFKAX_USERNAME", "SASL_USERNAME")
	password := firstFixtureValue(vars, "KAFKA_PASSWORD", "KAFKA_SASL_PASSWORD", "KAFKAX_PASSWORD", "SASL_PASSWORD")
	token := firstFixtureValue(vars, "KAFKA_TOKEN", "KAFKAX_TOKEN")
	protocolValue := firstFixtureValue(vars, "KAFKA_SECURITY_PROTOCOL", "KAFKA_PROTOCOL", "KAFKAX_SECURITY_PROTOCOL", "SECURITY_PROTOCOL")
	protocol, saslTLS, err := kafkaSecurityProtocol(protocolValue, username, password)
	if err != nil {
		return brokerFixture{raw: raw, source: source}, err
	}
	timeout := 30 * time.Second
	if timeoutValue := firstFixtureValue(vars, "KAFKAX_TIMEOUT", "KAFKA_TIMEOUT"); timeoutValue != "" {
		parsed, err := time.ParseDuration(timeoutValue)
		if err != nil {
			return brokerFixture{raw: raw, source: source}, fmt.Errorf("invalid timeout")
		}
		if parsed > 0 {
			timeout = parsed
		}
	}
	clientID := firstFixtureValue(vars, "KAFKAX_CLIENT_ID", "KAFKA_CLIENT_ID", "CLIENT_ID")
	if clientID == "" {
		clientID = "goalcli-kafkax"
	}
	groupID := firstFixtureValue(vars, "KAFKAX_GROUP_ID", "KAFKA_GROUP_ID", "GROUP_ID")
	if groupID == "" {
		groupID = "goalcli-kafkax"
	}
	fx = brokerFixture{
		raw:     raw,
		source:  source,
		saslTLS: saslTLS,
		cfg: kafkax.Config{
			Name:     "goalcli-kafka-broker",
			Brokers:  brokers,
			ClientID: clientID,
			Timeout:  timeout,
			Security: kafkax.SecurityConfig{
				Protocol: protocol,
				Username: username,
				Password: password,
				Token:    token,
			},
			Producer: kafkax.ProducerConfig{RequiredAcks: 1, BatchBytes: 1048576},
			Consumer: kafkax.ConsumerConfig{GroupID: groupID, StartOffset: kafkax.OffsetResetEarliest},
			Retry:    kafkax.RetryConfig{MaxAttempts: 3},
			Admin:    kafkax.AdminConfig{Timeout: timeout},
		},
	}
	if err := fx.cfg.Validate(); err != nil {
		return brokerFixture{raw: raw, source: source}, err
	}
	return fx, nil
}

func mergeFixtureLine(vars map[string]string, line string) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return
	}
	line = strings.TrimPrefix(line, "export ")
	if strings.Contains(line, "=") && strings.Count(line, "=") > 1 && !strings.Contains(line, " ") {
		// Preserve values containing '=' by parsing as one assignment below.
	} else if strings.Contains(line, " ") {
		for _, field := range strings.Fields(line) {
			mergeFixtureLine(vars, field)
		}
		return
	}
	var key, val string
	if before, after, ok := strings.Cut(line, "="); ok {
		key, val = before, after
	} else if before, after, ok := strings.Cut(line, ":"); ok {
		key, val = before, after
	} else {
		return
	}
	key = strings.TrimSpace(key)
	key = strings.Trim(strings.TrimPrefix(key, "export "), "'")
	key = strings.ToUpper(strings.TrimSpace(key))
	val = strings.TrimSpace(val)
	val = strings.Trim(val, `"'`)
	if key != "" {
		if u, err := url.Parse(val); err == nil && u.Scheme != "" && u.Host != "" && isBrokerFixtureKey(key) {
			mergeFixtureURL(vars, u)
			return
		}
		vars[key] = val
	}
}

func mergeFixtureMarkdownLine(vars map[string]string, line string) bool {
	if !strings.Contains(line, "|") || !strings.Contains(strings.ToLower(line), "kafka") {
		return false
	}
	cells := markdownFixtureCells(line)
	kafkaIndex := -1
	for i, cell := range cells {
		if strings.EqualFold(cell, "kafka") {
			kafkaIndex = i
			break
		}
	}
	if kafkaIndex < 0 {
		return false
	}
	if broker := brokerFromMarkdownKafkaRow(cells, kafkaIndex); broker != "" {
		vars["KAFKA_BROKERS"] = broker
	}
	if value := markdownCellAt(cells, kafkaIndex+3); value != "" {
		vars["KAFKA_USERNAME"] = value
	}
	if value := markdownCellAt(cells, kafkaIndex+4); value != "" {
		vars["KAFKA_PASSWORD"] = value
	}
	if protocol := protocolFromMarkdownKafkaRow(cells); protocol != "" {
		vars["KAFKA_SECURITY_PROTOCOL"] = protocol
	}
	return true
}

func markdownFixtureCells(line string) []string {
	parts := strings.Split(line, "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cell := cleanMarkdownFixtureCell(part)
		if cell != "" {
			cells = append(cells, cell)
		}
	}
	return cells
}

func cleanMarkdownFixtureCell(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "`")
	value = strings.TrimSpace(value)
	return value
}

func brokerFromMarkdownKafkaRow(cells []string, kafkaIndex int) string {
	host := markdownCellAt(cells, kafkaIndex+1)
	port := firstNumericToken(markdownCellAt(cells, kafkaIndex+2))
	if host != "" && port != "" {
		return host + ":" + port
	}
	for _, cell := range cells {
		if broker := firstBrokerAddress(cell); broker != "" {
			return broker
		}
	}
	return ""
}

func markdownCellAt(cells []string, index int) string {
	if index < 0 || index >= len(cells) {
		return ""
	}
	return strings.TrimSpace(cells[index])
}

func firstNumericToken(value string) string {
	for _, field := range strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsDigit(r)
	}) {
		if field != "" {
			return field
		}
	}
	return ""
}

func firstBrokerAddress(value string) string {
	for _, field := range strings.Fields(value) {
		field = strings.Trim(field, "`'\",")
		if u, err := url.Parse(field); err == nil && u.Scheme != "" && u.Host != "" {
			return u.Host
		}
		if strings.Count(field, ":") == 1 {
			before, after, _ := strings.Cut(field, ":")
			if before != "" && firstNumericToken(after) == after {
				return field
			}
		}
	}
	return ""
}

func protocolFromMarkdownKafkaRow(cells []string) string {
	joined := strings.ToUpper(strings.Join(cells, " "))
	for _, candidate := range []string{"SASL_PLAINTEXT", "SASL_SSL", "SASL_TLS", "PLAINTEXT", "SSL", "TLS", "SASL"} {
		if strings.Contains(joined, candidate) {
			return candidate
		}
	}
	return ""
}

func isBrokerFixtureKey(key string) bool {
	switch strings.ToUpper(strings.TrimSpace(key)) {
	case "KAFKA_BROKERS", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BROKER", "KAFKA_ADDR", "KAFKA_ADDRESS", "KAFKA_ENDPOINT", "KAFKA_URL", "KAFKA_DSN",
		"KAFKAX_BROKERS", "KAFKAX_BOOTSTRAP_SERVERS", "BOOTSTRAP_SERVERS", "REDPANDA_BROKERS", "REDPANDA_BOOTSTRAP_SERVERS":
		return true
	default:
		return false
	}
}

func mergeFixtureURL(vars map[string]string, u *url.URL) {
	vars["KAFKA_BROKERS"] = u.Host
	if u.User != nil {
		vars["KAFKA_USERNAME"] = u.User.Username()
		if password, ok := u.User.Password(); ok {
			vars["KAFKA_PASSWORD"] = password
		}
	}
	q := u.Query()
	if token := q.Get("token"); token != "" {
		vars["KAFKA_TOKEN"] = token
	}
	if protocol := q.Get("security_protocol"); protocol != "" {
		vars["KAFKA_SECURITY_PROTOCOL"] = protocol
	} else {
		switch strings.ToLower(u.Scheme) {
		case "kafka+ssl", "kafka+tls":
			vars["KAFKA_SECURITY_PROTOCOL"] = "TLS"
		case "sasl+ssl", "sasl+tls":
			vars["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
		case "broker", "kafka":
			vars["KAFKA_SECURITY_PROTOCOL"] = "PLAINTEXT"
		}
	}
	if timeout := q.Get("timeout"); timeout != "" {
		vars["KAFKAX_TIMEOUT"] = timeout
	}
}

func firstFixtureValue(vars map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(vars[strings.ToUpper(key)]); value != "" {
			return value
		}
	}
	return ""
}

func splitBrokers(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ';' || unicode.IsSpace(r) })
	brokers := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			brokers = append(brokers, field)
		}
	}
	return brokers
}

func kafkaSecurityProtocol(value, username, password string) (kafkax.SecurityProtocol, bool, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "PLAINTEXT":
		if username != "" || password != "" {
			return kafkax.SecurityProtocolSASL, false, nil
		}
		return kafkax.SecurityProtocolPlaintext, false, nil
	case "SSL", "TLS":
		return kafkax.SecurityProtocolTLS, false, nil
	case "SASL", "SASL_PLAINTEXT":
		return kafkax.SecurityProtocolSASL, false, nil
	case "SASL_SSL", "SASL_TLS":
		return kafkax.SecurityProtocolSASL, true, nil
	default:
		return "", false, fmt.Errorf("unsupported security protocol")
	}
}

func runKafkaBrokerGateEvidence(ctx context.Context, command string, fx brokerFixture, metrics *gateMetrics) error {
	switch command {
	case "kafka-integration":
		return runKafkaIntegrationGate(ctx, fx, metrics)
	case "kafka-fault-injection":
		return runKafkaFaultInjectionGate(ctx, fx, metrics)
	case "kafka-metrics-golden":
		return runKafkaMetricsGoldenGate(ctx, fx, metrics)
	case "kafka-admin-golden":
		return runKafkaAdminGoldenGate(ctx, fx, metrics)
	default:
		return errors.New("unknown Kafka broker gate")
	}
}

func runKafkaIntegrationGate(ctx context.Context, fx brokerFixture, metrics *gateMetrics) error {
	driver, client, err := newKafkaGateClient(ctx, fx, metrics)
	if err != nil {
		return err
	}
	defer func() { _ = driver.Close(context.Background()) }()
	defer func() { _ = client.Close(context.Background()) }()
	topic := uniqueKafkaGateTopic("kafka-integration")
	admin, err := client.Admin()
	if err != nil {
		return err
	}
	if err := ensureKafkaGateTopic(ctx, admin, topic); err != nil {
		return err
	}
	producer, err := client.Producer()
	if err != nil {
		return err
	}
	key := []byte("goalcli-key-" + topic)
	value := []byte("goalcli-value-" + topic)
	if _, err := producer.Send(ctx, kafkax.Message{Topic: topic, Key: key, Value: value, Timestamp: time.Now().UTC()}); err != nil {
		return err
	}
	consumer, err := client.Consumer(topic+"-group", topic)
	if err != nil {
		return err
	}
	defer func() { _ = consumer.Close(context.Background()) }()
	deadline := time.Now().Add(fx.cfg.Timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		batch, err := consumer.Poll(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		for _, record := range batch.Records {
			if record.Topic == topic && string(record.Key) == string(key) && string(record.Value) == string(value) {
				return consumer.Commit(ctx, record.Offset)
			}
		}
	}
	if lastErr != nil {
		return fmt.Errorf("message round trip not observed: %w", lastErr)
	}
	return errors.New("message round trip not observed")
}

func runKafkaFaultInjectionGate(ctx context.Context, fx brokerFixture, metrics *gateMetrics) error {
	driver, client, err := newKafkaGateClient(ctx, fx, metrics)
	if err != nil {
		return err
	}
	defer func() { _ = driver.Close(context.Background()) }()
	defer func() { _ = client.Close(context.Background()) }()
	topic := uniqueKafkaGateTopic("kafka-fault-injection")
	admin, err := client.Admin()
	if err != nil {
		return err
	}
	if err := ensureKafkaGateTopic(ctx, admin, topic); err != nil {
		return err
	}
	producer, err := client.Producer()
	if err != nil {
		return err
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := producer.Send(cancelled, kafkax.Message{Topic: topic, Key: []byte("cancelled"), Value: []byte("cancelled")}); err == nil {
		return errors.New("cancelled produce unexpectedly succeeded")
	}
	_, err = producer.Send(ctx, kafkax.Message{Topic: topic, Key: []byte("recovered"), Value: []byte("recovered"), Timestamp: time.Now().UTC()})
	return err
}

func runKafkaMetricsGoldenGate(ctx context.Context, fx brokerFixture, metrics *gateMetrics) error {
	if err := runKafkaIntegrationGate(ctx, fx, metrics); err != nil {
		return err
	}
	for _, metric := range []string{kafkax.MetricClientCreatedTotal, kafkax.MetricAdminOperationsTotal, kafkax.MetricProducerMessagesTotal, kafkax.MetricConsumerMessagesTotal} {
		if !metrics.hasCounter(metric) {
			return fmt.Errorf("metric %s was not recorded", metric)
		}
	}
	return nil
}

func runKafkaAdminGoldenGate(ctx context.Context, fx brokerFixture, metrics *gateMetrics) error {
	driver, client, err := newKafkaGateClient(ctx, fx, metrics)
	if err != nil {
		return err
	}
	defer func() { _ = driver.Close(context.Background()) }()
	defer func() { _ = client.Close(context.Background()) }()
	topic := uniqueKafkaGateTopic("kafka-admin-golden")
	admin, err := client.Admin()
	if err != nil {
		return err
	}
	if err := ensureKafkaGateTopic(ctx, admin, topic); err != nil {
		return err
	}
	descriptions, err := admin.DescribeTopics(ctx, topic)
	if err != nil {
		return err
	}
	for _, desc := range descriptions {
		if desc.Name == topic && desc.Partitions >= 1 {
			return nil
		}
	}
	return errors.New("created topic was not described")
}

func newKafkaGateClient(ctx context.Context, fx brokerFixture, metrics *gateMetrics) (*kafkago.Driver, *kafkax.Client, error) {
	driver, err := kafkago.New(fx.cfg, kafkago.WithMetrics(metrics), kafkago.WithSASLTLS(fx.saslTLS))
	if err != nil {
		return nil, nil, err
	}
	client, err := kafkax.New(ctx, fx.cfg, driver.ClientOptions()...)
	if err != nil {
		closeErr := driver.Close(context.Background())
		return nil, nil, errors.Join(err, closeErr)
	}
	return driver, client, nil
}

func ensureKafkaGateTopic(ctx context.Context, admin kafkax.Admin, topic string) error {
	plan, err := admin.PlanTopics(ctx, kafkax.TopicSpec{Name: topic, Partitions: 1, ReplicationFactor: 1})
	if err != nil {
		return err
	}
	_, err = admin.ApplyTopics(ctx, plan)
	return err
}

func uniqueKafkaGateTopic(command string) string {
	var b strings.Builder
	b.WriteString("goalcli-")
	for _, r := range command {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' {
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune('-')
		}
	}
	b.WriteString("-")
	b.WriteString(strconv.FormatInt(time.Now().UnixNano(), 10))
	return b.String()
}

type gateMetrics struct {
	mu       sync.Mutex
	counters map[string]int
}

func newGateMetrics() *gateMetrics { return &gateMetrics{counters: map[string]int{}} }

func (m *gateMetrics) IncCounter(name string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name]++
}

func (m *gateMetrics) ObserveHistogram(name string, value float64, labels map[string]string) {}
func (m *gateMetrics) SetGauge(name string, value float64, labels map[string]string)         {}

func (m *gateMetrics) hasCounter(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name] > 0
}

func safeKafkaGateError(err error, fx brokerFixture) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	redactions := []string{fx.raw, fx.cfg.Security.Username, fx.cfg.Security.Password, fx.cfg.Security.Token}
	redactions = append(redactions, fx.cfg.Brokers...)
	for _, secret := range redactions {
		secret = strings.TrimSpace(secret)
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "<redacted>")
		}
	}
	return message
}

func kafkaBrokerFixtureDetail(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "<unset>"
	}
	return "<set:redacted>"
}
