package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
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

	details := []string{
		"gate=" + command,
		"purpose=" + purpose,
		"broker_fixture=" + kafkaBrokerFixtureDetail(*fixture),
		"report_contract=gap_until_production_driver_and_broker_evidence",
	}
	gaps := []string{
		"production Kafka driver is not implemented",
		"broker-backed Kafka evidence is required before this gate can pass",
		"FakeKafka testkit evidence cannot satisfy broker-backed release evidence",
	}
	if strings.TrimSpace(*fixture) == "" {
		gaps = append(gaps, kafkaBrokerFixtureEnv+" is not set and --broker-fixture was not provided")
	}
	return emitReport(stdout, command, "gap", details, gaps)
}

func kafkaBrokerFixtureDetail(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "<unset>"
	}
	return value
}
