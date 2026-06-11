// Package kafkax provides the public contract surface for a Kafka L2 adapter.
//
// The package keeps Kafka-facing APIs driver-neutral so callers can depend on
// Producer, Consumer, Admin, Message, Config, HealthCheck, Error model, Metrics
// hooks, contracts, CI gates, release manifest, and agent evidence without
// importing a concrete Kafka client implementation.
//
// This package must not depend on github.com/bytechainx/x.go, github.com/ZoneCNH/x.go,
// or any x.go internal package.
package kafkax
