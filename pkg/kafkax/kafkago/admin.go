package kafkago

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/segmentio/kafka-go"
)

type admin struct{ d *Driver }

func newAdmin(d *Driver) *admin { return &admin{d: d} }

func (a *admin) DescribeTopics(ctx context.Context, topics ...string) ([]kafkax.TopicDescription, error) {
	const op = "kafkago.Admin.DescribeTopics"
	if a == nil || a.d == nil {
		return nil, kafkax.NewError(kafkax.ErrorKindDriver, op, "admin is closed", false)
	}
	ctx, cancel := a.d.timeoutContext(ctx, a.d.cfg.Admin.Timeout)
	defer cancel()
	conn, err := a.d.dialBroker(ctx)
	if err != nil {
		inc(a.d.metrics, kafkax.MetricAdminErrorsTotal, map[string]string{"op": "describe"})
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	partitions, err := conn.ReadPartitions(topics...)
	if err != nil {
		inc(a.d.metrics, kafkax.MetricAdminErrorsTotal, map[string]string{"op": "describe"})
		return nil, wrapContextOr(kafkax.ErrorKindAdmin, op, "describe topics", true, err)
	}
	byTopic := make(map[string]map[int]kafka.Partition)
	for _, partition := range partitions {
		if len(topics) > 0 && !contains(topics, partition.Topic) {
			continue
		}
		if _, ok := byTopic[partition.Topic]; !ok {
			byTopic[partition.Topic] = make(map[int]kafka.Partition)
		}
		byTopic[partition.Topic][partition.ID] = partition
	}
	descriptions := make([]kafkax.TopicDescription, 0, len(byTopic))
	for topic, topicPartitions := range byTopic {
		desc := kafkax.TopicDescription{Name: topic, Partitions: len(topicPartitions)}
		for _, partition := range topicPartitions {
			desc.ReplicationFactor = len(partition.Replicas)
			break
		}
		descriptions = append(descriptions, desc)
	}
	inc(a.d.metrics, kafkax.MetricAdminOperationsTotal, map[string]string{"op": "describe"})
	return descriptions, nil
}

func (a *admin) PlanTopics(ctx context.Context, specs ...kafkax.TopicSpec) (kafkax.TopicPlan, error) {
	const op = "kafkago.Admin.PlanTopics"
	if len(specs) == 0 {
		return kafkax.TopicPlan{Action: kafkax.TopicPlanNoop}, nil
	}
	plan := kafkax.TopicPlan{Action: kafkax.TopicPlanNoop, Specs: cloneSpecs(specs), DryRun: a.d.cfg.Admin.DryRun}
	for _, spec := range specs {
		if spec.Name == "" {
			return kafkax.TopicPlan{}, kafkax.NewError(kafkax.ErrorKindConfig, op, "topic name is required", false)
		}
		descriptions, err := a.DescribeTopics(ctx, spec.Name)
		if err != nil || len(descriptions) == 0 {
			plan.Action = kafkax.TopicPlanCreate
			plan.Spec = spec.Clone()
			plan.Changes = append(plan.Changes, kafkax.TopicChange{Field: spec.Name, From: "missing", To: "present"})
		}
	}
	return plan, nil
}

func (a *admin) ApplyTopics(ctx context.Context, plan kafkax.TopicPlan) (kafkax.TopicApplyResult, error) {
	const op = "kafkago.Admin.ApplyTopics"
	if plan.DryRun || a.d.cfg.Admin.DryRun {
		return kafkax.TopicApplyResult{DryRun: true}, nil
	}
	if plan.Action == kafkax.TopicPlanNoop || len(plan.Specs) == 0 && plan.Spec.Name == "" {
		return kafkax.TopicApplyResult{}, nil
	}
	ctx, cancel := a.d.timeoutContext(ctx, a.d.cfg.Admin.Timeout)
	defer cancel()
	conn, err := a.d.dialController(ctx)
	if err != nil {
		inc(a.d.metrics, kafkax.MetricAdminErrorsTotal, map[string]string{"op": "apply"})
		return kafkax.TopicApplyResult{}, err
	}
	defer func() { _ = conn.Close() }()
	specs := plan.Specs
	if len(specs) == 0 && plan.Spec.Name != "" {
		specs = []kafkax.TopicSpec{plan.Spec}
	}
	configs := make([]kafka.TopicConfig, 0, len(specs))
	for _, spec := range specs {
		configs = append(configs, toTopicConfig(spec))
	}
	if len(configs) > 0 {
		if err := conn.CreateTopics(configs...); err != nil {
			inc(a.d.metrics, kafkax.MetricAdminErrorsTotal, map[string]string{"op": "apply"})
			return kafkax.TopicApplyResult{}, wrapContextOr(kafkax.ErrorKindAdmin, op, "create topics", true, err)
		}
	}
	var names []string
	for _, spec := range specs {
		names = append(names, spec.Name)
	}
	descriptions, err := a.DescribeTopics(ctx, names...)
	if err != nil {
		return kafkax.TopicApplyResult{}, err
	}
	inc(a.d.metrics, kafkax.MetricAdminOperationsTotal, map[string]string{"op": "apply"})
	return kafkax.TopicApplyResult{Applied: descriptions}, nil
}

func (a *admin) Close(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return wrapContextOr(kafkax.ErrorKindUnavailable, "kafkago.Admin.Close", "close admin", false, ctx.Err())
	}
	return nil
}

func toTopicConfig(spec kafkax.TopicSpec) kafka.TopicConfig {
	partitions := spec.Partitions
	if partitions <= 0 {
		partitions = 1
	}
	replication := spec.ReplicationFactor
	if replication <= 0 {
		replication = 1
	}
	config := kafka.TopicConfig{Topic: spec.Name, NumPartitions: partitions, ReplicationFactor: replication}
	entries := map[string]string{}
	for key, value := range spec.Config {
		entries[key] = value
	}
	if spec.Retention > 0 {
		entries["retention.ms"] = strconv.FormatInt(spec.Retention.Milliseconds(), 10)
	}
	if spec.CleanupPolicy != "" {
		entries["cleanup.policy"] = string(spec.CleanupPolicy)
	}
	if spec.Compression != "" {
		entries["compression.type"] = string(spec.Compression)
	}
	if spec.MinInSyncReplicas > 0 {
		entries["min.insync.replicas"] = fmt.Sprint(spec.MinInSyncReplicas)
	}
	for key, value := range entries {
		config.ConfigEntries = append(config.ConfigEntries, kafka.ConfigEntry{ConfigName: key, ConfigValue: value})
	}
	return config
}

func cloneSpecs(specs []kafkax.TopicSpec) []kafkax.TopicSpec {
	cloned := make([]kafkax.TopicSpec, len(specs))
	for i, spec := range specs {
		cloned[i] = spec.Clone()
	}
	return cloned
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
