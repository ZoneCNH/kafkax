package kafkax

import "context"

type Admin interface {
	DescribeTopic(context.Context, string) (TopicDescription, error)
	PlanTopic(context.Context, TopicSpec) (TopicPlan, error)
	ApplyTopic(context.Context, TopicPlan) error
	Close(context.Context) error
}

type TopicSpec struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
}

func (s TopicSpec) Clone() TopicSpec {
	cloned := s
	cloned.Config = cloneStringMap(s.Config)
	return cloned
}

type TopicDescription struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
}

func (d TopicDescription) Clone() TopicDescription {
	cloned := d
	cloned.Config = cloneStringMap(d.Config)
	return cloned
}

type TopicPlanAction string

const (
	TopicPlanNoop   TopicPlanAction = "noop"
	TopicPlanCreate TopicPlanAction = "create"
	TopicPlanUpdate TopicPlanAction = "update"
	TopicPlanDelete TopicPlanAction = "delete"
)

type TopicPlan struct {
	Action  TopicPlanAction
	Spec    TopicSpec
	Changes []TopicChange
	DryRun  bool
}

func (p TopicPlan) Clone() TopicPlan {
	cloned := p
	cloned.Spec = p.Spec.Clone()
	cloned.Changes = append([]TopicChange(nil), p.Changes...)
	return cloned
}

type TopicChange struct {
	Field string
	From  string
	To    string
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
