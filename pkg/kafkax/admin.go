package kafkax

import "context"

type Admin interface {
	DescribeTopics(context.Context, ...string) ([]TopicDescription, error)
	PlanTopics(context.Context, ...TopicSpec) (TopicPlan, error)
	ApplyTopics(context.Context, TopicPlan) (TopicApplyResult, error)
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
	Specs   []TopicSpec
	Changes []TopicChange
	DryRun  bool
}

func (p TopicPlan) Clone() TopicPlan {
	cloned := p
	cloned.Spec = p.Spec.Clone()
	if len(p.Specs) > 0 {
		cloned.Specs = make([]TopicSpec, len(p.Specs))
		for i, spec := range p.Specs {
			cloned.Specs[i] = spec.Clone()
		}
	}
	cloned.Changes = append([]TopicChange(nil), p.Changes...)
	return cloned
}

type TopicChange struct {
	Field string
	From  string
	To    string
}

type TopicApplyResult struct {
	Applied []TopicDescription
	DryRun  bool
}

func (r TopicApplyResult) Clone() TopicApplyResult {
	if len(r.Applied) == 0 {
		return TopicApplyResult{DryRun: r.DryRun}
	}
	cloned := TopicApplyResult{Applied: make([]TopicDescription, len(r.Applied)), DryRun: r.DryRun}
	for i, description := range r.Applied {
		cloned.Applied[i] = description.Clone()
	}
	return cloned
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
