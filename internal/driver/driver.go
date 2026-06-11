package driver

type Capability string

const (
	CapabilityProducer Capability = "producer"
	CapabilityConsumer Capability = "consumer"
	CapabilityAdmin    Capability = "admin"
)

type Descriptor struct {
	Name         string
	Capabilities []Capability
}

func (d Descriptor) Clone() Descriptor {
	cloned := d
	cloned.Capabilities = append([]Capability(nil), d.Capabilities...)
	return cloned
}

type Driver interface {
	Descriptor() Descriptor
}
