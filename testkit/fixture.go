package testkit

import (
	"time"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
)

func Config(name string) kafkax.Config {
	return kafkax.Config{
		Name:    name,
		Timeout: time.Second,
	}
}
