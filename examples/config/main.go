package main

import (
	"fmt"
	"time"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
)

func main() {
	cfg := kafkax.Config{
		Name:    "kafkax",
		Timeout: time.Second,
		Secret:  "example",
	}

	fmt.Println(cfg.Sanitize().Secret)
}
