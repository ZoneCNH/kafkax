package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
)

func main() {
	run(os.Stdout, os.Stderr, kafkax.Config{Name: "kafkax"})
}

func run(stdout, stderr io.Writer, cfg kafkax.Config) {
	client, err := kafkax.New(context.Background(), cfg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "create client: %v\n", err)
		return
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	_, _ = fmt.Fprintln(stdout, kafkax.ModuleName)
}
