package kafkax

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlan008READMEContractDocumentsRetryDLT(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	readme := string(data)

	for _, token := range []string{
		"{topic}.retry",
		"{topic}.retry.{delay}",
		"{topic}.dlt",
		"x-original-topic",
		"x-retry-count",
		"x-max-retries",
		"max_retries",
		"idempotency key",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md must document %q", token)
		}
	}
}
