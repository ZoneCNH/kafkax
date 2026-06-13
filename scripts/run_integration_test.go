package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunIntegrationGeneratesStandardImpactBeforeReleaseEvidence(t *testing.T) {
	contents, err := os.ReadFile("run_integration.sh")
	if err != nil {
		t.Fatalf("read run_integration.sh: %v", err)
	}

	script := string(contents)
	standardImpactIndex := strings.Index(script, "GOWORK=off make standard-impact-check")
	if standardImpactIndex < 0 {
		t.Fatal("run_integration.sh does not generate standard impact evidence")
	}
	debtIndex := strings.Index(script, "\n    GOWORK=off make debt\n")
	if debtIndex < 0 {
		t.Fatal("run_integration.sh does not run downstream debt gate")
	}
	debtEvidenceIndex := strings.Index(script, "GOWORK=off make debt-evidence")
	if debtEvidenceIndex < 0 {
		t.Fatal("run_integration.sh does not generate downstream debt evidence")
	}
	debtChecksumIndex := strings.Index(script, "GOWORK=off make debt-evidence-checksum-check")
	if debtChecksumIndex < 0 {
		t.Fatal("run_integration.sh does not verify downstream debt evidence checksum")
	}
	evidenceIndex := strings.Index(script, "CHECK_STATUS=passed GOWORK=off make evidence")
	if evidenceIndex < 0 {
		t.Fatal("run_integration.sh does not generate release evidence")
	}
	checkIndex := strings.Index(script, "RELEASE_EVIDENCE_REQUIRE_PASSED=1 GOWORK=off make release-evidence-check")
	if checkIndex < 0 {
		t.Fatal("run_integration.sh does not verify release evidence")
	}

	if standardImpactIndex > debtIndex || debtIndex > debtEvidenceIndex || debtEvidenceIndex > debtChecksumIndex || debtChecksumIndex > evidenceIndex || evidenceIndex > checkIndex {
		t.Fatalf(
			"integration evidence order is wrong: standard-impact=%d debt=%d debt-evidence=%d debt-checksum=%d evidence=%d release-check=%d",
			standardImpactIndex,
			debtIndex,
			debtEvidenceIndex,
			debtChecksumIndex,
			evidenceIndex,
			checkIndex,
		)
	}
}

func TestRunIntegrationCoversRequiredDownstreams(t *testing.T) {
	contents, err := os.ReadFile("run_integration.sh")
	if err != nil {
		t.Fatalf("read run_integration.sh: %v", err)
	}

	script := string(contents)
	for _, target := range []string{
		"kernel|github.com/ZoneCNH/kernel|kernel",
		"configx|github.com/ZoneCNH/configx|configx",
		"redisx|github.com/ZoneCNH/redisx|redisx",
	} {
		if !strings.Contains(script, target) {
			t.Fatalf("run_integration.sh missing downstream target %q", target)
		}
	}

	if strings.Contains(script, "corekit|example.com/acme/corekit|corekit") {
		t.Fatal("run_integration.sh still includes legacy corekit integration target")
	}
}

func TestRenderedTemplateCheckSkipsMigratedInboxArchive(t *testing.T) {
	contents, err := os.ReadFile("check_rendered_template.sh")
	if err != nil {
		t.Fatalf("read check_rendered_template.sh: %v", err)
	}

	script := string(contents)
	for _, token := range []string{
		"--glob '!.agent/archive/inbox/**'",
		"--glob '!**/.agent/archive/inbox/**'",
		"-not -path '*/.agent/archive/inbox/*'",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("check_rendered_template.sh missing migrated inbox archive exclusion %q", token)
		}
	}
}

func TestRenderedTemplateCheckAllowsGoCompositeLiterals(t *testing.T) {
	modulePath := "example.com/rendered/kernel"
	repoDir := newMinimalRenderedRepo(t, modulePath, "kernel")
	writeTestFile(t, filepath.Join(repoDir, "pkg/kernel/api_contract_test.go"), `package kernel

type ProduceResult struct {
	Topic string
}

var _ = []ProduceResult{{Topic: "orders"}}
`)

	cmd := exec.Command("bash", "check_rendered_template.sh", repoDir, "kernel", modulePath, "kernel")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("check_rendered_template.sh rejected Go composite literal: %v\n%s", err, output)
	}
}

func TestRenderedTemplateCheckRejectsReleasePlaceholders(t *testing.T) {
	modulePath := "example.com/rendered/kernel"
	repoDir := newMinimalRenderedRepo(t, modulePath, "kernel")
	writeTestFile(t, filepath.Join(repoDir, "docs/release.md"), "commit: {{"+"COMMIT}}\n")

	cmd := exec.Command("bash", "check_rendered_template.sh", repoDir, "kernel", modulePath, "kernel")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("check_rendered_template.sh accepted stale release placeholder:\n%s", output)
	}
	if !strings.Contains(string(output), "found stale template placeholder") {
		t.Fatalf("check_rendered_template.sh failed for the wrong reason:\n%s", output)
	}
}

func newMinimalRenderedRepo(t *testing.T, modulePath, packageName string) string {
	t.Helper()

	repoDir := t.TempDir()
	writeTestFile(t, filepath.Join(repoDir, "go.mod"), "module "+modulePath+"\n\ngo 1.23\n")
	writeTestFile(t, filepath.Join(repoDir, "pkg", packageName, ".keep"), "")
	writeTestFile(t, filepath.Join(repoDir, "Dockerfile"), "FROM scratch\n")
	writeTestFile(t, filepath.Join(repoDir, "docker-compose.yml"), "services: {}\n")
	writeTestFile(t, filepath.Join(repoDir, ".dockerignore"), ".git\n")
	writeTestFile(t, filepath.Join(repoDir, ".devcontainer", "devcontainer.json"), "{}\n")
	writeTestFile(t, filepath.Join(repoDir, "scripts", "docker", "check_toolchain.sh"), "#!/usr/bin/env bash\n")
	writeTestFile(t, filepath.Join(repoDir, "scripts", "docker", "docker_gate.sh"), "#!/usr/bin/env bash\n")
	writeTestFile(t, filepath.Join(repoDir, "Makefile"), `.PHONY: docker-toolchain-check docker-build docker-build-check docker-shell docker-ci docker-release-check docker-release-final-check docker-goalcli docker-goalcli-image docker-goalcli-version docker-runtime-check docker-drift-check docker-contract
`)
	return repoDir
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
