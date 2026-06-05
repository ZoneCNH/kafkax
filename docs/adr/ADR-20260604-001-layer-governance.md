# ADR-20260604-001: Layer governance boundary

## Status

Accepted.

## Decision

`kafkax` is the Standard Source and governed L2/L0 template authority. L3 私有 business systems may consume generated libraries or compose adapters, but private business code, deployment secrets, and `x.go` models must not move into the public standard repository.

## Consequences

- `docs-check` must keep the layer-governance decision visible so template, generator, Harness, and Evidence changes do not blur public/private boundaries.
- Public contracts may mention `/home/k8s/secrets/env/*` only as a caller-owned deployment path; real secret contents must never be committed.
- L2 adapter contracts may define infrastructure-level Kafka surfaces, but L3 私有 business schemas stay outside this repository.
