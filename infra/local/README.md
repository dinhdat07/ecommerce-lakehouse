# Local Infrastructure Profile

This directory documents the existing laptop/demo deployment target.

Local/demo assets remain the current default:
- root `infra/docker-compose.yml`
- root `infra/scripts/run-e2e-demo.sh`
- root `infra/scripts/run-streaming-demo.sh`
- root `infra/scripts/run-phase3-benchmarks.sh`

Use this profile when:
- running on a laptop or WSL
- validating the bounded demo pipeline
- testing small replay samples
- running demo-scale benchmarks

Environment template:
- `infra/local/env/local.env.example`

Server/full-scale assets live separately under `infra/server/`.
