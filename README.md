# databricks-rag-platform

A reference implementation of a governed, multi-tenant RAG platform on
Databricks and Azure — ingestion through evaluation, with Declarative
Automation Bundle-based CI/CD for team onboarding.

## What this is

A working bundle definition and pipeline codebase implementing the
architecture documented in full in [`docs/DESIGN.md`](docs/DESIGN.md) —
every design decision here, along with the reasoning behind it, lives there.
This repo is the concrete "how it's actually built" companion to that
document.

## Structure
```
├── docs/DESIGN.md       # full architecture and rationale
├── src/                 # pipeline logic, per layer (bronze -> evaluation)
├── resources/           # DAB resource definitions (pipelines, dashboards,
│                           alerts, indexes, serving endpoints)
├── config/               # team-facing config (extraction, chunking,
│                           embedding, retrieval, evaluation)
├── databricks.yml        # root bundle definition
└── .github/workflows/    # CI/CD: validate, then gated deploy
```

## Getting started

See [`PREREQUISITES.md`](PREREQUISITES.md) before deploying anything.

## Status

Built layer by layer, matching the design doc's own sequence: bronze first,
then silver, gold, retrieval, evaluation. Each layer is complete and
documented before the next begins.