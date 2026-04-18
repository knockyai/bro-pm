# Bro-PM — System Architecture Spec



# 1. System Architecture Spec

## 1.1 Purpose

Bro-PM is an operational PM agent platform with two layers:

- Backend service with durable state, policy, permissions, audit, and execution safety.

- Hermes runtime for reasoning, planning, communication drafting, and exception analysis.

MVP enforces this split so that state and security do not depend on prompts.

## 1.2 Contextual boundaries

- **Authoritative truth**: Postgres state (projects, goals, tasks, policy, audit, risk, rollbacks).

- **Ephemeral reasoning**: Hermes returns structured suggestions only.

- **Execution safety**: all outbound actions execute through backend pipelines with policy checks.

## 1.3 Architecture layers

- API & command plane (FastAPI): boss/admin control surface and operational APIs.

- Domain service: project memory, tasks, task graph, members, risks.

- Policy engine: trust matrix and permission checks.

- Event ingestion: normalize inbound events from chats/trackers/docs.

- Hermes orchestrator: generates structured proposals from sliced context.

- Execution engine: validates, executes, verifies, audits each action.

- Audit/rollback service: immutable action history and selective reverse operations.

- Learning layer: heuristic metrics and pattern extraction from history.

## 1.4 Trust boundaries

- Hermes never mutates state directly.

- All credentials and integration secrets are managed in backend storage.

- Every mutation has idempotency and policy gate.

## 1.5 Runtime flow (MVP)

1. Event/command enters API or webhook.

2. Backend normalizes and persists inbound event.

3. Policy selector builds execution context.

4. Hermes job is invoked with structured request.

5. Response is schema-validated and mapped to one or more action proposals.

6. Engine validates policy and safe-pause state.

7. Integrations execute actions using adapters.

8. Execution is verified and always audited.

9. For the currently implemented timer-actions MVP, the default live app may also run a small in-process scheduler loop that periodically scans project reporting cadence and triggers `publish_report` through the existing reporting service path.

## 1.6 Deployment

- One backend service + worker sidecar initially.

- PostgreSQL for durable entities.

- Redis for locks, dedupe, and schedule/queueing.

## 1.7 Non-functional targets

- Deterministic IDs, traceable lineage, auditable actions.

- Replay-safe operations and deterministic retries.

- Notion-first operational visibility in MVP, dashboard later.
