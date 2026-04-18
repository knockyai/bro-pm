# Bro-PM — System Architecture Spec



# 1. System Architecture Spec

## 1.1 Purpose

Bro-PM is an operational PM agent platform with two layers:

- Backend service with durable state, policy, permissions, audit, and execution safety.

- Hermes runtime for reasoning, planning, communication drafting, and exception analysis.

MVP enforces this split so that state and security do not depend on prompts.

## 1.2 Contextual boundaries

- **Authoritative truth**: Postgres state (projects, goals, tasks, policy, audit, risk, rollbacks, due actions, inbound conversation events).

- **Ephemeral reasoning + chat runtime**: Hermes gateway listens to Telegram/DMs, talks to people, and returns structured suggestions only.

- **Execution safety**: all outbound actions execute through backend pipelines with policy checks.

## 1.3 Architecture layers

- API & command plane (FastAPI): boss/admin control surface and operational APIs.

- Domain service: project memory, tasks, task graph, members, risks.

- Policy engine: trust matrix and permission checks.

- Event ingestion: normalize inbound events from chats/trackers/docs.

- Hermes gateway: chat/session runtime that listens to Telegram and calls backend control APIs.

- Due-action outbox: durable queue of outbound actions/messages waiting for Hermes delivery and acknowledgement.

- Execution engine: validates, executes, verifies, audits each action.

- Audit/rollback service: immutable action history and selective reverse operations.

- Learning layer: heuristic metrics and pattern extraction from history.

## 1.4 Trust boundaries

- Hermes never mutates state directly.

- All credentials and integration secrets are managed in backend storage.

- Every mutation has idempotency and policy gate.

## 1.5 Runtime flow (MVP)

1. Hermes gateway receives a chat event or cron/timer wake-up.

2. Backend normalizes and persists inbound event or due action state.

3. Policy selector builds execution context.

4. Hermes can call backend command APIs for structured suggestions or allowed execution.

5. Backend validates policy and safe-pause state.

6. Integrations execute actions using adapters.

7. Outbound actions that should be delivered through chat are stored as durable `DueAction` rows.

8. Hermes gateway can also send normalized inbound events into backend storage as `ConversationEvent` rows and receive a structured disposition.

9. Hermes gateway claims due actions, delivers them to Telegram/DMs, and acknowledges delivery back to backend.

10. Execution/delivery is always audited.

11. For the currently implemented timer-actions MVP, the default live app may also run a small in-process scheduler loop that periodically scans project reporting cadence, triggers `publish_report` through the existing reporting service path, and every 10 minutes runs an autonomous decision review that can either create internal tasks or enqueue boss-escalation due actions for Hermes delivery.

## 1.6 Deployment

- One backend service + worker sidecar initially.

- PostgreSQL for durable entities.

- Redis for locks, dedupe, and schedule/queueing.

## 1.7 Non-functional targets

- Deterministic IDs, traceable lineage, auditable actions.

- Replay-safe operations and deterministic retries.

- Notion-first operational visibility in MVP, dashboard later.
