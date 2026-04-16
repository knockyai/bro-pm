# Bro-PM — ADR Log



# 14. ADR Log Structure / First ADRs

## 14.1 Format

Every ADR uses fields: id, date, status, context, decision, alternatives, consequences.

## 14.2 Storage policy

- keep markdown ADRs under `docs/adr/` in code repository

- keep same link list in Notion documentation hub

## ADR-0001: Backend owns truth

- Context: Need to avoid prompt state and unsafe autonomous actions.

- Decision: Hermes only proposes actions; backend validates and executes.

- Consequences: safer, auditable, slower startup.

## ADR-0002: Choose Postgres + Redis MVP stack

- Context: Need durable state + simple retries/dedup.

- Decision: Postgres + Redis.

- Alternatives: NoSQL + in-memory.

- Consequences: production-friendly migration path.

## ADR-0003: Notion-first reporting for MVP

- Context: Dashboard is optional in scope.

- Decision: Notion-first report publishing.

- Consequences: quicker value, simpler UI build.
