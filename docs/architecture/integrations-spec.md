# Bro-PM — Integrations Spec



# 5. Integrations Spec

## 5.1 Priority channels

- Telegram

- Slack

- Jira

- Trello

- Notion

- Yandex Tracker

## 5.2 Integration adapter interface

Current MVP adapter boundary already exists in code and supports board adapters such as `notion`, `jira`, `trello`, and `yandex_tracker`.

- `ingest_events()`

- `fetch_state()`

- `apply_action()`

- `verify_action_result()`

## 5.3 Canonical event format

`event_id`, `source`, `source_ref`, `kind`, `payload`, `actor`, `occurred_at`, `correlation_id`.

## 5.4 Idempotency and replay

- external actions contain dedupe key.

- repeated same key is treated as idempotent.

## 5.5 Reliability

- retry with exponential backoff on transient failures.

- dead-letter and escalation on persistent failure.

## 5.6 Verification

- every successful mutation must be verified against source-of-truth state.

- mismatches create risk events.

## 5.7 Security

- secure credential references.

- scope-limited tokens.

- event signatures where platform supports.
