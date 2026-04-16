# Bro-PM — Policy, Trust & Security Spec



# 4. Policy / Trust / Security Spec

## 4.1 Principles

No operation path may bypass policy evaluation or safe-pause checks.

## 4.2 Roles

- Boss

- Admin

- Agent runtime

- Executor

- Observer

## 4.3 Trust and authority

- Trusted command sources are explicit and versioned.

- Policy edits require boss/admin identity.

- Untrusted channels can read, suggest, clarify, but not mutate policy or critical actions.

## 4.4 Visibility model

- Boss: full detail.

- Executor: task-focused and safe summaries.

- Shared channels: compact, non-sensitive summaries.

- Audit sink: detailed with filtered redaction.

## 4.5 Safe pause triggers

- Source contradiction

- suspected prompt injection

- credential outage

- unresolved ambiguity on authority

- repeated integration integrity mismatch

## 4.6 Security controls

- Secrets stored outside LLM payloads.

- Strict allow-list for critical action types.

- Input normalization and command channel validation.

- Prompt-injection filtering before interpretation.

## 4.7 Policy mutability

- Policy core changes must be authorized and logged.

- Conflicting versions are rejected.
