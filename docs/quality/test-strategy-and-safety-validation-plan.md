# Bro-PM — Test Strategy & Safety Validation Plan



# 12. Test Strategy & Safety Validation Plan

## 12.1 Scope

Testing follows RED-GREEN-REFACTOR and covers policy safety first.

## 12.2 Layers

- Unit tests for domain/model/policy

- Integration tests for adapters

- Contract tests for Hermes IO

- Safety tests for injection and unauthorized actions

- Rollback and idempotency tests

- E2E slice for onboarding -> task -> status -> report

## 12.3 Safety test list

- unsafe command rejected

- unauthorized mutation rejected

- policy conflict rejection

- safe pause forced on ambiguity/contradiction

- integration verify mismatch generates risk signal

## 12.4 CI gates

- all tests pass

- lint checks

- migration smoke

- basic load test for one concurrent action stream
