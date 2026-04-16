# Bro-PM — Audit & Rollback Spec



# 6. Audit & Rollback Spec

## 6.1 Audit objective

Every decision, mutation and rollback is observable and reconstructable.

## 6.2 Audit fields

- event_id

- actor

- actor_type

- action_type

- target

- before_state

- after_state

- result

- policy_version

- request_id

- risk level

- visibility

## 6.3 Execution audit chain

1) proposal created

2) validated

3) execution

4) verification

5) post-verification state captured

## 6.4 Rollback graph

- store parent action and dependent actions.

- rollback executes in safe reverse topological order where possible.

## 6.5 Rollback constraints

- rollback requires allowed role.

- rollback requires reason and initiator.

- post-rollback verification mandatory.

## 6.6 Logging

- include why rollback happened and what changed.

- include remediation if dependencies remain unresolved.
