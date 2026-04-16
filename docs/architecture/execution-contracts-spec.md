# Bro-PM — Execution Contracts Spec



# 3. Execution Contracts Spec

## 3.1 Contract rule

Hermes must always return machine-validated JSON, never raw side-effect text.

## 3.2 Hermes request

- `request_id`

- `job_type`

- `project_id`, `goal_id`

- `policy_version`

- `policy_digest`

- `task_graph_snapshot`

- `recent_events`

- `constraints`

- `required_outputs`

Valid job types:

- interpret_boss_command

- build_initial_plan

- replan_after_dependency_break

- choose_assignee

- draft_executor_ping

- draft_boss_escalation

- draft_action_explanation

- evaluate_rollback_need

## 3.3 Hermes response

- `proposals` array

- `explanations`

- `risk_level`

- `safe_pause`

- `required_clarification`

- `clarification_questions`

- `confidence`

## 3.4 Proposal schema

- `action_idempotency_key`

- `action_type`

- `target_refs`

- `payload`

- `expected_verification`

- `rollback_candidate`

- `visibility`

## 3.5 Validator output

- reject invalid schema early

- map severity before execution

- if ambiguous, return clarification request rather than action

## 3.6 Rollback request schema

- original action reference

- reason

- initiator

- dry_run flag

- post_rollback_checks
