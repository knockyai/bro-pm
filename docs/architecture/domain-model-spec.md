# Bro-PM — Domain Model Spec



# 2. Domain Model Spec

## 2.1 Core entities

- `Project`

- `ProjectMembership`

- `Goal`

- `Task`

- `ExecutorCapacityProfile`

- `TaskDependency`

- `TaskAssignment`

- `ConversationEvent`

- `PolicyVersion`

- `AuditEvent`

- `ActionExecution`

- `RollbackAction`

- `RiskSignal`

- `SafePauseState`

- `HeuristicState`

- `IntegrationCredentialRef`

## 2.2 Task lifecycle

States: `backlog`, `planned`, `in_progress`, `review`, `done`, `closed`, `blocked`, `cancelled`.

Allowed transitions:

- backlog -> planned

- planned -> in_progress

- in_progress -> review or blocked

- review -> done

- done -> closed

- any non-final -> cancelled

Each transition stores reason, actor, previous owner, and context.

## 2.3 Goal lifecycle

States: `draft`, `active`, `achieved`, `failed`, `archived`.

- Only one active goal per project by default.

- Failed goals require risk/evidence and escalation entry.

- MVP autonomy state may attach `commitment_due_at` to both `Project` and `Goal`.

## 2.3.1 MVP autonomy state

- `Project.commitment_due_at` stores the current committed project-level target date.

- `Goal.commitment_due_at` stores the current committed goal-level target date.

- `Task.last_progress_at` stores the latest durable progress heartbeat usable by timer heuristics.

- `ExecutorCapacityProfile` stores normalized `team_name`, `actor`, `capacity_units`, `load_units`, and source metadata per project.

## 2.4 Policy lifecycle

- Policy versions are append-only.

- Each change includes actor, previous version, change reason, validation result.

## 2.5 Integrity constraints

- Unique indices for slugs and ids.

- Foreign-key constraints for dependencies and memberships.

- `ActionExecution` references immutable `PolicyVersion` and `SafePauseState` snapshot.

## 2.6 State partitioning

All entities are scoped by `project_id`, with visibility filters based on role and actor.

## 2.7 Rollback model

`RollbackAction` references original action and includes dependency list and optional corrective follow-up.
