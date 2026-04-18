# Bro-PM — Reporting & Notion Information Architecture Spec



# 10. Reporting & Notion Information Architecture Spec

## 10.1 Targets

MVP visibility primarily through Notion, with audit-first structure.

## 10.2 Notion IA

- Workspace root: Bro-PM

- Subpages: Projects, Audit, Policies, Reports, Heuristics

- Project page contains linked docs and current status blocks

## 10.3 Reports

- Sprint report

- Project report
  - current MVP also supports timer-driven autonomous publishing for `daily` and `weekly` cadences via an in-process scheduler
  - failure escalation from the 10-minute decision timer is queued as a durable `DueAction` for Hermes gateway delivery instead of pretending Bro-PM itself sends Telegram messages

- Risk report

- Rollback report

- Learning change report

## 10.4 Data model for reports

Each report contains summary, KPIs, risks, decisions, action IDs, and links.

Current shipped `project_report` behavior:

- `risks` may come from either `AuditEvent` or durable `DueAction` state, exposed via `source`.
- Timer-derived entries preserve `trace_label` so the originating heuristic stays visible in the report.
- `decisions` include `reason`, `mode`, and `lineage` when that context exists in stored audit payloads.
- `risks` include `due_action_id` and `lineage` when the backend queued Hermes delivery instead of executing an internal task.

Current autonomy/risk kinds surfaced by project report:

- `executor_overload`
- `idle_executor`
- `stalled_task`
- `commitment_risk`
- `overdue_tasks`
- `boss_escalation`

## 10.5 Retention

Keep long-horizon summaries in Notion and raw operational logs in DB.
