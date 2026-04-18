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

- Risk report

- Rollback report

- Learning change report

## 10.4 Data model for reports

Each report contains summary, KPIs, risks, decisions, action IDs, and links.

## 10.5 Retention

Keep long-horizon summaries in Notion and raw operational logs in DB.
