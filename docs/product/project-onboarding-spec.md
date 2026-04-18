# Bro-PM — Project Onboarding Spec



# 8. Project Onboarding Spec

## 8.1 Onboarding goals

Bring a project from zero state to active, constrained autonomy.

## 8.2 Procedure

1. create project identity and timezone

2. assign boss/admin and memberships

3. load default policy and reporting cadence

4. connect integrations
   - MVP board integrations now include `notion`, `jira`, `trello`, and `yandex_tracker`
   - onboarding smoke check must use the selected board adapter
   - if the selected board adapter is `yandex_tracker`, onboarding runs `create_task` through the single Yandex adapter
   - Yandex backend defaults to `BRO_PM_YANDEX_TRACKER_BACKEND` and can be overridden per project via `metadata.integrations.yandex_tracker.backend`
   - Yandex queue resolution prefers `metadata.integrations.yandex_tracker.queue` and falls back to `BRO_PM_YANDEX_TRACKER_DEFAULT_QUEUE`
   - when Yandex backend is `mcp`, Bro-PM uses stdio MCP with configured command/tool settings instead of direct HTTP

5. map team ownership and capacity
   - onboarding now persists normalized `ExecutorCapacityProfile` rows for each `team[]` entry
   - each profile stores `team_name`, `actor`, `capacity_units`, `load_units`, and `source="onboarding"`
   - duplicate `team[]` entries by `(name, owner)` are rejected at validation time instead of leaking DB integrity failures

6. run synthetic action smoke check

7. initialize autonomous reporting cadence
   - `reporting_cadence` now has a live MVP effect for `daily` and `weekly`
   - the default live app can run an in-process scheduler that periodically publishes project reports through the existing `publish_report` execution path
   - `manual` and unsupported cadence values are intentionally skipped by the scheduler

8. confirm first goal intake

## 8.3 Gate checks

- policy attachment present

- at least one communication integration available

- board sync healthy

- safe pause default OFF for active execution

## 8.4 Failure mode

If onboarding fails, mark project paused and escalate to admin.
