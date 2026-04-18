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
   - if the selected board adapter is `yandex_tracker`, onboarding runs a live `create_task` call using Bro-PM runtime env config
   - Yandex queue resolution prefers `metadata.integrations.yandex_tracker.queue` and falls back to `BRO_PM_YANDEX_TRACKER_DEFAULT_QUEUE`

5. map team ownership and capacity

6. run synthetic action smoke check

7. confirm first goal intake

## 8.3 Gate checks

- policy attachment present

- at least one communication integration available

- board sync healthy

- safe pause default OFF for active execution

## 8.4 Failure mode

If onboarding fails, mark project paused and escalate to admin.
