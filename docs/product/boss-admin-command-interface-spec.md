# Bro-PM — Boss/Admin Command Interface Spec



# 7. Boss/Admin Command Interface Spec

## 7.1 Inputs

- Command text in trusted chat

- Structured admin API endpoints

## 7.2 Command categories

- Goal operations

- Project operations

- Policy operations

- Safe pause controls

- Rollback controls

- Reports

## 7.3 Command lifecycle

- parse + normalize

- validate identity and scope

- ask clarifying questions if ambiguous

- execute via Hermes orchestration only when safe

## 7.4 Response contract

Every command response includes:

- outcome

- policy impact

- next step

- associated action ids

- any blockers

## 7.5 Authorization

- boss/admin boundaries are strict and testable.

- every high-impact command is logged to audit.
