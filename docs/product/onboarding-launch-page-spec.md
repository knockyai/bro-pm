# Bro-PM Onboarding Launch Page Spec

## 1. Shipped surface

Bro-PM now exposes a real server-rendered onboarding page at `GET /onboarding/`.

This page is the MVP launch surface for creating a project, wiring the selected tracker, storing tracker credential metadata outside `project.metadata`, seeding ownership/capacity, and optionally creating the first active goal.

It is intentionally implemented as a FastAPI-native HTML form with minimal JavaScript. There is no separate frontend stack in this slice.

## 2. Outcome

Successful submission must leave the system with:

1. a created project,
2. owner/admin memberships,
3. selected board integration metadata,
4. persisted tracker config plus masked secret markers in dedicated backend storage,
5. seeded executor capacity profiles,
6. reporting cadence in onboarding metadata,
7. a passed tracker smoke check,
8. an optional first active goal with optional auto-generated tasks.

## 3. URL and submit behavior

- `GET /onboarding/` renders the launch form.
- `POST /onboarding/` validates the form and runs the onboarding flow.
- On success, the page returns a success summary in HTML.
- On validation or smoke-check failure, the page re-renders with concrete errors.

The backend path is shared with the raw onboarding API so the project creation, smoke check, audit event, and failure semantics stay aligned.

## 4. Form sections

### 4.1 Project basics

Required fields:

- `name`
- `slug`
- `description`
- `timezone`
- `commitment_due_at` (optional but visible)

The page keeps visibility internal and does not expose raw metadata editing.

### 4.2 Launch ownership

Required fields:

- `boss`
- `admin`

Result:

- `boss` becomes `owner`
- `admin` becomes `admin` unless it is the same actor as `boss`

### 4.3 Tracker selection

Required field:

- `board_integration`

Supported values:

- `yandex_tracker`

The launch page is intentionally limited to the one real product-level adapter in this MVP. Placeholder adapters like `jira`, `trello`, and `notion` remain API/backend concepts, not UI launch targets.

### 4.4 Tracker credentials

The page collects provider-specific tracker data and splits it into:

- non-secret config stored in `project.metadata.integrations.<provider>`
- masked secret markers stored in `tracker_credentials`

Current required fields for the live UI provider:

- `yandex_tracker`: `org_id`, `queue`, `token`

For Yandex Tracker, the onboarding smoke check uses the submitted token directly for that request. Later assisted task creation reuses persisted non-secret config, but token resolution falls back to direct request credentials or runtime settings rather than DB plaintext.

## 5. Employees and capacity

The UI does **not** expose manual capacity input.

Instead it collects employee rows with:

- employee name
- team / function

Rules:

- at least one employee row is required,
- each row must have both fields,
- rows must be unique by `(function, employee)`.

Derivation model in this MVP:

- one employee row becomes one `team[]` onboarding entry,
- `team.name = function`,
- `team.owner = employee`,
- `team.capacity = 160`.

That means each employee contributes the default baseline of `160` monthly capacity units.

The page also stores a normalized `onboarding.employees[]` snapshot for operator visibility.

## 6. Reporting cadence

Required field:

- `reporting_cadence`

Visible options:

- `daily`
- `weekly`
- `manual`

Default:

- `weekly`

## 7. Initial goal block

Optional fields:

- `goal_title`
- `goal_description`
- `goal_commitment_due_at`
- `goal_auto_decompose`
- `goal_max_generated_tasks`

Behavior:

- if `goal_title` is empty, onboarding does not create a goal,
- if `goal_title` is provided, onboarding creates one active goal,
- if `goal_auto_decompose=true`, the backend generates up to `goal_max_generated_tasks` initial tasks using the existing planner path.

## 8. Intentionally hidden from UI

The page does **not** expose:

- messenger / communication selection,
- Telegram vs Slack choice,
- manual capacity,
- low-level Yandex MCP command/env settings,
- raw runtime/debug flags.

Current backend bridge:

- the page injects the hidden internal communication default `["telegram"]` to satisfy the current onboarding contract without exposing messenger choice.

## 9. Persistence model

### 9.1 Project metadata

`project.metadata` stores only non-secret onboarding and integration data, including:

- `onboarding.status`
- `onboarding.reporting_cadence`
- `onboarding.communication_integrations`
- `onboarding.board_integration`
- derived `onboarding.team`
- `onboarding.employees`
- smoke-check and gate-check results
- non-secret provider config under `integrations.<provider>`

### 9.2 Tracker credentials

Tracker credential rows are stored in the dedicated `tracker_credentials` table:

- `project_id`
- `provider`
- `config`
- `secrets`

This slice isolates tracker secrets from `project.metadata` but does not introduce external vault integration yet.
Secret fields in that table are masked placeholders only and are not reusable bearer tokens.

## 10. Failure semantics

If tracker smoke check fails:

- onboarding fails closed,
- the project is marked `safe_paused = true`,
- onboarding metadata is written with failed smoke-check status,
- an `onboard_project` failed audit event is created,
- a `draft_boss_escalation` audit event is created.

## 11. Validation summary

Minimum valid launch payload from the page requires:

- complete project basics,
- `boss` and `admin`,
- a supported board integration,
- complete provider-specific credential fields,
- at least one valid employee row,
- a valid reporting cadence.

Additional validation:

- `slug` must pass the existing project schema rules,
- `timezone` must be a valid IANA timezone,
- provider-specific required fields must be non-empty,
- duplicate employee ownership rows are rejected,
- goal fields are validated through the existing goal schema when present.
