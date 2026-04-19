# Bro-PM

Bro-PM — это MVP-бэкенд для operational PM-агента с жёстким разделением ответственности:

- backend хранит состояние, применяет policy, ведёт audit trail и выполняет изменения безопасно;
- Hermes gateway слушает Telegram/ЛС, общается с людьми и вызывает backend для разрешённых действий;
- в текущем репозитории Hermes-слой не мутирует state напрямую: Bro-PM хранит due actions и command/gateway API, а Hermes использует их как control plane.

Именно для этого проект и существует: чтобы состояние, права, safe-pause, idempotency и rollback не зависели от промптов. В архитектурной спецификации это сформулировано явно: durable state и execution safety должны жить в backend, а Hermes должен возвращать только структурированные suggestions.

Важно: в текущем MVP этот safety/policy-контур уже хорошо покрывает command/report/audit/rollback flow, но ещё не равномерно натянут на все прямые CRUD endpoint'ы. Например, `POST /api/v1/projects`, `POST /api/v1/projects/onboard`, `POST /api/v1/projects/{project_id}/tasks` и `POST /api/v1/projects/{project_id}/goals` сейчас проще: они не проходят через `PolicyEngine` и не пишут такой же command-style audit на каждую мутацию.

Статус проекта сейчас: это честный локально запускаемый MVP, а не финальная целевая архитектура.

## Зачем он нужен

По спецификации Bro-PM задуман как платформа operational PM agent с двумя слоями:

- backend service с durable state, policy, permissions, audit и execution safety;
- Hermes runtime для reasoning, planning, drafting коммуникаций и разбора исключений.

MVP-критерии в планировании тоже очень приземлённые:

- можно onboard'ить один проект;
- можно принять одну цель и разложить её на задачи;
- работают safe-pause и escalation-сценарии;
- можно откатить хотя бы одно действие;
- отчёты видны через Notion-first слой.

То есть Bro-PM сейчас — это не «автономный PM-комбайн», а заземлённая backend-основа под такой продукт.

## Что уже умеет текущий код

Сейчас в репозитории уже реализованы такие сценарии.

### 1. API на FastAPI

Приложение собирается в `src/bro_pm/api/app.py`, поднимает FastAPI и подключает три роутера под префиксом `/api/v1`:

- `/onboarding`
- `/api/v1/projects`
- `/api/v1/commands`
- `/api/v1/gateway`

При старте вызывается `init_db(...)`, поэтому локальная схема инициализируется автоматически.

### 2. Durable-модель данных

В коде уже есть SQLAlchemy-модели:

- `Project`
- `ProjectMembership`
- `Goal`
- `Task`
- `TrackerCredential`
- `AuditEvent`
- `RollbackRecord`
- `PolicyRule`
- `DueAction`

Из полезного:

- у проекта есть `safe_paused`, `timezone`, `visibility`, `metadata`;
- goal'ы привязаны к project;
- task'и могут быть как проектными, так и дочерними к goal;
- audit events поддерживают `idempotency_key`;
- rollback хранится отдельно в `RollbackRecord`;
- `DueAction` хранит очереди outbound-коммуникации для Hermes gateway: канал, получателя, payload, due-at, claim/delivery/ack timestamps и idempotency.

### 3. Onboarding проекта

Для живого MVP теперь есть и UI-путь: `GET /onboarding/`.

Это server-rendered launch page внутри самого сервиса. Она:

- собирает project basics, boss/admin, Yandex Tracker credentials, employee rows, reporting cadence и optional initial goal;
- **не** показывает messenger choice;
- **не** показывает manual capacity;
- выводит capacity из employee rows по правилу `1 employee = 160 monthly capacity units`;
- кладёт non-secret tracker config в `tracker_credentials`, а secret fields хранит там только в masked виде; в `project.metadata` остаётся только non-secret integration config;
- внутри backend bridge автоматически подставляет скрытый `communication_integrations=["telegram"]`, пока communication onboarding не вынесен в отдельный Hermes flow.

`POST /api/v1/projects/onboard` уже делает реальную вертикальную MVP-операцию:

- создаёт проект;
- создаёт memberships для `boss` и `admin`;
- умеет работать через общий onboarding service, который использует и HTML page;
- сохраняет onboarding-метаданные;
- после успешного smoke check ставит durable `DueAction(kind="project_launch_bootstrap")` в gateway outbox для Hermes/runtime handoff;
- кладёт в launch payload project identity, onboarding routing context и либо initial goal summary, либо явный follow-up `capture_initial_goal`;
- прогоняет реальный smoke check только для `yandex_tracker` через текущий backend (`native` или `mcp`);
- пишет audit event `onboard_project`.

Если smoke check падает, код честно:

- ставит проект на `safe_paused`;
- пишет failed audit для onboarding;
- создаёт `draft_boss_escalation` с `requires_approval`.

### 4. Intake цели и декомпозиция на задачи

`POST /api/v1/projects/{project_id}/goals` умеет:

- создать goal;
- сразу создать вложенные task'и из payload;
- по `auto_decompose=true` детерминированно сгенерировать 3 next-step task'а, если явные task'и не переданы;
- вернуть goal вместе с дочерними task'ами.

При этом на уровне модели и БД уже зафиксировано ограничение: для одного проекта может существовать только одна active goal. Для SQLite и PostgreSQL это поддержано partial unique index'ом.

Текущий MVP-правил для auto-decompose ровно такой:

- Bro-PM не вызывает скрытый planner/runtime и не строит «оптимизатор».
- Для новой goal или undecomposed task backend создаёт узкий фиксированный набор фаз:
  - `Clarify scope for <context>`
  - `Execute next slice for <context>`
  - `Verify and report for <context>`
- Assignment считается только из локального durable state:
  - сначала пересчитывается `load_units` по open task'ам;
  - кандидат должен иметь `capacity_units > effective_load`;
  - выбирается actor с минимальным текущим effective load;
  - при равенстве выигрывает тот, у кого больше remaining capacity;
  - при новом равенстве используется лексикографический порядок `actor`;
  - если свободной capacity нет, task остаётся без assignee.

Для уже существующей project-level task появился узкий backend path `POST /api/v1/projects/{project_id}/tasks/{task_id}/decompose`: он создаёт follow-up task'и по тем же правилам и, если source task не привязана к goal, цепляет их к текущей active goal проекта.

Для onboarding capacity-state есть ещё одно текущее ограничение контракта: `team[]` должен быть уникален по паре `(name, owner)`, иначе запрос режется на validation-слое до записи в БД.

UI-path поверх этого контракта строит `team[]` автоматически:

- employee row хранит `employee + function`;
- backend мапит это в `team.name=function`, `team.owner=employee`, `team.capacity=160`.

### 5. Работа с задачами проекта

Есть отдельные endpoint'ы:

- `POST /api/v1/projects/{project_id}/tasks`
- `GET /api/v1/projects/{project_id}/tasks`

То есть локальные task'и можно создавать напрямую через project API и читать списком.

### 6. Командный API с policy и audit

`POST /api/v1/commands` принимает текстовую команду, actor/role и флаги исполнения.

Сейчас Hermes adapter по умолчанию поддерживает только детерминированный парсер следующих форм:

- `pause project <project_id>`
- `resume project <project_id>`
- `create task <title>`
- `close task <task_id>`
- `draft_boss_escalation <message>`

Дальше `CommandService`:

- прогоняет proposal через `PolicyEngine`;
- поддерживает `dry_run`;
- поддерживает `idempotency_key`;
- пишет audit event;
- в некоторых режимах умеет прогонять integration validate/execute.

### 7. Safe-pause и rollback

Safe-pause уже реально работает как защитный стоп в policy engine для command/report/audit/rollback-путей.

При `safe_paused=True` unsafe-операции режутся, а разрешёнными остаются только специальные действия вроде:

- `unpause_project`
- `audit_view`
- `rollback_action`
- `draft_boss_escalation`

Отдельный `POST /api/v1/projects/{project_id}/rollback` уже есть и использует `CommandService.rollback(...)`.

Важно: это описание относится именно к путям, которые реально проходят через `PolicyEngine`. Прямые CRUD endpoint'ы проектов/goal/task'ов в текущем MVP проще и сейчас не используют тот же policy/safe-pause gate.

Важно: в текущем MVP rollback покрывает только минимальную локальную семантику pause/unpause, а не произвольные бизнес-операции.

### 8. Audit API

Уже доступны:

- `GET /api/v1/projects/{project_id}/audit-events`
- `GET /api/v1/projects/{project_id}/audit-events/{audit_event_id}`

Оба endpoint'а защищены policy-проверкой `audit_view`: нужен `role` в query-параметре и заголовок `x-actor-trusted: true`.

Detail endpoint отдаёт sanitized payload, а top-level `detail` берётся из сохранённого audit payload.

### 10. Gateway due-actions API

Новый `/api/v1/gateway` слой нужен не для того, чтобы Bro-PM сам слушал Telegram, а наоборот — чтобы **Hermes gateway** мог забирать из backend готовые due actions и подтверждать доставку.

Сейчас в MVP есть три базовых gateway endpoint'а:

- `POST /api/v1/gateway/due-actions:claim` — Hermes claim'ит готовые к отправке due actions;
- `POST /api/v1/gateway/due-actions/{due_action_id}/delivery` — Hermes подтверждает `delivered`, `failed` или `acked`;
- `POST /api/v1/gateway/events:ingest` — Hermes отправляет нормализованное inbound-событие в Bro-PM и получает disposition, что ему разрешено делать дальше.

Через `events:ingest` backend уже умеет минимум три полезных вещи:
- зафиксировать входящее событие как `ConversationEvent`;
- засчитать acknowledgement по уже отправленному `DueAction`;
- записать минимальный approval reply для `AuditEvent` в состоянии `awaiting_approval`.

То есть Hermes остаётся chat/runtime-слоем, а Bro-PM — durable control plane и для outbound-коммуникации, и для минимального inbound decision path.

### 11. Project report

`POST /api/v1/projects/{project_id}/reports/project` уже строит project report из текущего состояния:

- `summary`
- `kpis`
- `risks`
- `decisions`
- `action_ids`
- `links`
- `publish`

В текущем shipped-поведении report дополнительно делает две вещи для autonomy visibility:

- `risks` собираются не только из audit-событий, но и из durable `DueAction`, если timer уже поставил boss escalation в gateway outbox;
- timer-derived risk/decision entries несут явные `trace_label` и `lineage`, чтобы было видно, от какой автономной эвристики они появились и дошли ли они до internal task creation или Hermes due action.

Что именно уже отражается в report из реально существующих backend-эвристик:

- `executor_overload`
- `idle_executor`
- `stalled_task`
- `commitment_risk`
- `overdue_tasks`
- `boss_escalation`

Для `decisions` report сейчас возвращает не только краткий `summary`, но и:

- `reason`
- `mode`
- `trace_label`
- `lineage`

Для `risks` report сейчас возвращает источник и lineage:

- `source` (`audit_event` или `due_action`)
- `due_action_id` если риск surfaced из gateway outbox
- `trace_label`
- `lineage`

Если `execute_publish=false`, сервис не публикует ничего наружу, а возвращает Notion-ready publish contract со статусом `contract_ready`.

Если `execute_publish=true`, сервис не пишет в Notion напрямую: он ставит Hermes-facing `DueAction(kind="project_report_publish")`, пишет audit для `publish_report` со статусом `queued` и сохраняет idempotent replay-логику в Bro-PM.

Дополнительно в текущем MVP появился реальный timer-actions path:

- onboarding-поле `reporting_cadence` теперь не просто metadata;
- in-process scheduler умеет ставить scheduled project reports для cadence `daily` и `weekly` в Hermes handoff queue;
- каждые 10 минут тот же timer-actions runtime делает autonomous decision review по проектам;
- decision review может:
  - поставить в `DueAction` boss-escalation для последующей доставки через Hermes gateway, если видит повторяющиеся недавние сбои;
  - создать follow-up `create_task`, если есть active goal, но нет открытой работы;
  - создать replan `create_task`, если накопилось достаточно overdue open tasks;
  - создать follow-up `create_task`, если находит executor overload, idle executor with spare capacity, stalled task или commitment/deadline pressure;
- `manual` и неподдерживаемые reporting cadence scheduler осознанно пропускает;
- safe-paused проекты timer actions не трогают;
- dedupe сделан через idempotency keys по окнам времени, а повтор одного и того же decision-эвристического срабатывания дополнительно режется cooldown-проверкой по recent audit history.

## Что здесь пока упрощено или только намечено

Это важный раздел, без маркетинга.

### Hermes сейчас детерминированный по умолчанию

В `src/bro_pm/adapters/hermes_runtime.py` прямо написано, что MVP по умолчанию использует deterministic local parser.

Remote-path пока не реализован:

- адаптер проверяет `BRO_PM_HERMES_REMOTE=true` только если создан с `prefer_remote=True`;
- после этого вызывает `_remote_fallback(...)`;
- `_remote_fallback(...)` сейчас просто бросает `RuntimeError("remote Hermes runtime not enabled")`.

То есть `BRO_PM_HERMES_API_BASE` и `BRO_PM_HERMES_API_KEY` уже заведены в конфиге, но в видимом коде активная удалённая Hermes-интеграция ещё не подключена.

### Интеграции: Yandex Tracker теперь dual-path, остальные пока mostly stub-like

В `src/bro_pm/integrations/__init__.py` зарегистрированы адаптеры:

- `notion`
- `jira`
- `trello`
- `yandex_tracker`
- `telegram`
- `slack`

Сейчас поведение разделено так:

- `notion`, `jira`, `trello`, `telegram` и `slack` остаются lightweight MVP adapters: `validate(...)` проверяет допустимость action и базовые поля, а `execute(...)` в общем случае возвращает synthetic success вроде `"notion executed: create_task"`;
- `yandex_tracker` остаётся **одним** product-level adapter'ом, но внутри умеет два backend path'а для `create_task`:
  - `native` — прямой HTTP-вызов в Yandex Tracker API;
  - `mcp` — stdio MCP tool call через Python MCP SDK;
- backend по умолчанию задаётся env-переменной `BRO_PM_YANDEX_TRACKER_BACKEND` (`native` по умолчанию), а конкретный проект может переопределить его через `project.metadata.integrations.yandex_tracker.backend`;
- queue resolution для обоих backend'ов общая: `payload.queue` -> `project.metadata.integrations.yandex_tracker.queue` -> `BRO_PM_YANDEX_TRACKER_DEFAULT_QUEUE`;
- assisted `create_task` через `/api/v1/commands` и onboarding smoke check используют тот board adapter, который выбран в `project.metadata.onboarding.board_integration`.

То есть живой внешний вызов для `yandex_tracker create_task` теперь может идти либо напрямую в HTTP API, либо через MCP, без отдельного integration name вроде `yandex_tracker_mcp`.

### Assisted execution узкий

Командный API звучит шире, чем он реально работает.

Сейчас важно помнить:

- локальная мутация через `_apply_action(...)` покрывает только `pause_project` и `unpause_project`;
- `close_task` — это approval/audit flow, а не полноценное закрытие локальной task-записи;
- `validate_integration` и `execute_integration` сейчас поддерживают только `create_task`;
- assisted `create_task` через `/api/v1/commands` идёт через интеграционный слой и audit, не создаёт локальную запись `Task` в таблице проекта и теперь выбирает board adapter по `project.metadata.onboarding.board_integration` с legacy fallback на `notion`.

Если нужен локальный task в БД, сейчас для этого надо использовать project endpoint `POST /api/v1/projects/{project_id}/tasks` или goal intake с вложенными задачами.

### Текущий локальный runtime != целевая архитектура

В архитектурной спецификации целевая схема описана так:

- backend service;
- worker sidecar;
- PostgreSQL как authoritative state;
- Redis для locks/dedupe/scheduling.

Но текущий локально запускаемый код в этом репозитории — это:

- один FastAPI-процесс;
- SQLAlchemy;
- SQLite по умолчанию (`sqlite:///./bro_pm.db`);
- без видимого worker sidecar;
- без Redis-клиента в зависимостях;
- без отдельно поднятой очереди или event ingestion pipeline.

Именно так README и стоит воспринимать: спецификация показывает, куда проект целится, а код — что уже реально работает сейчас.

## Основные компоненты

### API слой

Файлы:

- `src/bro_pm/api/app.py`
- `src/bro_pm/api/v1/projects.py`
- `src/bro_pm/api/v1/commands.py`

Здесь живут HTTP endpoint'ы, dependency на DB session и wiring FastAPI.

### Domain / services

Файлы:

- `src/bro_pm/services/command_service.py`
- `src/bro_pm/services/reporting_service.py`

Что делают:

- `CommandService` — parse/execution flow, policy, idempotency, audit, rollback;
- `ReportingService` — сбор project report, Notion-ready publish contract, Hermes handoff queueing, publish audit, replay idempotency.

### Policy слой

Файл:

- `src/bro_pm/policy.py`

Это явный rule-based policy gate, чтобы доступ и ограничения не были спрятаны в prompt-логике.

### Hermes adapter

Файл:

- `src/bro_pm/adapters/hermes_runtime.py`

Сейчас это thin wrapper вокруг детерминированного parser'а команд.

### Persistence / schema

Файлы:

- `src/bro_pm/models.py`
- `src/bro_pm/database.py`

Что уже есть:

- SQLAlchemy engine/session;
- `init_db()`;
- `create_all()`;
- локальные legacy-upgrade helper'ы;
- проверка partial unique index для active goal.

### Integrations

Файл:

- `src/bro_pm/integrations/__init__.py`

Здесь лежит текущий registry интеграций и MVP-адаптеры.

### Документы продукта и архитектуры

Полезные файлы:

- `docs/architecture/system-architecture-spec.md`
- `docs/planning/delivery-backlog-and-epic-breakdown.md`
- `docs/product/project-onboarding-spec.md`
- `docs/product/reporting-and-notion-ia-spec.md`

## Конфигурация и env-переменные, которые реально видны в коде

В `src/bro_pm/config.py` и Hermes adapter сейчас видны такие knobs:

- `BRO_PM_DATABASE_URL`
  - default: `sqlite:///./bro_pm.db`
  - реально используется для engine/session.

- `BRO_PM_HERMES_API_BASE`
  - объявлен в конфиге;
  - в текущем коде полезен как задел под будущий remote Hermes path.

- `BRO_PM_HERMES_API_KEY`
  - объявлен в конфиге;
  - в текущем коде тоже пока задел, а не активный рабочий путь.

- `BRO_PM_MAX_PAYLOAD_BYTES`
  - объявлен в конфиге;
  - в видимых обработчиках этого README явное enforcement не используется.

- `BRO_PM_HERMES_REMOTE`
  - проверяется Hermes adapter'ом перед попыткой remote fallback;
  - сам remote fallback пока не реализован.

- `BRO_PM_YANDEX_TRACKER_API_BASE`
  - default: `https://api.tracker.yandex.net/v2`;
  - используется native backend'ом `yandex_tracker create_task`.

- `BRO_PM_YANDEX_TRACKER_BACKEND`
  - default: `native`;
  - допустимые значения: `native` или `mcp`;
  - задаёт глобальный backend по умолчанию для `yandex_tracker`, но проект может переопределить его через `metadata.integrations.yandex_tracker.backend`.

- `BRO_PM_YANDEX_TRACKER_TOKEN`
  - обязателен для native `yandex_tracker create_task`.

- `BRO_PM_YANDEX_TRACKER_AUTH_PREFIX` / `BRO_PM_YANDEX_TRACKER_AUTH_SCHEME`
  - задаёт префикс для `Authorization` header;
  - default: `OAuth`.

- `BRO_PM_YANDEX_TRACKER_ORG_HEADER_NAME`
  - default: `X-Org-ID`;
  - позволяет переключить имя org header под конкретный runtime.

- `BRO_PM_YANDEX_TRACKER_ORG_ID`
  - обязателен для native `yandex_tracker create_task`.

- `BRO_PM_YANDEX_TRACKER_DEFAULT_QUEUE`
  - optional fallback queue для обоих backend'ов `yandex_tracker create_task`;
  - если у проекта есть `metadata.integrations.yandex_tracker.queue`, он имеет приоритет над env default.

- `BRO_PM_YANDEX_TRACKER_MCP_COMMAND`
  - executable для stdio MCP server process;
  - обязателен, если выбран backend `mcp`.

- `BRO_PM_YANDEX_TRACKER_MCP_ARGS_JSON`
  - optional JSON array строк для argv MCP server'а;
  - пример: `["tracker-mcp"]`.

- `BRO_PM_YANDEX_TRACKER_MCP_ENV_JSON`
  - optional JSON object со string->string env overrides для MCP server'а.

- `BRO_PM_YANDEX_TRACKER_MCP_CWD`
  - optional working directory для MCP server process.

- `BRO_PM_YANDEX_TRACKER_MCP_TOOL_NAME`
  - имя MCP tool для создания issue;
  - обязателен, если выбран backend `mcp`;
  - вынесен в конфиг, потому что разные public server'ы используют разные имена вроде `issue_create` или `create_issue`.

- `BRO_PM_YANDEX_TRACKER_MCP_TIMEOUT_SECONDS`
  - timeout stdio MCP tool call;
  - default: `45`.

Пример project metadata override для MCP backend:

```json
{
  "integrations": {
    "yandex_tracker": {
      "backend": "mcp",
      "queue": "OPS"
    }
  },
  "onboarding": {
    "board_integration": "yandex_tracker"
  }
}
```

Дополнительно в коде фиксирован доверенный заголовок:

- `x-actor-trusted`

Без него policy-прослойка для privileged paths будет считать актора недоверенным.

## Локальный запуск

Требование по Python: `>=3.11`.

Самый прямой локальный сценарий:

```bash
cd /home/olegb/projects/bro-pm
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
export BRO_PM_DATABASE_URL="sqlite:///./bro_pm.db"
python -m uvicorn bro_pm.api.app:app --reload
```

После старта приложение само вызовет `init_db()` и создаст локальную схему.

Если нужен запуск без editable install, используй явный app-dir / `PYTHONPATH`:

```bash
PYTHONPATH=src python -m uvicorn bro_pm.api.app:app --reload
```

## Запуск через Docker

Для MVP самый короткий путь теперь такой:

```bash
cd /home/olegb/projects/bro-pm
docker compose up --build
```

Что это поднимет:

- контейнер с реальным FastAPI entrypoint `bro_pm.api.app:app`;
- `uvicorn` на `0.0.0.0:8000`;
- SQLite через `BRO_PM_DATABASE_URL=sqlite:////data/bro_pm.db`;
- named volume `bro_pm_data`, чтобы demo-данные переживали перезапуск контейнера.

После старта сервис будет доступен на:

```text
http://127.0.0.1:8000/
```

Полезные команды:

```bash
docker compose up --build -d
docker compose logs -f
docker compose down
```

Если нужен чистый локальный state SQLite для demo, можно удалить volume и поднять сервис заново:

```bash
docker compose down -v
docker compose up --build
```

## Что уже используется в локальном setup

По `pyproject.toml` и `requirements.txt` сейчас реально видны такие зависимости:

runtime:

- fastapi
- uvicorn[standard]
- sqlalchemy
- pydantic
- alembic
- python-dotenv
- mcp

dev/test:

- pytest
- httpx
- pytest-asyncio

Отдельно важно: наличие `alembic` в зависимостях не означает, что локальный MVP уже завязан на полноценный migration workflow. В текущем visible startup path схема поднимается через `init_db()` и `Base.metadata.create_all(...)` плюс небольшие legacy-upgrade helper'ы в `database.py`.

## Ключевые API endpoint'ы, которые реально есть сейчас

### Projects

`GET /onboarding/`
- server-rendered onboarding page внутри сервиса.

`POST /onboarding/`
- HTML submit path для launch onboarding;
- не требует от пользователя передавать `communication_integrations` или ручную capacity;
- сохраняет tracker credentials в dedicated storage и возвращает HTML summary/error state.

`GET /api/v1/projects`
- список проектов.

`POST /api/v1/projects`
- создаёт проект;
- принимает `name`, `slug`, `description`, `timezone`, `created_by`, `visibility`, `safe_paused`, `metadata`.

`POST /api/v1/projects/onboard`
- полный MVP-onboarding;
- принимает `boss`, `admin`, `reporting_cadence`, `communication_integrations`, `board_integration`, `team` и базовые поля проекта;
- создаёт memberships, gate checks, smoke check, onboarding audit и durable launch/bootstrap due action для Hermes gateway;
- остаётся raw API контрактом, а web page использует backend bridge над ним.

### Goals and tasks

`POST /api/v1/projects/{project_id}/goals`
- создаёт goal;
- умеет принять массив вложенных `tasks`.

`POST /api/v1/projects/{project_id}/tasks`
- создаёт локальный task напрямую в проекте.

`GET /api/v1/projects/{project_id}/tasks`
- возвращает task'и проекта.

### Commands

`POST /api/v1/commands`
- принимает:
  - `command_text`
  - `project_id` (optional)
  - `actor`
  - `role`
  - `idempotency_key` (optional)
  - `dry_run`
  - `validate_integration`
  - `execute_integration`
- использует header `x-actor-trusted`.

### Audit

`GET /api/v1/projects/{project_id}/audit-events?role=<role>`
- список audit events проекта;
- требует `x-actor-trusted: true`.

`GET /api/v1/projects/{project_id}/audit-events/{audit_event_id}?role=<role>`
- detail-view audit event;
- тоже требует `x-actor-trusted: true`.

### Reports and rollback

`POST /api/v1/projects/{project_id}/reports/project`
- принимает `actor`, `role`, optional `execute_publish`, optional `idempotency_key`;
- для policy-проверяемого пути нужен header `x-actor-trusted: true`;
- возвращает project report и publish block; при `execute_publish=true` publish block описывает queued Hermes ownership, а не backend Notion execution.

`BRO_PM_TIMER_ACTIONS_ENABLED`
- default: `true` for the default live app object;
- automatic scheduler startup is still suppressed for explicit `create_app(database_url=...)` test-style app construction unless `enable_scheduler=True` is passed;
- includes the in-process scheduler for timer actions in live runtime.

`BRO_PM_TIMER_ACTIONS_POLL_INTERVAL_SECONDS`
- default: `60`;
- задаёт polling interval для timer scheduler.

`POST /api/v1/projects/{project_id}/rollback`
- принимает `actor`, `role`, `audit_event_id`, `reason`;
- для успешного privileged rollback нужен header `x-actor-trusted: true`;
- возвращает результат rollback и `rollback_record_id`.

## Практический walkthrough

Ниже — живой MVP-флоу, который соответствует текущим endpoint'ам и моделям.

Сервер:

```bash
cd /home/olegb/projects/bro-pm
source .venv/bin/activate
export BRO_PM_DATABASE_URL="sqlite:///./bro_pm.db"
python -m uvicorn bro_pm.api.app:app --reload
```

### 1. Onboard проекта

Если нужен живой UI, открой в браузере:

```text
http://127.0.0.1:8000/
```

Корневой путь сейчас специально ведёт на demo entrypoint `/onboarding/`, чтобы сервис не открывался 404 на первом запуске.

Что делает page flow:

- собирает tracker credentials по выбранному provider;
- пишет non-secret provider config в `metadata.integrations.<provider>`;
- пишет non-secret provider config в `tracker_credentials`, а secret fields маскирует;
- добавляет employee rows и считает `160` capacity units на каждого;
- при желании создаёт первый active goal сразу из формы.

Если нужен raw API path, он остаётся таким:

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/projects/onboard \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Project Nova",
    "slug": "project-nova-local",
    "description": "локальная проверка MVP",
    "timezone": "UTC",
    "commitment_due_at": "2026-05-01T12:00:00Z",
    "boss": "olga",
    "admin": "alice",
    "reporting_cadence": "weekly",
    "communication_integrations": ["slack"],
    "board_integration": "notion",
    "team": [
      {
        "name": "operations",
        "owner": "alice",
        "capacity": 3
      }
    ]
  }'
```

Что произойдёт сейчас:

- проект создастся;
- у проекта сохранится `commitment_due_at` как MVP commitment target;
- у него появятся memberships owner/admin;
- из onboarding `team[]` создадутся durable `executor_capacity_profiles` с `capacity_units` и начальным `load_units = 0`;
- пройдёт smoke check через выбранную board integration; если выбрать `yandex_tracker`, это будет `create_task` через backend, выбранный env или project metadata;
- сразу после успешного onboarding в gateway outbox появится `project_launch_bootstrap` due action для Hermes/runtime delivery;
- если initial goal не передавался, payload due action явно потребует first-goal follow-up;
- в ответе будут `gate_checks`, `smoke_check` и `status`.

Скопируй `project.id` из ответа — он нужен дальше.

### 2. Добавить active goal с вложенными задачами

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/projects/<PROJECT_ID>/goals \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "Deliver first onboarding milestone",
    "description": "Разложить первую цель на исполнимые шаги",
    "status": "active",
    "tasks": [
      {
        "title": "Design onboarding plan",
        "description": "Описать первый план работ",
        "status": "todo",
        "priority": "high",
        "last_progress_at": "2026-04-18T10:15:00Z"
      },
      {
        "title": "Confirm owners",
        "description": "Подтвердить владельцев первого этапа",
        "status": "done",
        "priority": "medium"
      }
    ]
  }'
```

`Goal` теперь тоже может нести `commitment_due_at`, а `Task` сохраняет durable `last_progress_at` для stall heuristics следующего slice.

### 3. Посмотреть capacity/load профили команды

```bash
curl -sS http://127.0.0.1:8000/api/v1/projects/<PROJECT_ID>/capacity-profiles
```

Ты увидишь нормализованные capacity profile записи по участникам команды с `capacity_units` и текущим durable `load_units`.

### 4. Посмотреть project tasks

```bash
curl -sS http://127.0.0.1:8000/api/v1/projects/<PROJECT_ID>/tasks
```

Ты увидишь две task'и, привязанные к goal.

### 5. Построить project report

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/projects/<PROJECT_ID>/reports/project \
  -H 'Content-Type: application/json' \
  -H 'x-actor-trusted: true' \
  -d '{
    "actor": "alice",
    "role": "admin"
  }'
```

В текущем MVP это вернёт:

- `summary`
- KPI по task'ам/goal'ам/audit
- `decisions`
- `links`
- `publish.status = "contract_ready"`

То есть publish contract уже собран, но внешний publish по умолчанию ещё не исполнялся.

Если вызвать этот же endpoint с `execute_publish=true`, Bro-PM сохранит audit/idempotency и поставит Hermes due action для knowledge-writing вместо прямой backend-публикации в Notion.

Если нужен реальный timer-actions runtime, оставь live app scheduler включённым:

```bash
export BRO_PM_TIMER_ACTIONS_ENABLED=true
export BRO_PM_TIMER_ACTIONS_POLL_INTERVAL_SECONDS=60
python -m uvicorn bro_pm.api.app:app --reload
```

Тогда live API process будет:
- периодически запускать scheduled report publishing для проектов с `reporting_cadence = daily|weekly`;
- раз в 10-минутное decision window делать autonomous decision review и, если срабатывает эвристика, проводить дальнейшее действие через существующий `CommandService` / policy / audit flow.

### 5. Поставить проект на safe-pause через command API

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/commands \
  -H 'Content-Type: application/json' \
  -H 'x-actor-trusted: true' \
  -d '{
    "command_text": "pause project <PROJECT_ID>",
    "project_id": "<PROJECT_ID>",
    "actor": "alice",
    "role": "admin"
  }'
```

Это важный момент: команда реально меняет локальный state именно для pause/unpause.

### 6. Посмотреть audit trail

```bash
curl -sS 'http://127.0.0.1:8000/api/v1/projects/<PROJECT_ID>/audit-events?role=operator' \
  -H 'x-actor-trusted: true'
```

Если нужен detail конкретного события:

```bash
curl -sS 'http://127.0.0.1:8000/api/v1/projects/<PROJECT_ID>/audit-events/<AUDIT_ID>?role=operator' \
  -H 'x-actor-trusted: true'
```

## Как гонять тесты

Полный набор тестов:

```bash
cd /home/olegb/projects/bro-pm
source .venv/bin/activate
PYTHONPATH=src python3 -m pytest -q
```

Если нужен быстрый прогон onboarding/UI и основных API/MVP сценариев:

```bash
PYTHONPATH=src python3 -m pytest -q tests/test_onboarding_web_ui.py tests/test_project_onboarding_api.py tests/test_mvp_e2e_flow.py tests/test_api.py
```

Эти тесты уже покрывают важные живые флоу:

- onboarding;
- goal intake + task decomposition;
- project report;
- command API;
- audit access;
- rollback;
- idempotency edge cases.

## Коротко про честные границы MVP

Если в одном абзаце, то сейчас Bro-PM — это:

- уже полезный backend-каркас с policy, audit, safe-pause, report и rollback-скелетом;
- уже запускаемый локально на FastAPI + SQLite;
- уже с узким реальным timer-actions MVP:
  - scheduled report publishing;
  - 10-minute autonomous decision review для следующего действия, максимум одного autonomous action на project в одно decision window;
- уже покрытый API- и e2e-тестами;
- но пока ещё с детерминированным Hermes adapter'ом по умолчанию;
- с live Yandex Tracker `create_task`, но с остальными integration adapters по-прежнему mostly stub-like;
- пока ещё без полноценно реализованной целевой связки Postgres + Redis + worker sidecar.

Если нужен production-shaped контур — смотри архитектурную спекацию.
Если нужен честный текущий статус кода — ориентируйся на этот README и на тесты в `tests/`.
