# Bro-PM

Bro-PM — это MVP-бэкенд для operational PM-агента с жёстким разделением ответственности:

- backend хранит состояние, применяет policy, ведёт audit trail и выполняет изменения безопасно;
- Hermes-слой в текущем репозитории отвечает только за разбор команд в структурированные предложения, а не за прямую мутацию состояния.

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

Приложение собирается в `src/bro_pm/api/app.py`, поднимает FastAPI и подключает два роутера под префиксом `/api/v1`:

- `/api/v1/projects`
- `/api/v1/commands`

При старте вызывается `init_db(...)`, поэтому локальная схема инициализируется автоматически.

### 2. Durable-модель данных

В коде уже есть SQLAlchemy-модели:

- `Project`
- `ProjectMembership`
- `Goal`
- `Task`
- `AuditEvent`
- `RollbackRecord`
- `PolicyRule`

Из полезного:

- у проекта есть `safe_paused`, `timezone`, `visibility`, `metadata`;
- goal'ы привязаны к project;
- task'и могут быть как проектными, так и дочерними к goal;
- audit events поддерживают `idempotency_key`;
- rollback хранится отдельно в `RollbackRecord`.

### 3. Onboarding проекта

`POST /api/v1/projects/onboard` уже делает реальную вертикальную MVP-операцию:

- создаёт проект;
- создаёт memberships для `boss` и `admin`;
- сохраняет onboarding-метаданные;
- прогоняет synthetic smoke check через выбранную board integration;
- пишет audit event `onboard_project`.

Если smoke check падает, код честно:

- ставит проект на `safe_paused`;
- пишет failed audit для onboarding;
- создаёт `draft_boss_escalation` с `requires_approval`.

### 4. Intake цели и декомпозиция на задачи

`POST /api/v1/projects/{project_id}/goals` умеет:

- создать goal;
- сразу создать вложенные task'и из payload;
- вернуть goal вместе с дочерними task'ами.

При этом на уровне модели и БД уже зафиксировано ограничение: для одного проекта может существовать только одна active goal. Для SQLite и PostgreSQL это поддержано partial unique index'ом.

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

### 9. Project report

`POST /api/v1/projects/{project_id}/reports/project` уже строит project report из текущего состояния:

- `summary`
- `kpis`
- `risks`
- `decisions`
- `action_ids`
- `links`
- `publish`

Если `execute_publish=false`, сервис не публикует ничего наружу, а возвращает Notion-ready publish contract со статусом `contract_ready`.

Если `execute_publish=true`, сервис проходит через publish path и пишет audit для `publish_report`, включая idempotent replay-логику.

## Что здесь пока упрощено или только намечено

Это важный раздел, без маркетинга.

### Hermes сейчас детерминированный по умолчанию

В `src/bro_pm/adapters/hermes_runtime.py` прямо написано, что MVP по умолчанию использует deterministic local parser.

Remote-path пока не реализован:

- адаптер проверяет `BRO_PM_HERMES_REMOTE=true` только если создан с `prefer_remote=True`;
- после этого вызывает `_remote_fallback(...)`;
- `_remote_fallback(...)` сейчас просто бросает `RuntimeError("remote Hermes runtime not enabled")`.

То есть `BRO_PM_HERMES_API_BASE` и `BRO_PM_HERMES_API_KEY` уже заведены в конфиге, но в видимом коде активная удалённая Hermes-интеграция ещё не подключена.

### Интеграции пока MVP/stub-like

В `src/bro_pm/integrations/__init__.py` зарегистрированы адаптеры:

- `notion`
- `jira`
- `trello`
- `yandex_tracker`
- `telegram`
- `slack`

Но это именно stub-слой текущего MVP:

- `validate(...)` проверяет допустимость action и базовые поля;
- `execute(...)` в общем случае просто возвращает synthetic success вроде `"notion executed: create_task"`;
- `yandex_tracker` уже поддерживается как board integration для onboarding и assisted `create_task`, но пока на том же MVP/stub-уровне, а не как полноценный live Yandex API client.

То есть onboarding smoke check и publish flow уже проходят через integration boundary, но по умолчанию не означают живой внешний API-вызов к реальному Notion/Jira/Trello.

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
- `ReportingService` — сбор project report, publish contract, publish audit, replay idempotency.

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

## Что уже используется в локальном setup

По `pyproject.toml` и `requirements.txt` сейчас реально видны такие зависимости:

runtime:

- fastapi
- uvicorn[standard]
- sqlalchemy
- pydantic
- alembic
- python-dotenv

dev/test:

- pytest
- httpx
- pytest-asyncio

Отдельно важно: наличие `alembic` в зависимостях не означает, что локальный MVP уже завязан на полноценный migration workflow. В текущем visible startup path схема поднимается через `init_db()` и `Base.metadata.create_all(...)` плюс небольшие legacy-upgrade helper'ы в `database.py`.

## Ключевые API endpoint'ы, которые реально есть сейчас

### Projects

`GET /api/v1/projects`
- список проектов.

`POST /api/v1/projects`
- создаёт проект;
- принимает `name`, `slug`, `description`, `timezone`, `created_by`, `visibility`, `safe_paused`, `metadata`.

`POST /api/v1/projects/onboard`
- полный MVP-onboarding;
- принимает `boss`, `admin`, `reporting_cadence`, `communication_integrations`, `board_integration`, `team` и базовые поля проекта;
- создаёт memberships, gate checks, smoke check, onboarding audit.

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
- возвращает project report и publish block.

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

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/projects/onboard \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Project Nova",
    "slug": "project-nova-local",
    "description": "локальная проверка MVP",
    "timezone": "UTC",
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
- у него появятся memberships owner/admin;
- пройдёт synthetic smoke check через board integration stub;
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
        "priority": "high"
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

### 3. Посмотреть project tasks

```bash
curl -sS http://127.0.0.1:8000/api/v1/projects/<PROJECT_ID>/tasks
```

Ты увидишь две task'и, привязанные к goal.

### 4. Построить project report

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
python -m pytest -q
```

Если нужен быстрый прогон основных API/MVP сценариев:

```bash
python -m pytest -q tests/test_project_onboarding_api.py tests/test_mvp_e2e_flow.py tests/test_api.py
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
- уже покрытый API- и e2e-тестами;
- но пока ещё с детерминированным Hermes adapter'ом по умолчанию;
- пока ещё со stub-like integration adapters;
- пока ещё без полноценно реализованной целевой связки Postgres + Redis + worker sidecar.

Если нужен production-shaped контур — смотри архитектурную спекацию.
Если нужен честный текущий статус кода — ориентируйся на этот README и на тесты в `tests/`.
