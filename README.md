# Drivee NL2SQL Backend Core

Техническая документация по бэкенд-инфраструктуре NL2SQL сервиса Drivee.

## Архитектура и паттерны (DDD)

Проект спроектирован по принципам **Чистой Архитектуры (Clean Architecture)** с применением **Domain-Driven Design (DDD)**. Строгое разделение слоев позволяет изолировать бизнес-правила обработки интентов от деталей реализации LLM-каналов и транспортного слоя.

* **`app/domain/`** (Доменный слой): Изолированная бизнес-логика. Политики нормализации, управление уверенностью (Confidence Policy) и базовые интерфейсы репозиториев/генераторов.
* **`app/application/`** (Прикладной слой): Сценарии использования (Use Cases). Например, `AskQuestionUseCase`, который оркестрирует процесс: получение запроса → разрешение неоднозначностей (Intent Resolution) → генерация SQL → валидация → выполнение.
* **`app/infrastructure/`** (Инфраструктурный слой): Реализации адаптеров:
  * **NL2SQL Адаптеры (`nl2sql_adapters.py`)**: Абстракция над **Vanna.ai**. Включает паттерны Lazy Initialization для загрузки RAG-контекста в память, механизмы Regenerate при синтаксических ошибках SQL, генерацию человекочитаемых Explain-блоков и LLM-классификатор неоднозначностей (Intent Classifier).
  * **Безопасность (`security/sql_guardrails.py`)**: AST-парсер на базе `sqlglot`. Анализирует сгенерированный SQL до его исполнения, жестко блокирует мутирующие запросы (DML/DDL: DROP, UPDATE, INSERT) и валидирует скоуп (ограничение доступа только к разрешенным таблицам, например `orders`).
  * **MCP-интеграция (`mcp/query_server.py`)**: Реализация сервера Model Context Protocol (через FastMCP) для экспорта аналитических инструментов во внешние AI-агенты (интроспекция схемы, безопасное выполнение запросов).
  * **Обучение (`training/`)**: Пайплайны векторизации DDL, документации (dotML) и few-shot примеров для RAG-составляющей.
* **`app/interfaces/`** (Транспортный слой):
  * **HTTP (`http/server.py`)**: Асинхронный REST API (FastAPI) с поддержкой механизмов Clarification (диалоговое уточнение метрик или периодов, если запрос двусмысленный).
  * **CLI (`cli/`)**: Скрипты инициализации БД и прогрева RAG-памяти.

## Особенности реализации NL2SQL Pipeline

1. **Многоуровневая обработка интента (Intent Resolution)**: Если запрос пользователя неоднозначен (например, требуется выбрать конкретную метрику или период), пайплайн не генерирует SQL с галлюцинациями, а возвращает запрос на уточнение (`needs_clarification=True`).
2. **AST Guardrails**: Защита базы данных реализована на уровне синтаксического дерева SQL, что исключает обходы через SQL-инъекции, возможные при использовании регулярных выражений.
3. **Thread-Safe LLM Runtime**: Ленивая загрузка LLM-контекста и потокобезопасное кэширование экземпляров через `VannaRuntime`, что оптимизирует использование памяти и ускоряет ответ API.

## Инструкция по развертыванию (Local Env)

### 1. Окружение
Проект требует Python 3.10+ и запущенный Docker daemon.
```bash
python -m venv venv
venv\Scripts\activate
pip install -r backend/requirements.txt
```

### 2. Переменные окружения
Скопируйте `cp backend/.env.example backend/.env`. Обязательные ключи:
- `OPENAI_API_KEY` (в проекте настроен прокси через OpenRouter, ключ должен быть совместимым).
- `OPENAI_MODEL` (например, `openrouter/gpt-4o-mini`).
- `READONLY_DB_PASSWORD` (локальный пароль read-only пользователя PostgreSQL; не коммитьте реальный `.env`).
- `ENABLE_LLM_INTENT_CLASSIFIER=1` (опционально, для активации LLM-классификатора интентов).

CLI-загрузка данных использует `PGUSER`/`PGPASSWORD`, а runtime-выполнение NL2SQL-запросов использует `READONLY_DB_USER`/`READONLY_DB_PASSWORD`.

### 3. Запуск инфраструктуры
Команды выполняются из корня проекта:

```bash
# 1. Поднимаем PostgreSQL с тестовыми данными
docker compose -f backend/docker-compose.yml --env-file backend/.env up -d

# 2. Накатываем DDL и seed-данные из train.csv
python -m backend.app.interfaces.cli.init_db_cli

# 3. Векторизуем схему и few-shot примеры для RAG (Vanna.ai)
python -m backend.app.interfaces.cli.init_vanna_cli
```

Если PostgreSQL volume уже создан до настройки `READONLY_DB_PASSWORD`, пересоздайте локальный volume перед повторной инициализацией.

### 3.1. Полная загрузка CSV на сервере
Для production-загрузки полного `train.csv` используйте chunked loader:

```bash
python -m backend.app.interfaces.cli.load_orders_cli --csv /opt/drivee/data/train.csv --chunk-size 50000 --truncate
```

Флаг `--truncate` очищает таблицу `orders` перед загрузкой. Без него строки будут добавляться в существующую таблицу.

### 4. Точки входа
- **REST API (FastAPI)**:
  ```bash
  python -m backend.app.interfaces.http.server
  ```
- **MCP Server (FastMCP)**:
  ```bash
  python -m backend.app.infrastructure.mcp.query_server
  ```

*(Примечание: папка `backend/scripts/` содержит legacy-обёртки для обратной совместимости вызовов и подлежит удалению в будущих релизах. Используйте прямые вызовы модулей `app.interfaces`, как показано выше).*
