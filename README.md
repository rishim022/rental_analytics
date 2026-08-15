Welcome to your dbt project! This repository contains a complete analytics engineering workflow following the **staging -> intermediate -> marts** pattern, including tests, documentation, and modeling best practices.

---

## 🚀 Getting Started

### 1. Install dependencies
Create and activate your Python virtual environment:
```bash
python3 -m venv dbt-venv
source dbt-venv/bin/activate
```

### 2. Configure your profile
Update your `~/.dbt/profiles.yml` with your BigQuery project, dataset, and credentials.

---

## ▶️ Running the Project

### Run models
```bash
dbt run
```

### Run tests
```bash
dbt test
```

### Build docs
```bash
dbt docs generate
dbt docs serve
```

---

## 📁 Project Structure
```
/models
  /staging        → Clean, typed, renamed raw tables
  /intermediate   → Business logic transformations
  /marts          → Final fact & dimension models
/snapshots        → Historical tracking
/macros           → Reusable logic (e.g., amenity, bathroom utils)
```

---

## 🛫 Airflow Orchestration
The `airflow/` directory adds an Airflow pipeline that orchestrates this dbt project end-to-end, and doubles as a reference for common orchestration patterns.

### Run it locally
```bash
cd airflow
cp .env.example .env
cp profiles/profiles.yml.example profiles/profiles.yml   # fill in your GCP project + dataset
# drop your BigQuery service-account key at airflow/keys/gcp-service-account.json
docker-compose up --build
```
Airflow UI: http://localhost:8080 (`admin` / `admin`). The DAG (`rental_analytics_dbt_pipeline`) is paused on creation — unpause it or trigger a manual run from the UI. Without a real service-account key the dbt tasks will fail at the BigQuery auth step; the DAG structure, scheduling, and UI are still fully explorable regardless.

### What it demonstrates
| Concept | Where |
|---|---|
| **Scheduling** | `schedule_interval="0 6 * * *"`, `catchup=False`, explicit `start_date` — [`dbt_rental_analytics_dag.py`](airflow/dags/dbt_rental_analytics_dag.py) |
| **Dependencies** | `dbt_deps → staging → intermediate → marts → generate_docs`, mirroring the dbt DAG; `run → test` within each layer |
| **Retries & retry delay** | `retries=2, retry_delay=5min` on dbt tasks (BigQuery transient errors); tighter `retries=1, retry_delay=1min` on `dbt_deps` |
| **Failure handling** | A `notify_failure` task with `trigger_rule=ONE_FAILED`, wired to every step, fires only when something upstream fails |
| **Task groups** | One `TaskGroup` per dbt layer — `staging`, `intermediate`, `marts` |
| **Callbacks** | `on_failure_callback` / `on_success_callback` / `on_retry_callback` in [`common/callbacks.py`](airflow/dags/common/callbacks.py) — structured logging today, with a single `send_notification()` extension point to wire in Slack/email later |
| **Parameters** | DAG-level `params`: `full_refresh` (bool, appends `--full-refresh` for the incremental `marts` layer) and `target_override` (string, one-off dbt `--target` override) — settable from the Trigger DAG UI |
| **Environment-specific config** | [`common/config.py`](airflow/dags/common/config.py) reads the `RENTAL_ANALYTICS_ENV` Airflow Variable (`dev`/`prod`) and resolves the BigQuery dataset, dbt `--target`, and whether notifications actually fire |

### Layout
```
airflow/
  dags/
    dbt_rental_analytics_dag.py   → the DAG
    common/config.py              → env-specific config (dev/prod)
    common/callbacks.py           → task callbacks + notifier stub
  docker-compose.yml              → Postgres + webserver + scheduler (LocalExecutor)
  Dockerfile                      → Airflow image with dbt-bigquery installed
  profiles/profiles.yml.example   → dbt profile template (copy → profiles.yml, gitignored)
  .env.example                    → AIRFLOW_UID, RENTAL_ANALYTICS_ENV
```

---

## 🧪 Testing Strategy
- **Schema tests:** `not_null`, `unique`, `accepted_values`, `relationships`
- **Custom tests:** `dbt_utils.unique_combination_of_columns`
- **Freshness checks:** Validates source table recency

All generic tests use the updated `arguments:` format for dbt 1.11 compatibility.

---

## 📊 Marts
Key outputs include:
- `fct_listing_day` – Daily grain fact table combining listings, availability, pricing, and reservations

---

## 🧱 dbt Best Practices Followed
- Naming conventions: `stg_`, `int_`, `dim_`, `fct_`
- One model per file
- Documentation for every model + column
- Business logic only in intermediate models
- Incremental strategies for large fact tables

---

## 📚 Helpful Resources
- dbt Docs: https://docs.getdbt.com/
- dbt Discourse: https://discourse.getdbt.com/
- dbt Slack: https://community.getdbt.com/
- dbt Blog: https://blog.getdbt.com/

---

## 🤝 Contributing
1. Create a branch
2. Make your changes
3. Run `dbt build` before pushing
4. Submit a pull request

---

## ⭐ About This Project
This repo powers a full analytics pipeline for rental marketplace data, built to demonstrate modeling, testing, and semantic layer design aligned with modern analytics engineering standards.
