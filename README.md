# Zomato AI Data Engineering: A Complete Pipeline, Start to Finish

This project is a full batch data pipeline that carries Zomato-style food delivery data from raw CSV files through to AI-driven analytics:

**Food Delivery Dataset → Amazon S3 → Snowflake → dbt → Airflow → AI (OpenAI)**

Source files are stored in an S3 data lake and loaded into Snowflake through a storage integration. From there, dbt reshapes the data across medallion layers: RAW (Bronze) tables populated with `COPY INTO`, cleaned STAGING (Silver) views, and analytics-ready MARTS (Gold) made up of dimensions, incremental fact tables, and aggregate marts. Apache Airflow runs the entire flow as a single daily DAG. An AI lane built on OpenAI sits on top of the warehouse: LLM enrichment converts free-text reviews into structured, queryable columns, RAG lets you converse with your reviews, and text-to-SQL lets you question the warehouse in plain English. Streamlit delivers the dashboards and AI apps.

![Architecture](docs/architecture.png)

> 📂 **Dataset and project slides:** [Google Drive folder](https://drive.google.com/drive/folders/1FEnGWMHhHzzTUCZOw1-YnH2v3DMuM-rs?usp=sharing). Download the CSVs from here and put them in `data/` (they are too big to store in the repo).

## What this project builds

| Layer | Location | Description |
|---|---|---|
| **Source** | `data/` (local) | 4 real dimension CSVs (restaurants, users, food, menu) plus 3 generated fact files: **10M orders**, **~23M order items**, and **300K free-text reviews** |
| **Lake** | Amazon S3 | A single bucket with one `raw/<table>/` folder per CSV |
| **Bronze** | Snowflake `ZOMATO.RAW` | Tables loaded with `COPY INTO` from S3 via a keyless storage integration |
| **Silver** | Snowflake `ZOMATO.STAGING` | dbt staging views that clean, type, and rename every source |
| **Gold** | Snowflake `ZOMATO.MARTS` | Dimensions, **incremental** facts (MERGE), business marts, and an SCD2 snapshot |
| **AI** | Snowflake `ZOMATO.AI` | LLM-enriched reviews (sentiment and topic), RAG chat, and text-to-SQL |
| **Orchestration** | Airflow (Docker) | One daily DAG: load → transform → enrich → AI mart |

## Tech stack

Python · Pandas · Amazon S3 · Snowflake · dbt (dbt-snowflake) · Apache Airflow 3 (Docker) · OpenAI (`gpt-4o-mini`, `text-embedding-3-small`) · Streamlit

## Repository layout

```
├── airflow/                  # Airflow 3 running on Docker
│   ├── Dockerfile            #   Snowflake + OpenAI providers, dbt in a separate venv
│   ├── docker-compose.yaml   #   postgres + api-server + scheduler; credentials via env vars
│   ├── example.env           #   template for SNOWFLAKE_* / OPENAI_API_KEY
│   └── dags/zomato_batch.py  #   the pipeline DAG (4 tasks)
├── zomato/                   # dbt project
│   ├── models/staging/       #   7 staging views (Silver) + sources + tests
│   ├── models/marts/         #   dims, incremental facts, business marts (Gold)
│   └── macros/               #   custom schema-name macro
├── ai/                       # AI layer
│   ├── enrich_reviews.py     #   LLM enrichment → ZOMATO.AI.REVIEW_ENRICHED
│   ├── rag_chat.py           #   RAG, "chat with your reviews" (Streamlit)
│   ├── text_to_sql.py        #   text-to-SQL, "chat with your warehouse" (Streamlit)
│   └── example.env           #   template for AI credentials
├── snowflake/                # Snowflake setup SQL (run in Snowsight, in order)
│   ├── 01_setup.sql          #   warehouse ZOMATO_WH, database ZOMATO, schemas, role
│   ├── 02_storage_integration.sql  # keyless S3 link (works together with aws/iam/)
│   ├── 03_stage_and_formats.sql    # external stage + CSV file format
│   ├── 04_raw_tables.sql     #   RAW (Bronze) table DDL; column order mirrors the CSVs
│   └── 05_copy_into.sql      #   COPY INTO RAW from the stage
├── aws/iam/                  # IAM policy + role trust policies for the S3 ↔ Snowflake handshake
└── docs/architecture.png     # architecture diagram
```

> The `data/` folder (~2.3 GB of CSVs), logs, and dbt `target/` artifacts are deliberately left out of version control. Get the dataset and slides from the [Google Drive folder](https://drive.google.com/drive/folders/1FEnGWMHhHzzTUCZOw1-YnH2v3DMuM-rs?usp=sharing).

## How the pipeline works

### 1 · Getting data into S3

All seven CSVs are uploaded to `s3://<BUCKET>/raw/<table>/`, with a separate folder for each table (`restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`).

### 2 · S3 to Snowflake: a single keyless handshake

Snowflake accesses the bucket without any stored credentials, using a storage integration paired with an IAM role. The Snowflake half is in [`snowflake/02_storage_integration.sql`](snowflake/02_storage_integration.sql), and the AWS JSON documents are in [`aws/iam/`](aws/iam/):

| File | Purpose |
|---|---|
| [`s3-read-policy.json`](aws/iam/s3-read-policy.json) | IAM **policy** `zomato-s3-read`, granting read-only access to the bucket |
| [`snowflake-role-trust-policy-initial.json`](aws/iam/snowflake-role-trust-policy-initial.json) | IAM **role** `snowflake-s3-role`, using a placeholder trust policy at creation time |
| [`snowflake-role-trust-policy-final.json`](aws/iam/snowflake-role-trust-policy-final.json) | Final trust policy, containing Snowflake's IAM user ARN and the external ID from `DESC INTEGRATION` |

Sequence matters here: create the AWS policy and role, then create the Snowflake `STORAGE INTEGRATION` that points to the role ARN, run `DESC INTEGRATION` to obtain `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`, and paste both into the role's trust policy. Two lessons learned the hard way: the trust `Principal` has to be Snowflake's IAM user ARN rather than `:root`, and you should never re-run `CREATE OR REPLACE` on the integration afterward, since that generates a new external ID and breaks the trust.

### 3 · Loading with `COPY INTO`

The table DDL in [`snowflake/04_raw_tables.sql`](snowflake/04_raw_tables.sql) follows each CSV's column order. Then [`snowflake/05_copy_into.sql`](snowflake/05_copy_into.sql) pulls every file from the stage into the `ZOMATO.RAW` tables: 10M orders, ~23M order items, and 300K reviews.

### 4 · Transforming with dbt (medallion architecture)

- **Staging (Silver):** one view per source. This step handles the messy restaurant dimension (`--` becomes null, `₹ 200` becomes 200), lowercases emails, derives `is_delivered`, and so on.
- **Dimensions (Gold):** `dim_restaurants`, `dim_customer` (including age segments), `dim_food`, and a generated `dim_date` calendar.
- **Facts (Gold, incremental):** `fct_orders` and `fact_order_items` are built with `materialized='incremental'` and a MERGE strategy, so a re-run only handles new rows rather than rebuilding 10M+ records.
- **Marts (Gold):** one table for each business question: daily city revenue (GMV, AOV, cancel rate), restaurant performance, delivery SLA (p50/p90 by city and hour), and review insights.
- **Tests:** `unique`, `not_null`, `relationships`, and `accepted_values` checks, plus a singular reconciliation test. `dbt build` executes models and tests in dependency order.

### 5 · Orchestrating with Airflow

A single daily DAG, [`zomato_batch`](airflow/dags/zomato_batch.py), executes everything as one graph:

```
reload_raw  →  dbt_build_core  →  enrich_reviews  →  dbt_build_ai
(COPY from S3)  (dbt build + tests)  (OpenAI enrichment)   (AI mart)
```

Credentials stay out of the code. docker-compose injects `SNOWFLAKE_*` environment variables (which dbt's `profiles.yml` reads through `env_var()`) along with an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection used by the COPY task.

### 6 · The AI layer: three capabilities

1. **LLM enrichment** (`ai/enrich_reviews.py`): *the LLM as a transformation step.* It reads review text, asks `gpt-4o-mini` to return structured JSON (sentiment and topic), and writes the result to `ZOMATO.AI.REVIEW_ENRICHED`. dbt then models that table into `mart_review_insights` just like any other source. The process is idempotent and capped by a sample size (`SAMPLE_N`), so you never pay twice for the same review.
2. **RAG** (`ai/rag_chat.py`): *chat with your reviews.* It embeds the reviews, retrieves the ones most similar to a question, and produces an answer grounded in those real reviews, with sources.
3. **Text-to-SQL** (`ai/text_to_sql.py`): *chat with your warehouse.* The LLM receives the marts' schema and writes Snowflake SQL for an English question. A SELECT-only guard validates the query before it runs as `DBT_ROLE`.

## Getting started

```bash
# Snowflake objects (warehouse ZOMATO_WH, database ZOMATO, schemas RAW/STAGING/MARTS/SNAPSHOTS/AI, role DBT_ROLE)
# + the S3 storage integration: run snowflake/01→05 in Snowsight; see aws/iam/ for the AWS side.

# dbt
cd zomato
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai

# Airflow
cd airflow
cp example.env .env          # fill in SNOWFLAKE_*, OPENAI_API_KEY, SAMPLE_N
docker compose build && docker compose up -d
# http://localhost:8080 → un-pause zomato_batch → Trigger

# AI apps
export OPENAI_API_KEY=sk-...
python ai/enrich_reviews.py
streamlit run ai/rag_chat.py      # chat with reviews
streamlit run ai/text_to_sql.py   # chat with the warehouse
```