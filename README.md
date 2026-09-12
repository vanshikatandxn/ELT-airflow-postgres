# ELT Pipeline with Apache Airflow + PostgreSQL

A hands-on data engineering project built to learn and demonstrate the core
skills behind production batch pipelines: orchestration, idempotent data
loads, layered data modeling, data quality enforcement, and reproducible
infrastructure — using a real API, a real orchestrator, and a real database,
not toy examples.

## What this project demonstrates

This isn't just "a pipeline that moves weather data." Each part of it was
built to practice a specific, transferable data engineering skill:

| Skill | Where it shows up |
|---|---|
| Orchestration & dependency management | Airflow DAG with ordered, retry-safe tasks |
| ELT architecture (raw → staging → marts) | Three-layer Postgres schema design |
| Idempotent pipeline design | `ON CONFLICT ... DO UPDATE` upserts, safe re-runs |
| Data quality enforcement | A dedicated SQL check that fails the DAG on bad data |
| Infrastructure as code | Dockerized Airflow + Postgres, one-command startup |
| Version control discipline | Small, reviewable commits; secrets kept out of git |
| CI automation | GitHub Actions running tests on every push/PR |

## Why ELT, not ETL

Data is loaded into Postgres **untouched first** (`raw` schema), then
transformed **after loading** using SQL (`staging`, `marts` schemas). This is
the same pattern modern data teams use (and what tools like dbt are built
around): raw data becomes a permanent source of truth, and transformations
can change or be re-run without ever re-hitting the original source.

## Architecture