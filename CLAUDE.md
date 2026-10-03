# CLAUDE.md — EarthScape Climate Analytics (Big Data eProject)

Project memory for Claude Code. Read this first, every session.

## What this project is

An Aptech Big Data eProject for the fictional **EarthScape Climate Agency**: a Hadoop-based platform that ingests climate data (satellite, weather stations, sensors), processes it in batch and real time, runs ML for anomaly/trend/correlation analysis, shows interactive dashboards, and raises alerts. A secure web portal fronts it all (roles: `admin`, `analyst`).

The submission is graded on **working software + documentation + a demo video**, so documentation is a first-class deliverable, not an afterthought.

## Source-of-truth files

| File | Purpose | Rule |
|---|---|---|
| `spec.md` | WHAT to build. Requirement IDs (FR-x, NFR-x) with acceptance criteria | Never invent requirements. Propose changes, don't silently make them |
| `design.md` | HOW it is built. Architecture, data model, APIs, job designs | Update it whenever implementation diverges |
| `task.md` | Ordered work plan with checkboxes, mapped to requirement IDs | Tick boxes only when the "Done when" check passes |
| `docs/ASSUMPTIONS.md` | Every assumption/deviation made (feeds the submission ReadMe) | Append, with date and reason |

## Tech stack (fixed by the brief, see design.md for versions)

- **Storage:** HDFS (Hadoop 3.x). Curated data as Parquet, partitioned by `year/month`
- **Batch processing:** Hadoop MapReduce via **Hadoop Streaming with Python** mappers/reducers
- **SQL layer:** Impala over Parquet tables (Hive Metastore)
- **Real-time:** Kafka + Python consumer (micro-batches to HDFS, live rules to MongoDB)
- **App database:** MongoDB (users, alert rules/events, tickets, audit log, metrics)
- **ML / analysis:** Python (pandas, scikit-learn, statsmodels) in Jupyter/Anaconda; R/RStudio optional for exploratory stats
- **Web portal:** Python FastAPI + Jinja2 templates, JWT in httpOnly cookie, served behind Apache (TLS, load balancing)
- **Visualization:** Tableau (connected via Impala/extracts) embedded in portal; Plotly for live views
- **Dev environment:** Windows 10/11 host, **WSL2 Ubuntu 22.04** for Hadoop/Kafka/Impala. Editor: VS Code

## Repository layout

```
earthscape/
├── CLAUDE.md  spec.md  design.md  task.md
├── ingestion/        # batch loaders, validators, Kafka producer/consumer
├── mapreduce/        # one folder per job: mapper.py, reducer.py, run.sh, README
├── ml/               # training, evaluation, model registry helpers
├── alerts/           # rule evaluator + notifiers (email, in-app)
├── web/              # FastAPI app: auth, api, templates, static
├── ops/              # start/stop scripts, backup.sh, monitor.py, apache conf
├── data/
│   ├── sample/       # small committed samples
│   └── test/         # test data submitted with the project
├── tests/            # pytest: unit + integration
├── docs/             # report, diagrams, user guide, developer guide, ASSUMPTIONS.md
└── submission/       # final zip contents (ReadMe, video link, report)
```

## Common commands

```bash
# Cluster (inside WSL2)
ops/start-cluster.sh            # HDFS + YARN + Kafka + Mongo + Impala
ops/stop-cluster.sh
hdfs dfs -ls /climate           # sanity check

# Ingestion
python -m ingestion.batch_loader --source ghcn --path data/sample/ghcn.csv
python -m ingestion.stream_producer --rate 5          # simulated sensors
python -m ingestion.stream_consumer

# MapReduce (Hadoop Streaming)
bash mapreduce/monthly_avg/run.sh

# ML
python -m ml.train_anomaly && python -m ml.train_trend

# Web
uvicorn web.app.main:app --reload --port 8000

# Tests
pytest -q
```

If a command above doesn't exist yet, it's a planned deliverable in `task.md`; create it as part of that task.

## How to work in this repo

1. **Work one task at a time** from `task.md`, in phase order. State which task ID you are doing before you start.
2. **Small, verifiable steps.** Each task has a "Done when" check. Run it, show the output, then tick the box.
3. **Trace to requirements.** Commit messages and PR notes reference IDs, e.g. `FR-4.2: anomaly z-score job`.
4. **Keep docs in sync.** Code change that affects architecture, schema, or an API → update `design.md` in the same change.
5. **Record deviations.** If the brief can't be met as written (e.g. a tool won't run on the hardware), add an entry to `docs/ASSUMPTIONS.md` and flag it to me before proceeding.
6. **Ask when the spec is ambiguous**; otherwise pick the simplest option that satisfies the acceptance criteria and log it as an assumption.

## Conventions

- **Python 3.11**, type hints on public functions, `black` + `ruff`, docstrings that say *what and why*.
- **Config via env vars / `.env`** (never hard-code hosts, passwords, keys). Commit `.env.example` only.
- **Timestamps:** UTC, ISO 8601. **Units:** SI (°C, hPa, mm, ppm). Convert on ingest and record the original unit.
- **Missing values:** never silently drop. Flag with `quality_flag`, impute only in processing jobs, and log counts (FR-4.3).
- **Schemas:** columns are `snake_case`; HDFS paths are lowercase; Parquet compression `snappy`.
- **MapReduce jobs:** mapper/reducer read stdin and write tab-separated stdout; each job has a local test (`cat sample | mapper | sort | reducer`) before running on the cluster.
- **API:** REST, JSON, versioned under `/api/v1`. Every endpoint declares required role. Errors use `{ "error": { "code", "message" } }`.
- **Tests:** every requirement with an acceptance criterion gets at least one test or a documented manual check.

## Security rules (non-negotiable)

- Passwords hashed with bcrypt; never logged or returned by the API.
- RBAC enforced **server-side** on every route; the UI hiding a button is not access control.
- No secrets, API keys, or real credentials in git. Sample data only.
- All external traffic over TLS (Apache); sensitive Mongo fields encrypted at rest as per design.md.
- Audit-log: logins, role changes, ingestion runs, rule changes, data exports.

## Definition of done (per feature)

- [ ] Meets the acceptance criteria in `spec.md`
- [ ] Tests pass (`pytest -q`) or manual check recorded in `docs/`
- [ ] `design.md` / `docs/` updated
- [ ] `task.md` box ticked
- [ ] Nothing hard-coded, no secrets committed

## Submission reminder

Final zip must contain: project report (problem definition, design specs, flowcharts/DFDs, source code, test data, install instructions), `ReadMe.doc` listing assumptions, and a **demo video** of the working application. Optionally a live URL. See `task.md` Phase 9.

## Known risks (watch these)

- **Impala on WSL2/Windows is the hardest install.** Time-box it; fallback is Hive or Spark SQL, logged as a deviation.
- **16 GB RAM** is tight for HDFS + YARN + Kafka + Mongo + Impala together. Start only what the current task needs.
- **Tableau live connection to Impala** may not be embeddable; use extracts/Tableau Public for the portal and keep Tableau Desktop for the live demo.
