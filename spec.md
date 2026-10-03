# spec.md — EarthScape Climate Analytics

Specification derived from the Aptech eProject brief ("Big Data – EarthScape Climate Agency").
Priority: **MUST** = stated in the brief · **SHOULD** = strongly implied / needed to make a MUST work · **COULD** = optional creativity ("improve the portal").

---

## 1. Problem definition

Climate change is producing more frequent and severe weather events, driven by greenhouse-gas emissions, deforestation and land-use change. The EarthScape Climate Agency collects very large volumes of climate data from satellites, weather stations and environmental sensors, but lacks a robust way to **process, analyze and visualize** it for informed decision-making.

**Goal:** build a Hadoop-based big data platform that ingests diverse climate data (historical + real time), stores it at scale, detects patterns/anomalies/correlations, predicts trends with machine learning, visualizes results on interactive dashboards, and alerts stakeholders when thresholds are crossed.

## 2. Scope

**In scope:** everything in sections 4 and 5, plus documentation and demo video deliverables (section 7).
**Out of scope:** operating real satellites/sensors, production-grade multi-datacenter deployment, paid cloud services, native mobile apps.

## 3. Users and roles

| Role | Who | Can do |
|---|---|---|
| **Administrator** | Agency IT / data steward | Everything an analyst can, plus: manage users and roles, run/schedule ingestion, manage alert rules and thresholds, view audit log and system monitoring, manage support tickets, trigger model retraining |
| **Analyst** | Climate scientist / policy analyst | View dashboards, query datasets within their granted scope, view anomalies/predictions, subscribe to alerts, submit feedback/support requests |

Optional third role (COULD): **Viewer** (read-only dashboards for external stakeholders).

---

## 4. Functional requirements

### FR-1 User authentication and authorization (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-1.1 | Secure login/logout for registered users | Valid credentials → session established; invalid → generic error; passwords stored as bcrypt hashes; lockout/backoff after 5 failed attempts |
| FR-1.2 | Role-based access: administrator, analyst | Each API route and page declares a required role; analyst calling an admin route gets `403`; verified by automated tests |
| FR-1.3 | Data access controls by role and responsibility | Admin can assign an analyst dataset scopes (e.g. `weather`, `satellite`, `sensor`); queries outside scope return `403` and are audit-logged |
| FR-1.4 | User management (admin) | Admin can create, deactivate, reset password, and change role of users; changes appear in the audit log |
| FR-1.5 | Session security (SHOULD) | Tokens expire (default 30 min idle), httpOnly + SameSite cookies, CSRF protection on state-changing forms |

### FR-2 Data ingestion (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-2.1 | Ingest satellite imagery data | Sample satellite files (GeoTIFF/NetCDF/PNG tiles) land in HDFS raw zone; metadata (satellite, band, timestamp, bounding box, resolution, path) registered in a queryable table |
| FR-2.2 | Ingest weather station records | CSV/JSON station files validated, normalized and written as Parquet to the curated zone |
| FR-2.3 | Ingest environmental sensor data | Sensor readings (temperature, humidity, pressure, CO₂, PM2.5, sea level, etc.) accepted via file and via stream |
| FR-2.4 | Historical (batch) and real-time sources | Both paths work end-to-end; a record from either appears in the same queryable dataset |
| FR-2.5 | Common climate data formats | At minimum: CSV, JSON, NetCDF (CF conventions), GeoTIFF (as imagery), Parquet. Unsupported format → rejected with a clear message |
| FR-2.6 | Validation and quarantine (SHOULD) | Records failing schema/range checks go to `/climate/quarantine/` with a reason; counts reported per ingestion job |
| FR-2.7 | Ingestion job tracking (SHOULD) | Each run records source, start/end, rows in/out/rejected, status in `ingestion_jobs` |
| FR-2.8 | Admin upload/trigger UI (SHOULD) | Admin can upload a file or trigger a configured source from the portal |

### FR-3 Data storage (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-3.1 | Use HDFS for scalable, fault-tolerant storage | All raw, curated and processed datasets live in HDFS; replication factor and block size documented |
| FR-3.2 | Partitioning and organization to optimize retrieval | Curated tables partitioned by `year`/`month` (and `source` where useful); a date-bounded query reads only the relevant partitions (demonstrated with `EXPLAIN` or timing comparison) |
| FR-3.3 | Zoned layout (SHOULD) | `raw/`, `curated/`, `processed/`, `models/`, `quarantine/` zones with documented naming conventions |
| FR-3.4 | SQL access (SHOULD) | Curated and processed data queryable through Impala (the brief lists an Impala server) |

### FR-4 Data processing (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-4.1 | Hadoop MapReduce jobs for parallel processing across nodes | At least 4 jobs run on YARN: monthly/annual aggregates, climatology baseline, anomaly detection, correlation prep. Each shows >1 mapper task in the job history |
| FR-4.2 | Algorithms for climate patterns, anomalies and correlations | Patterns: seasonal and long-term trends per station/region. Anomalies: deviation from climatological baseline (z-score, default \|z\| ≥ 3). Correlations: Pearson/Spearman between variables (e.g. CO₂ vs temperature, humidity vs precipitation) with p-values |
| FR-4.3 | Handle missing/incomplete data gracefully | Jobs don't crash on nulls/malformed rows; missing counts reported; documented imputation (e.g. interpolation within gaps ≤ N hours, otherwise flagged and excluded). Test data includes deliberately broken rows |

### FR-5 Real-time data processing (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-5.1 | Real-time streaming ingestion | A simulated sensor stream (≥ 5 msgs/s) flows through Kafka to a consumer with end-to-end latency < 10 s to availability in the portal |
| FR-5.2 | Seamless integration with batch processing | Streaming data is persisted to HDFS in micro-batches in the same schema; a unified view (batch ∪ stream) returns both with no duplicates (dedupe key: `source_id + timestamp + variable`) |
| FR-5.3 | Streaming rule evaluation | Alert rules (FR-8) are evaluated against the live stream, not only batch results |

### FR-6 Machine learning models (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-6.1 | Predictive models for climate trends and impacts | Forecast at least temperature (and one more variable, e.g. CO₂ or sea level) for a configurable horizon; report MAE/RMSE against a held-out period and a naive baseline |
| FR-6.2 | Anomaly detection | Isolation Forest (or equivalent) flags anomalies; precision/recall reported against labelled/injected anomalies in test data |
| FR-6.3 | Trend prediction and correlation analysis | Trend slope with confidence interval per region; correlation matrix exposed in the portal |
| FR-6.4 | Regular update/refinement with latest data | Retraining runnable on demand (admin) and on a schedule (e.g. weekly); model versions stored with training window and metrics; the portal shows the active version |

### FR-7 Data visualization (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-7.1 | Interactive dashboards | At least 3 dashboards (Overview, Anomalies, Forecast) with filters (date range, region/station, variable) |
| FR-7.2 | Visual representations of patterns, anomalies, predictions | Time series, seasonal heatmap, geographic map of stations/anomalies, correlation heatmap, forecast with confidence band |
| FR-7.3 | Customizable, user-friendly interface for stakeholders | Users can choose variables/date ranges and save a view (SHOULD); layout usable on laptop and tablet |
| FR-7.4 | Tableau (from the brief's tool list) | Dashboards built in Tableau from Impala/extract data, embedded or linked from the portal; live data views may use Plotly |

### FR-8 Notifications and alerts (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-8.1 | Automated alerts on predefined thresholds for anomalies/significant events | Rule example: `temperature > 45 °C at station X` or `anomaly_z ≥ 3 for ≥ 3 consecutive readings`. Triggering produces an alert event |
| FR-8.2 | Configurable alerting | Admin can create/edit/disable rules (variable, region, operator, threshold, severity, cooldown, recipients) without code changes |
| FR-8.3 | Real-time notification | In-app notification within 10 s of the triggering reading; email notification (SMTP) as a second channel; duplicate suppression via cooldown |
| FR-8.4 | Alert history (SHOULD) | Alerts list with severity, status (new/acknowledged/resolved), who acknowledged |

### FR-9 Feedback and support (MUST)

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-9.1 | Users can contact support, report issues, give feedback | Form with type (issue / question / feedback), subject, message, optional attachment; confirmation shown |
| FR-9.2 | Ticket handling (SHOULD) | Admin sees tickets, changes status (open / in progress / closed), replies; submitter sees status |

---

## 5. Non-functional requirements

| ID | Category | Requirement | Acceptance / verification |
|---|---|---|---|
| NFR-1 | Performance | Monitoring tools track system performance, resource utilization, processing times | Admin monitoring page shows: HDFS usage, YARN job durations, ingestion throughput, API latency (p50/p95), stream lag. Source: Hadoop REST APIs + app metrics stored in Mongo |
| NFR-2 | Performance | Optimization strategies for overall efficiency | Documented and measured: Parquet + partitioning vs raw CSV, combiners in MapReduce, Impala stats; before/after numbers in the report |
| NFR-3 | Data security | Encryption of sensitive data in storage and in transit | TLS on all portal traffic; HDFS encryption zone (or documented equivalent) for sensitive datasets; bcrypt for passwords; secrets from env; verified by checklist |
| NFR-4 | Data security | Compliance with relevant data protection regulations/standards | Documented mapping of controls to principles from GDPR / Pakistan's data protection requirements as applicable: data minimization, access control, audit logging, retention |
| NFR-5 | Reliability | ≥ 99% uptime; scheduled maintenance communicated in advance | Health endpoint + uptime log; maintenance banner feature (SHOULD); computed uptime over the test window reported |
| NFR-6 | Reliability | Regular automated backups | `ops/backup.sh` scheduled (cron/Task Scheduler): HDFS critical zones via `distcp`/snapshots, Mongo via `mongodump`; restore procedure tested once and documented |
| NFR-7 | Scalability | Horizontal scaling for growing data volumes and processing demand | Documented and demonstrated: add a DataNode/NodeManager and show rebalanced blocks / more parallel tasks |
| NFR-8 | Scalability | Load balancing | Apache `mod_proxy_balancer` across ≥ 2 app instances; failover shown by stopping one instance |
| NFR-9 | Compliance and standards | Adhere to environmental data standards and protocols | CF conventions for NetCDF, ISO 8601 timestamps, SI units / WMO variable naming, station IDs per source standard; documented |
| NFR-10 | Compliance and standards | Industry best practices for big data processing and analytics | Documented: schema-on-write for curated zone, idempotent jobs, lineage notes, data quality checks |
| NFR-11 | Documentation | User documentation | User guide, FAQ, tutorials (how to read a dashboard, create an alert rule, file a ticket) |
| NFR-12 | Documentation | Developer documentation | Architecture, data processing workflows, ML model cards, setup guide, API reference |
| NFR-13 | Documentation | Demo video | Video showing complete working of the application end to end (see task.md Phase 9 script) |

---

## 6. Datasets and test data

Use real public data where possible so the analytics are meaningful; keep committed samples small.

| Need | Suggested source | Notes |
|---|---|---|
| Weather stations | NOAA GHCN-Daily, or Berkeley Earth station data | Daily TMAX/TMIN/PRCP; large enough to justify MapReduce |
| Global/regional temperature | NASA GISTEMP, Berkeley Earth | Long-term trend baseline |
| CO₂ / greenhouse gases | NOAA Mauna Loa CO₂, Global Carbon Project | Correlation vs temperature |
| Satellite imagery | NASA Worldview/GIBS tiles, Sentinel/Landsat samples, or MODIS NDVI/land surface temperature | Store scenes; derive metadata and a few aggregate stats (e.g. mean NDVI) |
| Sensor stream | Simulated (Python generator with seasonal signal + noise + injected anomalies) | Injected anomalies provide ground truth for FR-6.2 |
| Broken data | Hand-made | Nulls, wrong units, out-of-range, duplicates, malformed lines, late-arriving timestamps |

Exact sources and licenses are recorded in `docs/ASSUMPTIONS.md`. Test data submitted with the project lives in `data/test/`.

---

## 7. Deliverables (from the brief)

1. Working application (source code) — portal, ingestion, MapReduce, ML, alerts
2. **Project report** containing: problem definition, design specifications, diagrams (flowcharts for activities, Data Flow Diagrams), source code, test data used, installation instructions
3. User documentation (guide, FAQ, tutorials) and developer documentation (architecture, workflows, ML models)
4. **ReadMe** listing assumptions, inside the final **zip**
5. **Video clip** demonstrating the working application (optional: live hosted URL)
6. Extra creativity beyond the spec is welcome (see section 8)

## 8. Optional enhancements (COULD)

- Region comparison tool and "what-if" scenario slider on forecasts
- Downloadable PDF/CSV reports from dashboards (audit-logged)
- Climate-risk index per region (combining anomaly frequency, trend slope, extreme events)
- Natural-language query helper over the Impala tables
- Dark mode, bilingual UI (English/Urdu)
- Viewer role for external stakeholders

## 9. Assumptions (initial; mirror in `docs/ASSUMPTIONS.md`)

| ID | Assumption |
|---|---|
| A-1 | The cluster runs pseudo-distributed on a single machine (WSL2 Ubuntu); horizontal scaling is demonstrated with an added node (Docker) rather than a physical cluster |
| A-2 | "Real-time" means seconds-level latency using Kafka + a Python consumer; Spark Structured Streaming is out of scope unless time allows |
| A-3 | Satellite "imagery" is handled as stored files plus extracted metadata and simple derived statistics, not full image-processing pipelines |
| A-4 | Public datasets stand in for the agency's private data |
| A-5 | MapReduce jobs are written in Python via Hadoop Streaming (Java is not required by the brief) |
| A-6 | Email alerts use a test SMTP account / local mail catcher in the demo |
| A-7 | If Impala cannot run on the available hardware, Hive/Spark SQL is substituted and documented as a deviation |

## 10. Requirement → component traceability

| Requirement | Component(s) |
|---|---|
| FR-1 | `web/auth`, MongoDB `users`, `audit_log` |
| FR-2 | `ingestion/`, HDFS raw/curated/quarantine, Mongo `ingestion_jobs` |
| FR-3 | HDFS layout, Impala tables, `ops/` |
| FR-4 | `mapreduce/` |
| FR-5 | Kafka, `ingestion/stream_*`, unified Impala view |
| FR-6 | `ml/`, Mongo `model_registry` |
| FR-7 | Tableau workbooks, `web/templates` + Plotly |
| FR-8 | `alerts/`, Mongo `alert_rules`, `alert_events`, `notifications` |
| FR-9 | `web/` support module, Mongo `feedback_tickets` |
| NFR-1…8 | `ops/`, Apache config, monitoring page |
| NFR-9…13 | `docs/`, report |
