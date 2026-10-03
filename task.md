# task.md — EarthScape Climate Analytics

Ordered build plan. Work top to bottom, one task at a time. Tick a box only when its **Done when** check passes.
IDs in brackets map to `spec.md`. Architecture details are in `design.md`.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done · 🔴 risky/time-box · 📄 produces report material

---

## Phase 0 — Setup and foundations

- [ ] **0.1 Repo + folder structure** per `CLAUDE.md`; `.gitignore`, `.env.example`, `README` stub, black/ruff config
  - Done when: `tree` matches CLAUDE.md layout; first commit made
- [ ] **0.2 WSL2 Ubuntu 22.04 + Python env** (`earthscape`, Python 3.11, requirements.txt)
  - Done when: `python -V` and `pip list` show pinned deps
- [ ] **0.3 Install Hadoop 3.3.x pseudo-distributed** (HDFS + YARN) [FR-3.1]
  - Done when: `hdfs dfs -mkdir /climate && hdfs dfs -ls /` works; NameNode UI (9870) and YARN UI (8088) reachable
- [ ] **0.4 Run Hadoop's bundled wordcount** as a smoke test
  - Done when: job finishes SUCCEEDED in YARN UI
- [ ] **0.5 Install MongoDB 7 + Compass/mongosh**
  - Done when: `mongosh` connects; test insert/read works
- [ ] **0.6 Install Kafka (KRaft)**; create topic `sensor.readings`
  - Done when: console producer → consumer round-trip works
- [ ] 🔴 **0.7 Impala (Docker quickstart) pointing at HDFS + Hive Metastore** [FR-3.4]
  - Time-box: 1 day. If blocked, switch to Hive/Spark SQL and log deviation (A-7)
  - Done when: `impala-shell` runs `SHOW DATABASES` and can read a Parquet file from HDFS
- [ ] **0.8 `ops/start-cluster.sh` and `stop-cluster.sh`**
  - Done when: one command brings the stack up/down cleanly
- [ ] 📄 **0.9 `docs/ASSUMPTIONS.md` created** with A-1…A-7 from spec.md and installed versions
  - Done when: file exists and lists versions actually installed

## Phase 1 — Data acquisition and test data

- [ ] **1.1 Pick region + datasets** (weather stations, global temp, CO₂, satellite sample) and record sources/licences [spec §6]
  - Done when: `docs/ASSUMPTIONS.md` lists each dataset with URL and licence
- [ ] **1.2 Download and place samples** in `data/sample/` (small) and a larger set outside git for scale tests
  - Done when: ≥ 1 year of multi-station data + CO₂ series + ≥ 3 satellite scenes available locally
- [ ] **1.3 Sensor simulator** `ingestion/stream_producer.py`: seasonal signal + noise + **injected anomalies with ground-truth labels** [FR-5.1, FR-6.2]
  - Done when: produces N msgs/s to Kafka; label file saved for evaluation
- [ ] **1.4 Broken-data test files** in `data/test/`: nulls, wrong units, out-of-range, duplicates, malformed lines, late timestamps [FR-4.3]
  - Done when: each file documented in `data/test/README.md` with expected outcome
- [ ] 📄 **1.5 Data dictionary** (`docs/data-dictionary.md`): variables, units, ranges, source mapping

## Phase 2 — Ingestion and storage (batch)

- [ ] **2.1 Canonical schema + validation rules** (`ingestion/schema.py`, `rules.yaml`) [FR-2.6, NFR-9]
  - Done when: unit tests cover range, null, duplicate, timestamp checks
- [ ] **2.2 Format readers**: CSV, JSON, NetCDF (CF), GeoTIFF/PNG metadata [FR-2.5, FR-2.1]
  - Done when: each sample file parses to canonical rows/metadata; unsupported format raises a clear error
- [ ] **2.3 Normalization**: unit conversion to SI, UTC ISO 8601, long-form pivot, registry join for lat/lon/region [NFR-9]
  - Done when: round-trip tests pass (°F→°C, K→°C, mm/in)
- [ ] **2.4 Batch loader**: raw copy → quarantine → curated Parquet partitioned `year/month` [FR-2.2, FR-3.1, FR-3.2]
  - Done when: `python -m ingestion.batch_loader ...` produces raw + curated + quarantine in HDFS; counts match
- [ ] **2.5 Satellite ingest**: store scenes in raw, metadata table in curated [FR-2.1]
  - Done when: `SELECT * FROM satellite_metadata` returns the scenes
- [ ] **2.6 Idempotency** via content hash [design §4.1]
  - Done when: re-running same file is a no-op
- [ ] **2.7 `ingestion_jobs` tracking in Mongo** [FR-2.7]
  - Done when: each run has a record with rows in/out/rejected and status
- [ ] **2.8 Impala external tables** (`observations_batch`, `satellite_metadata`) + `COMPUTE STATS` [FR-3.4]
  - Done when: counts in Impala equal counts from loader report
- [ ] **2.9 Partition-pruning proof** [FR-3.2, NFR-2]
  - Done when: date-bounded query vs full scan timing/`EXPLAIN` recorded in `docs/benchmarks.md`
- [ ] 📄 **2.10 Ingestion flowchart + DFD level 1 (process 2.0, 3.0)** exported to `docs/diagrams/`

## Phase 3 — Batch processing (MapReduce)

- [ ] **3.1 Parquet → text export step** for streaming jobs (or TSV mirror) with column pruning [NFR-2]
  - Done when: export script produces tab-separated input in HDFS
- [ ] **3.2 `monthly_avg` job** (+ combiner) [FR-4.1]
  - Done when: local pipe test matches pandas; YARN run shows > 1 map task; output in `/climate/processed/monthly_stats`
- [ ] **3.3 `climatology` job** (baseline period) [FR-4.2]
  - Done when: per source/variable/month mean+std match pandas within tolerance
- [ ] **3.4 `anomaly` job** (z-score using climatology via distributed cache) [FR-4.2]
  - Done when: injected anomalies in test data are flagged; false-positive rate recorded
- [ ] **3.5 `correlation_prep` job + `ml/correlate.py`** [FR-4.2, FR-6.3]
  - Done when: CO₂ vs temperature Pearson/Spearman computed with p-values and lagged variants
- [ ] **3.6 `quality_report` job** [FR-4.3]
  - Done when: broken-data test set yields correct ok/imputed/suspect/missing counts
- [ ] **3.7 Missing-data policy implemented and tested** (interpolate ≤ N, flag, exclude; `BAD_RECORD` counter) [FR-4.3]
  - Done when: jobs complete on broken-data file without failing; counters visible in job history
- [ ] **3.8 Impala tables for processed outputs** (`monthly_stats`, `climatology`, `anomalies`, `correlations`)
  - Done when: queries return expected results
- [ ] **3.9 Combiner/reducer tuning + CSV vs Parquet benchmark** [NFR-2]
  - Done when: before/after table in `docs/benchmarks.md`
- [ ] 📄 **3.10 Job documentation** (`mapreduce/*/README.md`): input, key/value design, output schema, how to run

## Phase 4 — Real-time processing

- [ ] **4.1 Stream consumer**: validate, normalize, upsert `latest_readings`, buffer micro-batches [FR-5.1]
  - Done when: simulated stream visible in Mongo within 10 s
- [ ] **4.2 Micro-batch write to HDFS** (`/climate/curated/sensor_readings/.../day=DD`) every 30 s [FR-5.2]
  - Done when: Parquet files appear and are readable from Impala
- [ ] **4.3 Unified view** `observations = batch ∪ stream` with dedupe key [FR-5.2]
  - Done when: duplicate injected in both paths appears once; batch+stream totals correct
- [ ] **4.4 Late/duplicate handling** (accept < 1 h late, quarantine older) [design §4.2]
  - Done when: tests cover on-time, late, very late, duplicate
- [ ] **4.5 Throughput test** at 5 and 50 msgs/s [FR-5.1, NFR-1]
  - Done when: latency and lag numbers recorded in `docs/benchmarks.md`
- [ ] 📄 **4.6 Sequence diagram + DFD process 5.0** in `docs/diagrams/`

## Phase 5 — Machine learning

- [ ] **5.1 Feature building from Impala** (`ml/features.py`: rolling stats, seasonal residuals) [FR-6.2]
  - Done when: deterministic feature table for a station/variable
- [ ] **5.2 Anomaly model (Isolation Forest)** with comparison to z-score [FR-6.2]
  - Done when: precision/recall/F1 reported on injected-anomaly ground truth
- [ ] **5.3 Trend/forecast model** (temperature + one more variable) with intervals [FR-6.1]
  - Done when: MAE/RMSE/MAPE reported vs naive seasonal baseline on held-out months
- [ ] **5.4 Trend slope + CI per region** [FR-6.3]
  - Done when: slope table written to `forecasts`/trend table
- [ ] **5.5 Model registry + versioned artifacts in HDFS** [FR-6.4]
  - Done when: training registers version, metrics, window, path; `active` flag promotion rule works
- [ ] **5.6 Retraining on demand + scheduled** [FR-6.4]
  - Done when: `python -m ml.train_*` run via schedule creates a new version from fresh data
- [ ] **5.7 Scoring writes** `anomalies` and `forecasts` back to HDFS/Impala
  - Done when: portal-facing tables populated
- [ ] 📄 **5.8 Model cards** (`docs/models/*.md`): purpose, data, features, metrics, limitations

## Phase 6 — Web portal, security, and alerts

### Auth and RBAC
- [ ] **6.1 FastAPI skeleton, config, logging, health endpoint** [NFR-5]
  - Done when: `/api/v1/health` returns OK; structured logs written
- [ ] **6.2 Users + login/logout + bcrypt + lockout** [FR-1.1]
  - Done when: tests for valid/invalid login, lockout after 5 fails
- [ ] **6.3 JWT cookie sessions + CSRF + expiry** [FR-1.5]
  - Done when: expired token rejected; cookie flags verified
- [ ] **6.4 RBAC dependencies (`require_role`, `require_scope`)** [FR-1.2, FR-1.3]
  - Done when: automated role × route matrix test passes (analyst gets 403 on admin routes)
- [ ] **6.5 User management (admin) + audit log** [FR-1.4]
  - Done when: create/deactivate/role change/reset password work and are audited

### Data/API
- [ ] **6.6 Impala query layer** (parameterized, scope-checked, row-limited) [FR-1.3]
  - Done when: injection attempts rejected; out-of-scope dataset returns 403
- [ ] **6.7 Data endpoints**: observations, stats, anomalies, forecasts, correlations, datasets
  - Done when: each returns expected data for sample set
- [ ] **6.8 Admin ingestion UI/endpoints** (upload, trigger, jobs list) [FR-2.8]
  - Done when: uploading a file from the portal results in curated data + job record

### Alerts
- [ ] **6.9 Alert rule CRUD (admin)** [FR-8.2]
  - Done when: create/edit/disable rule via UI/API without restart
- [ ] **6.10 Rule evaluator on stream + new anomalies** (value + consecutive-anomaly rules, cooldown) [FR-5.3, FR-8.1]
  - Done when: injected spike triggers exactly one alert per cooldown window
- [ ] **6.11 Notifications**: in-app (< 10 s) and email via SMTP/mail catcher [FR-8.3]
  - Done when: demo alert appears in portal and in mailbox
- [ ] **6.12 Alert history + acknowledge** [FR-8.4]
  - Done when: status transitions saved with user and time

### Support
- [ ] **6.13 Feedback/support form + ticket list/status/reply** [FR-9.1, FR-9.2]
  - Done when: user submits, admin replies, user sees reply

## Phase 7 — Visualization

- [ ] **7.1 Connect Tableau to Impala (ODBC/JDBC) or build extracts** [FR-7.4]
  - Done when: a worksheet shows data from `observations`
- [ ] **7.2 Overview dashboard**: time series, seasonal heatmap, station map, filters (date, region, variable) [FR-7.1, FR-7.2]
- [ ] **7.3 Anomalies dashboard**: anomaly timeline, map, top stations, z-score distribution
- [ ] **7.4 Forecast dashboard**: history + forecast with confidence bands, trend slopes
- [ ] **7.5 Correlation view**: heatmap + lagged correlation chart [FR-6.3]
- [ ] **7.6 Embed/link dashboards in portal**; fallback to Tableau Public/extracts if live embed not possible [FR-7.4]
  - Done when: dashboards load inside the portal for a logged-in analyst
- [ ] **7.7 Live view page (Plotly + SSE/polling)** [FR-5.1, FR-7.2]
  - Done when: values update without refresh during the simulated stream
- [ ] **7.8 Saved views / customizable filters** [FR-7.3] (SHOULD)
- [ ] **7.9 UI polish and responsiveness check** (laptop + tablet) [FR-7.3]
- [ ] 📄 **7.10 Screenshots** of every dashboard for the report

## Phase 8 — Non-functional hardening

- [ ] **8.1 Monitoring collector** (`ops/monitor.py`) → `metrics` → Admin Monitoring page [NFR-1]
  - Done when: page shows HDFS usage, YARN job durations, ingest throughput, API p50/p95, stream lag
- [ ] **8.2 TLS via Apache** (self-signed cert) + HTTP→HTTPS redirect [NFR-3]
  - Done when: portal only reachable over HTTPS
- [ ] **8.3 Encryption**: HDFS encryption zone (or documented equivalent), AES-GCM for PII fields, secrets via env [NFR-3]
  - Done when: checklist in `docs/security.md` ticked with evidence
- [ ] **8.4 Compliance mapping** (`docs/compliance.md`) [NFR-4]
- [ ] **8.5 Load balancing**: Apache balancer over 2 app instances; failover test [NFR-8]
  - Done when: stopping instance A leaves the site up
- [ ] **8.6 Horizontal scaling demo**: add DataNode/NodeManager (Docker), rebalance, show more tasks [NFR-7]
  - Done when: before/after screenshots + notes recorded
- [ ] **8.7 Backups**: `ops/backup.sh` (HDFS snapshot/distcp + mongodump) scheduled; **restore drill** performed [NFR-6]
  - Done when: restored copy verified and steps documented
- [ ] **8.8 Uptime tracking + maintenance banner** [NFR-5]
  - Done when: uptime % displayed; banner can be toggled by admin
- [ ] **8.9 Security test pass** (role matrix, lockout, token expiry, injection attempts) [design §11]
  - Done when: all security tests green; findings fixed or logged

## Phase 9 — Testing, documentation, and submission

### Testing
- [ ] **9.1 Run full test suite** (`pytest -q`) and fix failures
- [ ] **9.2 Acceptance walkthrough**: every spec.md criterion marked pass/fail with evidence in `docs/test-report.md`
- [ ] **9.3 End-to-end rehearsal** on a clean start of the cluster (cold start → ingest → process → ML → dashboard → alert)
  - Done when: full demo path runs without manual fixes

### Documentation
- [ ] 📄 **9.4 User documentation**: user guide, FAQ, tutorials (read a dashboard, create an alert rule, file a ticket) [NFR-11]
- [ ] 📄 **9.5 Developer documentation**: architecture, workflows, API reference, ML models, setup/run guide [NFR-12]
- [ ] 📄 **9.6 Project report** with: problem definition, design specifications, flowcharts + DFDs, source code (listing or appendix), test data used, installation instructions, benchmarks, test report
- [ ] **9.7 ReadMe.doc** listing all assumptions and deviations (from `docs/ASSUMPTIONS.md`)
- [ ] **9.8 Installation instructions** verified by following them on a clean environment

### Demo video [NFR-13]
- [ ] **9.9 Write demo script** (suggested order):
  1. Problem + architecture (1 min)
  2. Login as admin vs analyst; show RBAC (1 min)
  3. Ingest a batch file (with a broken file → quarantine) (1.5 min)
  4. Start stream; show live view (1 min)
  5. MapReduce job run in YARN + results in Impala (1.5 min)
  6. ML: anomalies, forecast, correlations; retraining/model version (1.5 min)
  7. Dashboards in Tableau/portal with filters (1.5 min)
  8. Trigger an alert; show in-app + email (1 min)
  9. Feedback ticket flow (0.5 min)
  10. Monitoring, backup, scaling/load-balancing highlights (1.5 min)
- [ ] **9.10 Record and edit video**; check audio and readability of text
  - Done when: video plays end to end with all requirement areas visible

### Packaging
- [ ] **9.11 Build submission zip**: report, source code, test data, ReadMe.doc, video (or link), install instructions
  - Done when: unzip on another folder/machine shows all items present
- [ ] **9.12 Optional: live hosted URL** (only if time allows)
- [ ] **9.13 Final checklist** against spec.md §7 deliverables, then submit to the eProjects Team

---

## Optional enhancements (only after Phase 9 is green)

- [ ] Region comparison + what-if forecast slider
- [ ] PDF/CSV export from dashboards (audit-logged)
- [ ] Climate-risk index per region
- [ ] Viewer role for external stakeholders
- [ ] Urdu/English UI toggle

---

## Progress summary (update as you go)

| Phase | Tasks | Done |
|---|---|---|
| 0 Setup | 9 | 0 |
| 1 Data | 5 | 0 |
| 2 Ingestion/storage | 10 | 0 |
| 3 MapReduce | 10 | 0 |
| 4 Real-time | 6 | 0 |
| 5 ML | 8 | 0 |
| 6 Portal/security/alerts | 13 | 0 |
| 7 Visualization | 10 | 0 |
| 8 NFR hardening | 9 | 0 |
| 9 Test/docs/submission | 13 | 0 |

## Suggested order if time is short

Keep the demo-critical path first: **0 → 1 → 2 → 3 (monthly_avg, anomaly) → 4 → 5.2/5.3 → 6.1–6.4, 6.9–6.11, 6.13 → 7.2–7.4 → 9**. Then return for the remaining MapReduce jobs, NFR hardening (8.x), and polish. Log anything cut in `docs/ASSUMPTIONS.md`.
