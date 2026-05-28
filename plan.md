# The Pasha Data Layer: A Step-by-Step Lab Plan

*A hands-on, phased plan that takes you from "I don't really understand data infrastructure" to "I can explain and operate every layer of this stack." It is built around constructing a small, real, working lakehouse on your own machine using the open-source primitives that Pasha's commercial tools (IOMETE, Ataccama, Dataiku) are built on top of.*

---

## How to use this plan

This is a sequence, not a menu. Each phase introduces one concept, asks you to learn the *why* behind it, then has you do something concrete that proves you understood it, and ends with a checkpoint question you should be able to answer out loud before moving on. Resist the urge to skip the "learn the concept" reading and jump to the commands — the commands are easy and the understanding is the entire point. If you can run every command but cannot answer the checkpoint questions, you have learned a tool, not the architecture, and interviews probe the architecture.

A realistic pace is one phase every two to three evenings, which puts the whole plan at roughly four to six weeks of part-time work. That is deliberately unhurried; you are building durable understanding, not racing a deadline.

A word on what you are building. By the end you will have a local pipeline that mirrors Pasha's data band end to end: a fake source database, change events flowing through Kafka, Spark jobs transforming raw data into clean analytical tables stored as Iceberg tables on MinIO object storage, Airflow orchestrating the whole thing on a schedule, Great Expectations gating data quality, and a notebook querying the result. It is small, but it is structurally the real thing.

A note before you start: tool versions and exact image tags move quickly, so when a command below pins a version, treat it as illustrative and check the current stable tag if something fails. The concepts are stable; the version numbers are not.

---

## Phase 0 — Foundations and your lab environment

**The concept to learn first.** Before any tooling, get crystal clear on the one idea the entire data layer exists to serve: the separation of transactional (OLTP) systems from analytical (OLAP) systems. Spend an hour reading about why databases tuned for fast single-row writes are bad at large analytical scans, and vice versa. Write yourself one paragraph, in your own words, explaining why Pasha cannot simply run its loss-ratio report directly against the AdInsure production database. If you can write that paragraph convincingly, you understand the reason the data layer exists, and everything afterward is mechanism.

**What to do.** Set up the lab environment, which for you is the comfortable part. Install Docker and Docker Compose if you do not have them, and confirm `kind` (Kubernetes-in-Docker) is available since you will use it later. Create a clean project directory with subfolders for each component you will add (`sources`, `kafka`, `storage`, `spark`, `airflow`, `quality`, `notebooks`). Initialize a git repository immediately and commit at the end of every phase — this becomes your portfolio artifact and your record of progress. Do not build everything at once; you will add one container per phase so that you always understand what each piece is doing.

**Checkpoint.** You can explain OLTP vs OLAP to a non-technical friend using the "running the business versus understanding the business" framing, and your lab repo exists with a clean structure and an initial commit.

---

## Phase 1 — The source system and the idea of Change Data Capture

**The concept to learn.** Learn what Change Data Capture (CDC) is and specifically what *log-based* CDC means. The key insight: every relational database keeps a write-ahead log (Postgres calls it the WAL, MySQL the binlog) recording every change. CDC reads that log and emits each insert, update, and delete as an event, rather than repeatedly querying the table. Understand *why this is gentler on the source* than a nightly full export — it adds almost no query load and it captures every change including deletes, which a naive "select everything modified today" query would miss. The broader industry context worth knowing: streaming CDC pipelines built on Kafka and Debezium are increasingly replacing nightly batch ETL precisely because they give sub-minute freshness without hammering the source.

**What to do.** Stand up a single Postgres container to act as your stand-in for AdInsure. Create two tables that mirror an insurer's reality: `policies` (policy id, product line, region, premium, start date) and `claims` (claim id, policy id, claim amount, status, event timestamp). Then write a small Python script — your "source simulator" — that continuously inserts new policies and claims and occasionally updates claim statuses, mimicking a live operational system. This script is important: it gives you a realistic, *changing* source rather than a static dump, which is what makes the rest of the pipeline meaningful.

```python
# source_simulator.py — generates a realistic, continuously-changing stream of
# insurance events into Postgres, so the downstream pipeline has live data to process.
import psycopg2, random, time, uuid
from datetime import datetime

# Connect to the local Postgres standing in for the AdInsure core system.
conn = psycopg2.connect("dbname=insurance user=postgres password=postgres host=localhost")
conn.autocommit = True
cur = conn.cursor()

product_lines = ["auto", "home", "health", "travel"]
regions = ["baku", "ganja", "sumqayit", "mingachevir"]

while True:
    # Most of the time we write a brand-new claim (an INSERT the CDC layer will capture).
    policy_id = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO policies (id, product_line, region, premium, start_date) "
        "VALUES (%s, %s, %s, %s, %s)",
        (policy_id, random.choice(product_lines), random.choice(regions),
         round(random.uniform(200, 2000), 2), datetime.now())
    )
    # Occasionally a claim is filed against an existing policy.
    if random.random() < 0.4:
        cur.execute(
            "INSERT INTO claims (id, policy_id, amount, status, event_ts) "
            "VALUES (%s, %s, %s, %s, %s)",
            (str(uuid.uuid4()), policy_id,
             round(random.uniform(100, 15000), 2), "open", datetime.now())
        )
    time.sleep(1)  # one event per second keeps the stream observable by eye
```

**Checkpoint.** You can explain why reading the transaction log is better than re-querying the table, and your simulator is writing live data into Postgres that you can watch grow with a simple `SELECT count(*)`.

---

## Phase 2 — Kafka as the ingestion backbone

**The concept to learn.** This is a big one, so give it real time. Learn the four ideas that make Kafka what it is. First, a *topic* is a named, append-only, durable log of events. Second, a topic is split into *partitions*, which are the unit of parallelism, and crucially, ordering is only guaranteed *within* a partition — this is why the choice of partition key matters (key your claim events by policy id and all events for one policy stay ordered). Third, *consumer groups* let many independent consumers read the same topic without stepping on each other, each tracking its own *offset* (its position in the log). Fourth, *retention* means Kafka holds data for a configured time window, which is the property that lets a downstream consumer crash, come back an hour later, and catch up with zero data loss. Map these onto your Kubernetes intuition: partitions are horizontal scale units, and *consumer lag* (how far behind a consumer is) is the single most important health metric — it is the data world's equivalent of a growing queue depth.

**What to do.** Add a Kafka broker to your Docker Compose (Redpanda is a lighter, Kafka-compatible option that is friendlier on a laptop, and learning on it transfers directly). Connect Debezium to your Postgres so that the changes your simulator is making now flow automatically onto a Kafka topic as CDC events. Then write a tiny consumer script that prints each event as it arrives, and *watch* the events appear in near-real-time as your simulator writes them. This visceral "I changed a row and an event popped out the other end" moment is when CDC stops being abstract.

After it works, do one deliberate learning exercise: stop your consumer for two minutes while the simulator keeps running, then restart it, and observe that it catches up from where it left off rather than losing data. You have just witnessed offsets and retention doing their job — write down what you saw.

**Where this maps in Pasha's stack.** This is exactly the Kafka that appears in both K8S-PRIVATE-PROD and K8S-DATA-PROD. The seam you just built — operational changes becoming a durable event stream — is the seam between Pasha's transactional world and its analytical world.

**Checkpoint.** You can explain partitions, offsets, consumer groups, and retention without notes, and you have personally demonstrated a consumer catching up after downtime.

---

## Phase 3 — Object storage and the Parquet file format

**The concept to learn.** Two distinct ideas here that beginners often blur together, so keep them separate in your mind. The first is *object storage*: a flat, infinitely scalable store of "objects" (files) addressed by key, accessed over an S3-compatible API, with no real folder hierarchy underneath. This is the cheap, durable foundation the whole lakehouse sits on, and decoupling storage from compute is what lets you spin compute up and down freely while the data stays put. The second is *Parquet*, a *columnar* file format: instead of storing row by row, it stores all the values of one column together, which means an analytical query that needs only three of fifty columns reads only those three, and the column-wise layout compresses far better. Understand the phrase "columnar storage enables column pruning and better compression" well enough to explain *why* it makes analytical scans fast.

**What to do.** Add MinIO to your Compose stack — it is S3-compatible object storage you run yourself, and it is genuinely the same thing as the "Minio Infra" already sitting in Pasha's K8S-INFRA cluster, so this is not a toy substitute but the real article at small scale. Create a bucket. Then, separately from the pipeline, do a small hands-on Parquet exercise in a notebook: load a chunk of your claims data into a pandas or Polars dataframe, write it once as CSV and once as Parquet, and compare the file sizes and the time to read back only two columns. Seeing the Parquet file be dramatically smaller and faster for a column-subset read makes the columnar concept concrete in a way no diagram can.

**Where this maps in Pasha's stack.** MinIO here is the S3 box in the IOMETE block, and Parquet is the Parquet box. You have now built the storage substrate of the lakehouse.

**Checkpoint.** You can explain why object storage is the right foundation for analytics, and you have measured for yourself why columnar Parquet beats row-based CSV for analytical reads.

---

## Phase 4 — Iceberg: turning files into a real table

**The concept to learn.** This is the conceptual heart of the entire data layer, so this is the phase to slow down on most. Internalize the single most important sentence in the whole stack: a folder of Parquet files is just a data *lake*, but add a *table format* on top and you have a *lakehouse* — without Iceberg you have a data lake, with it you have a lakehouse. Then learn the four capabilities Iceberg adds and, for each, *why an operator cares*. ACID transactions mean two jobs writing the same table concurrently do not corrupt each other — Iceberg achieves this with *optimistic commits*, where each write creates a new snapshot and atomically tries to publish it, and if someone else committed first, it simply retries. Schema evolution means you can add, rename, or drop a column without rewriting existing files. Time travel means you can query the table as it existed at a past snapshot, which is invaluable for auditing and for reproducing exactly what data a model was trained on. Partitioning plus partition pruning means a query for "March claims" physically skips every non-March file, turning an hours-long scan into a seconds-long one. Spend time here; this is what people mean by "lakehouse" and it is the concept most likely to come up in an interview.

**What to do.** Configure Spark (you will install it properly in the next phase, but you can start with a local Spark shell or PySpark now) with the Iceberg runtime and point its catalog at your MinIO bucket. Create your first Iceberg table and write a few thousand rows into it. Then, and this is the valuable part, *explore the metadata*: open the bucket in the MinIO console and look at what Iceberg actually wrote — the data files, the manifest files, and the metadata JSON that tracks snapshots. Seeing the snapshot structure with your own eyes demystifies time travel and ACID instantly, because you realize a "commit" is just Iceberg atomically pointing the table at a new metadata file.

**Where this maps in Pasha's stack.** This is the Iceberg box, and it is the layer IOMETE manages for them. When IOMETE talks about running compaction, maintaining the catalog, and enforcing encryption, it is operating exactly this layer on their behalf.

**Checkpoint.** You can explain the lake-versus-lakehouse distinction, describe all four Iceberg capabilities and why each matters operationally, and you have inspected real Iceberg metadata in object storage.

---

## Phase 5 — Spark transformations and the medallion architecture

**The concept to learn.** Two ideas. First, *Spark* as a distributed compute engine: it reads data, transforms it in parallel across partitions, and writes results, and critically it is *separate from* and *disposable relative to* the storage — the cluster can come and go while the Iceberg tables persist. Second, the *medallion architecture*: data flows through bronze (raw, exactly as ingested, no logic applied), silver (cleaned, deduplicated, conformed, joined), and gold (business-level aggregates ready for consumption). Learn *why* this layering exists: it makes pipelines debuggable, because when a number in a report looks wrong you walk back up the layers — is it wrong in gold, in silver, or already wrong in bronze? — and you find the break quickly. Without layering, every pipeline is one opaque jump from raw mess to final answer and is nearly impossible to debug.

**What to do.** Install Spark properly (the official Docker image, or run it inside your kind cluster to flex your Kubernetes muscle and mirror how IOMETE deploys Spark on Kubernetes). Now build the three medallion layers as actual Iceberg tables. The bronze job reads the raw CDC events off Kafka and lands them in an Iceberg table with essentially no transformation. The silver job reads bronze, deduplicates (CDC commonly delivers the same event more than once, so this is where idempotency starts to matter), parses fields into proper types, and joins claims to policies. The gold job aggregates silver into your business answer: loss ratio (total claims paid divided by total premium) grouped by product line and region. Write each as a separate, independently runnable Spark job — keeping them separate is the whole point.

```python
# gold_loss_ratio.py — the business-level aggregation, reading clean silver data
# and producing the table that will actually feed the dashboard.
from pyspark.sql import functions as F

# Read the conformed, cleaned silver tables (not the raw bronze) — we always
# build gold from silver so the layering stays debuggable.
claims = spark.table("lakehouse.silver.claims")
policies = spark.table("lakehouse.silver.policies")

# Join claims to their policies so each claim carries product line and region.
joined = claims.join(policies, claims.policy_id == policies.id)

# Loss ratio is total claims paid over total premium — the core insurance KPI.
gold = (joined
        .groupBy("product_line", "region")
        .agg(
            F.sum("amount").alias("total_claims"),
            F.sum("premium").alias("total_premium"))
        .withColumn("loss_ratio", F.col("total_claims") / F.col("total_premium")))

# Write the result as an Iceberg table; downstream tools all read this one table.
gold.writeTo("lakehouse.gold.loss_ratio_by_segment").createOrReplace()
```

**Where this maps in Pasha's stack.** Spark here is the Spark box managed by IOMETE, and the medallion tables live in the Iceberg/S3 storage you built in Phases 3 and 4. The Spark autoscaling and cluster management that IOMETE provides is exactly the operational burden you are feeling when you run these jobs by hand.

**Checkpoint.** You can explain compute–storage separation and the bronze/silver/gold layering with the "walk back up the layers to debug" justification, and your three jobs run in sequence to produce a real loss-ratio table.

---

## Phase 6 — Orchestration with Airflow

**The concept to learn.** Learn what an orchestrator does and, just as importantly, what it does *not* do — Airflow does not move data itself, it *coordinates* the jobs that do. Learn the DAG (Directed Acyclic Graph): a set of tasks with dependencies, where "acyclic" means pipelines have a clear start and end and never loop back. Then learn the single most important concept in all of data engineering, which finally becomes concrete here: *idempotency*. An idempotent task produces the same result whether it runs once or five times, so that when a task fails halfway and Airflow retries it, you do not get double-counted claims. Tie this back to your dedup step in silver — that is what makes the pipeline safely retryable. Also learn *backfills*: when you fix a bug or add a column, you must reprocess history, and Airflow's date-partitioned runs exist precisely for replaying the past. The analogy that will make this click for you: Airflow is to data pipelines what a dependency-aware, retry-capable CI/CD orchestrator is to deployments — a domain you already know.

**What to do.** Add Airflow to your stack and express your three Spark jobs as a single DAG: a sensor or simple wait that confirms fresh data has landed, then the bronze task, then silver, then gold, with a failure on any step halting the run and alerting. Schedule it to run on an interval. Then deliberately *break* one task (raise an exception in silver), watch Airflow stop the pipeline and retry per your retry policy, fix it, and watch it recover — this teaches you failure handling far better than reading about it. Finally, trigger a backfill for a past date range and watch it reprocess, so backfills stop being theoretical.

**Where this maps in Pasha's stack.** This is the Airflow PROD and Airflow STAGE in K8S-DATA-PROD. The fact that Pasha runs a separate STAGE instance is a sign of a mature promotion path, and now you understand what those two instances are *for* — testing DAG changes before they touch production data.

**Checkpoint.** You can explain DAGs, idempotency, and backfills clearly, you can articulate why the dedup step makes the pipeline safely retryable, and you have personally watched a pipeline fail, retry, and recover.

---

## Phase 7 — Data quality and governance

**The concept to learn.** Learn why, in a regulated insurer, data quality is a hard requirement rather than a nicety — a wrong number in a regulatory or solvency report is a serious problem, so data must be *trusted* before it is allowed downstream. Learn three patterns: *validation rules* (a claim amount is never negative; every claim references a real policy; a loss ratio above some sane bound is suspicious), the *quarantine pattern* (bad records are diverted and flagged rather than silently dropped or silently passed through), and the idea of a *quality gate* in the pipeline (if validation fails, the pipeline halts and the bad data never reaches gold). Also learn, conceptually, what *data catalog* and *lineage* mean — knowing where every number came from and what feeds off it — because that is a large part of what governance tooling provides.

**What to do.** Add Great Expectations (free and open source) as your quality engine — it is the learnable stand-in for Pasha's Ataccama, and the *concepts* transfer completely even though the product differs. Define a suite of expectations on your silver data: amounts non-negative, no orphan claims, statuses drawn from a known set. Wire the quality check into your Airflow DAG as a gate *between silver and gold*, so that if expectations fail, gold never runs. Then test it honestly: inject some bad rows in your simulator (negative claim amounts), and watch the gate catch them and halt the pipeline. Seeing bad data get stopped at the gate is the lesson.

**Where this maps in Pasha's stack.** Great Expectations stands in for Ataccama. When you discuss this in an interview, the framing to use is that you understand the *role* data quality plays — validation, gating, quarantine, lineage — and that Ataccama is the enterprise platform that provides it; you learned the discipline on the open-source equivalent.

**Checkpoint.** You can explain the quality gate and quarantine patterns and why they matter specifically for a regulated insurer, and you have watched your gate stop bad data from reaching the gold layer.

---

## Phase 8 — Consumption

**The concept to learn.** Learn the architectural elegance you are about to demonstrate: every consumer — a SQL dashboard, a data scientist's notebook, a future AI model — reads the *same* Iceberg gold tables, so there is one version of the truth and no copying data into yet another silo. Understand the difference between the heavyweight transformation engine (Spark, for building the tables) and a lightweight query engine (something like DuckDB or Trino, for fast interactive reads of the finished tables) — they serve different access patterns against the same storage.

**What to do.** Open a Jupyter notebook (your stand-in for Dataiku and the literal Jupyter in Pasha's IOMETE block) and query your gold loss-ratio table, then make a simple bar chart of loss ratio by product line. The point is not the chart; it is the realization that you are reading the very same Iceberg table your gold Spark job wrote, with no export step in between. That closes the loop from a live operational write in Phase 1 to an analytical answer here.

**Where this maps in Pasha's stack.** Your notebook stands in for Dataiku and Jupyter. The gold table you are querying is the same kind of table Pasha's analysts and data scientists consume, and — connecting forward to your AI ambitions — it is exactly the kind of curated table that would later feed a fraud-detection model or become the knowledge base for a retrieval-augmented assistant.

**Checkpoint.** You can articulate the "one version of the truth, many consumers" principle, and you have produced an analytical answer that traces all the way back to a live source write.

---

## Phase 9 — The operational experiments that make you credible

**The concept to learn.** This phase is where you move from "I built a pipeline" to "I can operate a lakehouse," and it is the most interview-relevant phase of all. The headline concept is the *small files problem and compaction*: a streaming pipeline that commits frequently creates thousands of tiny files, and query performance collapses under the metadata overhead, so the fix is *compaction* — periodically rewriting many small files into fewer large ones (a common target is around 512 MB each) — and committing in time windows of several minutes rather than per event. This single phenomenon is behind a huge fraction of real-world "the dashboard got slow over the last month" incidents, and being able to diagnose it sets you apart.

**What to do.** Run three deliberate experiments and write up what you observed for each, because the write-up is what you will draw on in interviews. First, the compaction experiment: configure your bronze job to commit very frequently so it produces many tiny files, measure how slow a query becomes and inspect the file count in MinIO, then run an Iceberg compaction (rewrite data files) and re-measure — the before/after speedup is the lesson, and now you can tell a real story about it. Second, the schema evolution experiment: add a new column to your claims table (say `fraud_flag`) and confirm that existing data is untouched and old queries still work, proving Iceberg evolved the schema without rewriting files. Third, the time-travel experiment: note a snapshot id, write more data, then query the table *as of* the earlier snapshot and watch the old state come back — this makes ACID and snapshots tangible and gives you a concrete auditing story.

**Where this maps in Pasha's stack.** These three experiments are precisely the maintenance work IOMETE automates for Pasha — compaction, schema management, snapshot handling. Understanding what is being automated, and what breaks when it isn't, is the difference between an operator and a button-pusher.

**Checkpoint.** You can tell a clear, personal story about diagnosing and fixing a small-files problem, and you have demonstrated schema evolution and time travel with your own hands.

---

## Phase 10 — Monitoring, Logging, and Pipeline Observability

**The concept to learn.** By the time your pipeline is running end-to-end you have a new problem: how do you know it is *still* running correctly an hour, a day, or a week from now? Learn the distinction between *metrics* (numeric time-series measurements — Kafka consumer lag, Spark job duration, records processed per second) and *logs* (structured text events emitted by a process at a point in time). Understand the three questions every data engineer should be able to answer without opening a shell: Is the pipeline running? Is it healthy? When did it last succeed? Then learn why *consumer lag* is the single most important health signal in a streaming pipeline — if lag is growing, your consumers are falling behind and data freshness is deteriorating, and diagnosing *why* (slow downstream processing versus a spike in upstream volume) is the real skill. The operational framing worth internalising: monitoring is not an afterthought bolted on after the pipeline works; it is what tells you the pipeline has *stopped* working before your stakeholders do.

**What to do.** Add Prometheus and Grafana to your kind cluster using the `kube-prometheus-stack` Helm chart, which installs both together with sensible defaults and pre-built Kubernetes dashboards. Redpanda exposes a Prometheus metrics endpoint natively — write a `ServiceMonitor` resource to scrape it and build a Grafana panel showing consumer lag for your `insurance.public.policies` and `insurance.public.claims` topics. Airflow exposes StatsD metrics; configure the StatsD exporter sidecar and create panels for DAG success rate, task duration, and last-run timestamp. For logs, add Loki (the Grafana-native log store) and Promtail as a DaemonSet to ship pod logs from your `data-lab` namespace, then wire Loki as a Grafana data source so you can query Debezium connector logs and Spark job output from the same UI you use for metrics — no `kubectl logs` required. Once the stack is up, do one deliberate exercise: stop the source simulator, wait five minutes, and watch consumer lag flatten in Grafana while the Airflow DAG eventually fails and turns red. That is the "the dashboard told you before a human noticed" moment, and it is the entire point of observability.

**Where this maps in Pasha's stack.** Pasha's K8S-INFRA cluster almost certainly runs a Prometheus and Grafana stack — monitoring is table stakes for any production Kubernetes environment. The consumer lag dashboard you just built is the exact panel an on-call engineer watches. Loki-based log aggregation maps directly to however Pasha centralises pod logs across its clusters. Being able to say "I wired up Prometheus scraping, built a consumer lag alert, and aggregated pipeline logs in Loki" is a concrete operational credential, not a theoretical one.

**Checkpoint.** You can explain the difference between metrics and logs, articulate why consumer lag is the most critical streaming health signal and how to diagnose its root cause, and you have a working Grafana dashboard that answers all three health questions — pipeline running, healthy, last success — from one UI without touching the shell.

---

## Phase 11 — Synthesis and interview readiness

**The concept to learn.** Nothing new — this phase is consolidation. The goal is that you can stand at a whiteboard and draw Pasha's entire data band from memory, narrating one sentence per stage about why it exists and what breaks without it.

**What to do.** Three things. First, draw the full data flow from AdInsure to dashboard on paper, from memory, and check it against the diagram. Second, write the README for your lab repository as if explaining the project to a hiring manager — what you built, why each layer exists, and what the three operational experiments taught you; this is now a genuine portfolio piece. Third, rehearse out loud the answers to the questions an interviewer is most likely to ask, which are gathered in the checklist below. Being able to *teach* each concept, not just recognize it, is the bar.

**The interview-readiness checklist — be able to explain each from memory:** OLTP versus OLAP and why they are separated; log-based CDC and why it is gentler than full extracts; Kafka partitions, consumer groups, offsets, and lag; the difference between a file format (Parquet) and a table format (Iceberg); Iceberg's ACID/optimistic-commit model, schema evolution, time travel, and partition pruning; compute–storage separation; the medallion (bronze/silver/gold) layering and its debugging rationale; Airflow DAGs, idempotency, and backfills; the data-quality gate and quarantine pattern and why regulation makes them mandatory; the small-files problem and compaction; and the difference between metrics and logs, why consumer lag is the primary streaming health signal, and how Prometheus, Grafana, and Loki fit together as an observability stack. The bridge sentence that ties it together for an insurer: this stack is self-hosted for *data sovereignty and regulatory compliance* (frameworks like GDPR and DORA), keeping data processing inside Pasha's own infrastructure — that is the *why* behind the architecture, not just the *what*.

---

## A summary of the open-source-to-Pasha mapping

As you build, keep this correspondence in mind so the lab always points back at the real job. Your Postgres source stands in for AdInsure and 1C; Kafka (or Redpanda) is literally Pasha's Kafka; MinIO is literally Pasha's Minio Infra; Parquet, Iceberg, and Spark are the open primitives that IOMETE manages for Pasha; Airflow is literally Pasha's Airflow; Great Expectations stands in for Ataccama; and your Jupyter notebook stands in for Dataiku and Jupyter. You are not learning toys — you are learning the real components, with three of them wrapped by commercial platforms you will recognize on day one.

*Remember to re-verify current image tags and version-specific syntax as you go; the concepts in this plan are durable, but the exact commands drift over time.*    