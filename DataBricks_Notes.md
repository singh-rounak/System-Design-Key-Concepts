# Data Engineering Interview Master Notes
## Databricks + Apache Spark + PySpark + Delta Lake + Kafka + Azure + Lakehouse + Streaming + Governance + System Design

> **Target:** Data Engineer II / Senior Data Engineer interviews, with special emphasis on a Databricks + Kafka + Delta Lake + Azure stack.
>
> **Interview rule:** Do not memorize product names. Be able to explain **why the component exists, what problem it solves, what happens when it fails, and what trade-off you made**.

---

# 0. THE ONE-PAGE MENTAL MODEL

```text
                         ┌────────────────────────────────────────────┐
                         │              MICROSOFT AZURE                │
                         │  Identity | Network | Storage | Monitoring │
                         │  Key Vault | Event Hubs | ADF | DevOps     │
                         └───────────────────┬────────────────────────┘
                                             │
              ┌──────────────────────────────┴──────────────────────────────┐
              │                       DATABRICKS                            │
              │                                                              │
              │  ┌──────────────┐   ┌──────────────┐   ┌─────────────────┐ │
Sources ─────►│  │ Spark/        │   │ Delta Lake   │   │ Unity Catalog   │ │
              │  │ PySpark       │◄─►│ Storage      │◄─►│ Governance      │ │
              │  └──────┬───────┘   └──────────────┘   └─────────────────┘ │
              │         │                                                   │
              │  ┌──────▼────────┐   ┌───────────────────────────────────┐ │
              │  │ Structured    │   │ Lakeflow Pipelines / Jobs         │ │
              │  │ Streaming     │   │ Orchestration + Data Quality      │ │
              │  └──────┬────────┘   └───────────────────────────────────┘ │
              │         │                                                   │
              │  Bronze ─────► Silver ─────► Gold ─────► BI / ML / Apps     │
              └─────────────────────────────────────────────────────────────┘
                       ▲
                       │
                 Kafka / Event Hubs
```

### Remember the layers

| Layer | Think of it as | Main job |
|---|---|---|
| Azure | Cloud foundation | Infrastructure, networking, identity, storage |
| Kafka | Event backbone | Durable, replayable event stream |
| Databricks | Data platform | Compute + engineering + analytics + AI |
| Spark | Distributed engine | Parallel processing |
| PySpark | Python API | Write Spark workloads in Python |
| Delta Lake | Storage/table layer | ACID + versioning + reliable lake tables |
| Unity Catalog | Governance plane | Access, discovery, lineage, metadata |
| Lakeflow Pipelines | Declarative pipelines | Build data transformations + quality |
| Lakeflow Jobs | Orchestration | Schedule/depend/retry tasks |
| SQL Warehouse | SQL compute | BI/SQL workloads |
| Photon | Native execution engine | Accelerate supported SQL/DataFrame workloads |

**The most important relationship:**

> **Spark computes. Delta stores reliably. Unity Catalog governs. Kafka transports events. Databricks integrates them. Azure provides the cloud foundation.**

---

# 1. INTERVIEW PRIORITY

## Tier 1 — Must be strong

1. SQL
2. Spark execution model
3. PySpark transformations/actions
4. Partitioning and shuffle
5. Joins and skew
6. Delta Lake
7. Kafka fundamentals
8. Structured Streaming
9. Databricks architecture
10. Medallion architecture
11. Unity Catalog
12. Data quality
13. Idempotency and exactly-once reasoning
14. System design
15. Batch vs streaming
16. Performance and cost optimization

## Tier 2 — Very likely

- Auto Loader
- Lakeflow Pipelines / former Delta Live Tables
- Lakeflow Jobs
- CDC
- MERGE
- SCD Type 1/2
- Time travel
- OPTIMIZE / file layout
- Photon
- Cluster/compute choices
- Monitoring
- CI/CD
- Terraform
- Git
- Airflow integration
- Azure Data Factory
- Azure Event Hubs
- ADLS Gen2
- Key Vault
- Service principals

## Tier 3 — JD-specific / differentiators

- Real-time operational state
- Time-series data
- Digital twin architecture
- Industrial telemetry
- Feature engineering
- ML data pipelines
- Teradata / warehouse integration
- Data profiling and anomaly detection
- Production incident handling
- Backfills and replay
- Disaster recovery

---

# 2. STACK LOGOS / QUICK IDENTIFICATION

![Apache Kafka](https://cdn.simpleicons.org/apachekafka/231F20) ![Apache Spark](https://cdn.simpleicons.org/apachespark/E25A1C) ![Databricks](https://cdn.simpleicons.org/databricks/FF3621) ![Microsoft Azure](https://cdn.simpleicons.org/microsoftazure/0078D4) ![Python](https://cdn.simpleicons.org/python/3776AB) ![Apache Airflow](https://cdn.simpleicons.org/apacheairflow/017CEE) ![Terraform](https://cdn.simpleicons.org/terraform/844FBA) ![GitLab](https://cdn.simpleicons.org/gitlab/FC6D26)

---

# 3. AZURE — WHAT YOU ACTUALLY NEED TO KNOW

## Core services

### ADLS Gen2
Cloud object storage used as the lake storage layer.

```text
Sources
   │
   ▼
ADLS Gen2
   │
   ├── bronze/
   ├── silver/
   └── gold/
```

Know:

- containers/filesystem
- hierarchical namespace
- RBAC
- ACLs
- encryption
- lifecycle policies
- hot/cool/archive concepts
- integration with Databricks
- managed vs external data locations

### Azure Event Hubs

Managed event ingestion service.

Conceptually similar to Kafka for many Azure workloads.

```text
Applications / IoT
       │
       ▼
Azure Event Hubs
       │
       ▼
Databricks Structured Streaming
```

**Do not say:** "Event Hubs is Kafka."

Better:

> Event Hubs is an Azure-native event streaming service with Kafka protocol compatibility, but Kafka itself is an open-source distributed event streaming platform.

### Azure Data Factory

Primarily orchestration and data integration.

```text
ADF
 │
 ├── schedule
 ├── dependencies
 ├── trigger
 └── invoke Databricks
          │
          ▼
      Spark job
```

**ADF ≠ Spark.**

ADF coordinates; Spark performs distributed transformation.

### Key Vault

Secrets/credentials.

Never hard-code:

```python
password = "SuperSecret123"
```

Use managed identities/service principals/secret scopes/Key Vault integration as appropriate.

### Microsoft Entra ID

Identity and access management.

### Azure DevOps

Git repositories, pipelines, CI/CD, work items, etc.

---

# 4. APACHE SPARK

![Apache Spark](https://cdn.simpleicons.org/apachespark)

## What is Spark?

Apache Spark is a distributed data processing engine.

It splits work across multiple machines.

```text
                Driver
                  │
           Logical / Physical Plan
                  │
              Scheduler
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
    Executor   Executor   Executor
      │           │          │
    tasks       tasks      tasks
      │           │          │
      └───────────┼──────────┘
                  ▼
              Output
```

## Driver

Responsible for:

- application coordination
- building execution plan
- scheduling tasks
- maintaining SparkSession/context
- collecting results when requested

## Executors

Perform:

- computation
- task execution
- caching
- shuffle operations

### Interview trap

**Q: Does the driver process all the data?**

No.

The driver coordinates. Executors process distributed data.

`collect()` can bring large data to the driver and cause driver OOM.

---

# 5. SPARK LAZY EVALUATION

Transformations build a plan.

Actions execute it.

### Transformations

```python
df2 = (
    df
    .filter("amount > 100")
    .select("customer_id", "amount")
)
```

Nothing necessarily runs yet.

### Actions

```python
df2.count()
df2.show()
df2.write.format("delta").save(...)
```

Actions trigger execution.

### Why lazy evaluation?

Spark can optimize the complete plan before executing it.

---

# 6. NARROW VS WIDE TRANSFORMATIONS

## Narrow

Data does not need to move between partitions.

Examples:

- filter
- select
- map
- withColumn

```text
P1 ──► P1
P2 ──► P2
P3 ──► P3
```

## Wide

Requires redistribution/shuffle.

Examples:

- groupBy
- join
- distinct
- repartition
- orderBy

```text
P1 ─┐
P2 ─┼──► SHUFFLE ──► New partitions
P3 ─┘
```

### Interview answer

> Wide transformations are usually more expensive because they involve network transfer, serialization and potentially disk spill.

---

# 7. SHUFFLE

Shuffle redistributes records between executors based on keys.

Example:

```python
df.groupBy("customer_id").sum("amount")
```

All records for the same customer must reach the same aggregation partition.

### Why shuffle is expensive

- network I/O
- serialization
- disk spill
- CPU
- memory pressure
- more tasks/stages

### Slow Spark job checklist

Ask:

1. Where is the shuffle?
2. How much data is shuffled?
3. Is there skew?
4. Can filtering happen earlier?
5. Can aggregation happen earlier?
6. Can a small table be broadcast?
7. Are there too many partitions?
8. Are there too many tiny files?
9. Is the query scanning unnecessary columns?

---

# 8. PARTITIONING

Partitioning is parallelism.

```text
100 GB
  │
  ├── P0
  ├── P1
  ├── P2
  ├── ...
  └── P99

100 tasks can potentially process partitions in parallel.
```

### Too few partitions

- under-utilized cluster
- poor parallelism
- long-running tasks

### Too many

- task scheduling overhead
- tiny tasks
- tiny files
- metadata overhead

### Repartition vs coalesce

`repartition(n)`:

- reshuffles
- can increase/decrease partitions

`coalesce(n)`:

- generally avoids full shuffle
- mainly reduces partitions

---

# 9. DATA SKEW

Suppose:

```text
customer_id = 999 → 80% of records

Partition 1 → 80 GB
Partition 2 → 2 GB
Partition 3 → 2 GB
...
```

One task becomes the straggler.

### Solutions

- broadcast small dimension
- pre-aggregate
- salting
- isolate hot keys
- better partitioning
- Spark/Databricks skew handling
- reduce data before join

### Trick question

**Q: More partitions always solve skew?**

No.

If the same hot key is routed to the same partition, you can simply create more partitions without fixing the hot key.

---

# 10. SPARK JOINS

## Broadcast join

If one side is genuinely small:

```text
Large fact ───────────────► Executors
Small dimension ─► broadcast to executors
```

Avoids a large shuffle of the fact table.

```python
from pyspark.sql.functions import broadcast

result = fact.join(
    broadcast(dim),
    "customer_id"
)
```

### Do not blindly broadcast

A table that looks small may exceed executor memory.

---

# 11. CATALYST OPTIMIZER

Spark SQL/DataFrame operations produce a logical plan.

Conceptually:

```text
Parsed Logical Plan
        ↓
Analyzed Logical Plan
        ↓
Optimized Logical Plan
        ↓
Physical Plan
        ↓
Execution
```

Important ideas:

- predicate pushdown
- projection pruning
- join strategy selection
- constant folding
- expression optimization

### Interview trap

**Q: Is Catalyst the execution engine?**

No.

Catalyst is Spark SQL's optimizer.

---

# 12. PYSPARK

![Python](https://cdn.simpleicons.org/python)

PySpark is the Python API for Spark.

## Prefer DataFrame API over Python loops

Bad:

```python
for row in rows:
    ...
```

Better:

```python
df.groupBy("customer_id").agg(...)
```

Spark needs operations it can optimize and distribute.

## Common operations

```python
df.select(...)
df.filter(...)
df.where(...)
df.withColumn(...)
df.drop(...)
df.groupBy(...)
df.join(...)
df.orderBy(...)
df.repartition(...)
df.dropDuplicates(...)
```

## Avoid UDFs when built-in functions exist

Built-in Spark functions are generally more optimizable and avoid Python serialization overhead.

---

# 13. DATabricks

![Databricks](https://cdn.simpleicons.org/databricks)

Databricks is a cloud data + AI platform built around technologies including Apache Spark, Delta Lake and Unity Catalog.

Think:

```text
Spark      → compute
Delta      → reliable tables/storage
Unity      → governance
Databricks → integrated platform
```

## Lakehouse

Combines useful properties of:

- data lakes
- data warehouses

```text
             Lakehouse
          ┌─────────────┐
          │   BI / SQL  │
          │     ML      │
          │ Streaming   │
          └──────┬──────┘
                 │
            Delta Tables
                 │
          Cloud Object Store
```

---

# 14. MEDALLION ARCHITECTURE

```text
Raw Sources
    │
    ▼
┌──────────┐
│  BRONZE  │  Raw / minimally transformed
└────┬─────┘
     │
     ▼
┌──────────┐
│  SILVER  │  Cleaned / validated / deduplicated
└────┬─────┘
     │
     ▼
┌──────────┐
│   GOLD   │  Business-ready / aggregates
└────┬─────┘
     │
     ├── BI
     ├── ML
     ├── APIs
     └── Operational systems
```

### Bronze

Preserve source fidelity.

### Silver

Clean and conform.

### Gold

Business semantics.

### Interview trap

**Q: Must every pipeline have exactly three layers?**

No.

Medallion is a pattern, not a law. Some systems need more/fewer layers.

---

# 15. DELTA LAKE

![Delta Lake](https://docs.delta.io/latest/_static/Delta-Lake-logo.png)

## What is Delta Lake?

Open-source storage/table layer that extends Parquet with a transaction log.

```text
Cloud Object Storage
│
├── part-000.parquet
├── part-001.parquet
├── part-002.parquet
│
└── _delta_log/
      ├── 000000.json
      ├── 000001.json
      ├── 000002.json
      └── checkpoints
```

### Key idea

> Delta = Parquet data files + transaction log + table protocol/metadata.

---

# 16. WHY DELTA?

Traditional data lake:

```text
CSV / JSON / Parquet
       ↓
Files
       ↓
Problem:
- partial writes
- schema drift
- duplicate data
- hard updates
- no reliable transactions
```

Delta:

```text
Parquet + Transaction Log
       ↓
ACID
Versioning
Schema enforcement
MERGE
UPDATE
DELETE
Time travel
Streaming + batch
```

---

# 17. ACID

### Atomicity

Transaction happens completely or not at all.

### Consistency

Table remains valid according to its constraints/schema/protocol.

### Isolation

Concurrent operations should not expose inconsistent intermediate states.

### Durability

Committed data survives failures according to the storage system's guarantees.

### Trick question

**Q: Does ACID mean no pipeline can ever lose data?**

No.

ACID protects committed table transactions. Source delivery, application logic, checkpoints, networking, external systems and business semantics still matter.

---

# 18. DELTA TIME TRAVEL

```sql
SELECT *
FROM sales VERSION AS OF 10;
```

or:

```sql
SELECT *
FROM sales
TIMESTAMP AS OF '2026-09-01 10:00:00';
```

Useful for:

- auditing
- debugging
- reproducibility
- rollback
- investigating accidental writes

### Important

Time travel depends on retained transaction/data files.

`VACUUM` removes old files after retention rules.

---

# 19. DELTA MERGE

Used heavily for:

- CDC
- upserts
- SCD
- deduplication patterns

```sql
MERGE INTO target t
USING source s
ON t.id = s.id

WHEN MATCHED THEN
  UPDATE SET *

WHEN NOT MATCHED THEN
  INSERT *;
```

### Type 1 SCD

Overwrite current state.

### Type 2 SCD

Preserve history.

Typical fields:

```text
business_key
effective_from
effective_to
current_flag
record_hash
```

---

# 20. SCHEMA ENFORCEMENT VS SCHEMA EVOLUTION

## Enforcement

Rejects incompatible data.

Purpose:

> Protect table quality.

## Evolution

Allows controlled schema changes.

Examples:

- add new column
- compatible changes depending on operation/configuration

### Trick question

**Q: Schema evolution means "accept any schema."**

No.

Evolution is controlled. It should be deliberately configured and governed.

---

# 21. DELTA PERFORMANCE

## 1. Reduce scanned data

- select only required columns
- filter early
- exploit partition pruning
- use good table layout

## 2. Control small files

Too many tiny files:

```text
1 KB
3 KB
7 KB
2 KB
...
```

causes metadata/task overhead.

Use appropriate compaction/optimization.

## 3. OPTIMIZE

Compacts files and can improve layout.

## 4. Z-Ordering

Historically used to colocate related data for data skipping on selected columns.

Do not say:

> "Z-order is a replacement for partitioning."

It is not.

## 5. Partition carefully

Bad:

```text
PARTITION BY user_id
```

when user_id has millions of values.

Better partition on appropriate low/moderate-cardinality access patterns such as date, depending on workload.

---

# 22. VACUUM

`VACUUM` removes old data files no longer needed by Delta after the retention threshold.

### Trick question

**Q: VACUUM deletes old Delta table versions immediately?**

No.

It removes obsolete files after retention. Time travel to versions requiring deleted files will no longer work.

---

# 23. DELTA CHANGE DATA FEED (CDF)

CDF exposes row-level changes between Delta table versions.

Useful for:

- incremental downstream processing
- CDC-like pipelines
- audit
- synchronization

Think:

```text
Version 10
   ↓
Version 11
   ↓
CDF
   ├── insert
   ├── update
   └── delete
```

---

# 24. KAFKA

![Apache Kafka](https://cdn.simpleicons.org/apachekafka)

Kafka is a distributed event streaming platform.

It is designed for:

- high throughput
- durable event logs
- replay
- decoupling producers/consumers
- scalable streaming

---

# 25. KAFKA CORE CONCEPTS

| Concept | Meaning |
|---|---|
| Broker | Kafka server |
| Cluster | Group of Kafka servers |
| Topic | Named event stream |
| Partition | Ordered log within a topic |
| Record | Individual event |
| Offset | Position of record within partition |
| Producer | Writes events |
| Consumer | Reads events |
| Consumer group | Parallel consumption group |
| Replication factor | Number of copies |
| Leader | Replica handling partition requests |
| Follower | Replica maintaining a copy |

---

# 26. KAFKA ARCHITECTURE

```text
                    Kafka Cluster

Producer A ───────► Topic: vehicle_events
Producer B ───────►
                         │
                ┌────────┼────────┐
                ▼        ▼        ▼
              P0       P1       P2
             Broker1  Broker2  Broker3
                │        │        │
                └──── replicated ─┘
                         │
             ┌───────────┴───────────┐
             ▼                       ▼
       Consumer Group A        Consumer Group B
       Spark Streaming         Alert Service
```

---

# 27. TOPIC vs PARTITION

Topic:

> Logical stream.

Partition:

> Physical ordered log that provides scalability.

```text
Topic: orders

P0: 0 1 2 3 4 5
P1: 0 1 2 3 4
P2: 0 1 2 3 4 5 6
```

### Critical interview fact

Kafka guarantees ordering **within a partition**, not across the whole topic.

---

# 28. KAFKA KEY

If:

```text
key = vehicle_id
```

Kafka's partitioning strategy can route events with the same key to the same partition.

This preserves per-key ordering.

### Example

```text
Vehicle A → P0
Vehicle A → P0
Vehicle A → P0

Vehicle B → P1
Vehicle B → P1
```

### Trick question

**Q: Same key guarantees global ordering?**

No.

It gives ordering within the relevant partition, not global ordering across all partitions.

---

# 29. CONSUMER GROUPS

Suppose:

```text
Topic = 6 partitions
Consumer group = 3 consumers
```

Partitions are distributed across consumers.

```text
Consumer 1 → P0 P1
Consumer 2 → P2 P3
Consumer 3 → P4 P5
```

### Critical rule

Within a consumer group, a partition is assigned to at most one consumer at a time.

### Trick

6 partitions + 10 consumers:

> At most 6 consumers can actively consume partitions at once for that group.

Extra consumers are idle.

---

# 30. SAME TOPIC + DIFFERENT GROUPS

```text
Topic
 │
 ├── Consumer Group A → Analytics
 │
 ├── Consumer Group B → Fraud detection
 │
 └── Consumer Group C → Archive
```

Each group can independently consume the same events.

This is Kafka's publish-subscribe behavior.

---

# 31. OFFSETS

Offset identifies position in a partition.

```text
P0:
offset 0 → event A
offset 1 → event B
offset 2 → event C
offset 3 → event D
```

Consumer stores progress.

If consumer crashes:

```text
last committed offset
       ↓
restart
       ↓
resume/re-read
```

This is central to replay and recovery.

---

# 32. KAFKA DELIVERY SEMANTICS

### At most once

May lose messages; no duplicates.

### At least once

Messages are not intentionally lost, but duplicates can occur.

### Exactly once

Requires coordinated semantics across producer/processing/sink.

### Brutally important interview answer

Do not say:

> "Kafka guarantees exactly once."

Say:

> Kafka supports idempotent and transactional producer semantics, but end-to-end exactly-once processing depends on the entire source-processing-sink design.

---

# 33. REPLICATION

```text
Partition P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

Replication provides fault tolerance.

If leader fails, another replica can become leader depending on cluster state.

### Replication factor

If RF = 3:

```text
3 copies
```

It does **not** mean:

> 3 consumers.

---

# 34. KAFKA ACKS

Producer acknowledgement controls durability/latency trade-offs.

Conceptually:

- `acks=0` → don't wait for broker acknowledgement
- `acks=1` → leader acknowledges
- `acks=all` → wait for required replicas according to ISR/configuration

### Trick

Higher durability usually costs latency/throughput.

---

# 35. KAFKA RETENTION

Kafka is not simply:

> "a queue where data disappears after being consumed."

Consumers track offsets independently.

Messages remain according to retention policies.

Possible retention dimensions include:

- time
- size

This enables replay.

---

# 36. KAFKA vs TRADITIONAL QUEUE

| Kafka | Traditional MQ |
|---|---|
| Distributed log | Queue/message broker model |
| Replayable | Often consumption-oriented |
| Partitions | Queue/work distribution |
| High throughput | Often optimized for messaging patterns |
| Long retention | Usually shorter retention |
| Consumer groups | Competing consumers / subscriptions |
| Stream processing ecosystem | Messaging-centric ecosystem |

Don't claim one is universally better.

---

# 37. KAFKA KRAFT

Modern Kafka uses **KRaft**.

Kafka 4.x removed ZooKeeper mode.

```text
Old:
Kafka Brokers ↔ ZooKeeper

Modern:
Kafka Brokers ↔ KRaft Controllers
```

### Interview trap

**Q: What does ZooKeeper do in current Kafka 4.x?**

For Kafka 4.x, ZooKeeper is not used; Kafka operates in KRaft mode.

---

# 38. KAFKA CONNECT

Kafka Connect moves data between Kafka and external systems.

```text
Database ──► Kafka Connect Source ──► Kafka
Kafka ──► Kafka Connect Sink ──► Database / S3 / etc.
```

Useful for CDC and integration.

---

# 39. KAFKA SCHEMA REGISTRY

Schema Registry manages event schemas, commonly with formats such as:

- Avro
- Protobuf
- JSON Schema

Why?

Without schema discipline:

```text
Producer A → customer_id
Producer B → customerId
Producer C → cust_id
```

Schema contracts reduce integration chaos.

---

# 40. KAFKA INTERVIEW TRAPS

### Q: Kafka is a database?

No. It is a distributed event streaming platform/log.

### Q: Kafka preserves global order?

No. Partition-level order.

### Q: More consumers always increase throughput?

No. Only up to available partitions.

### Q: Replication factor = number of consumers?

No.

### Q: Kafka deletes data after consumption?

No. Retention controls deletion.

### Q: Consumer reads a message and it disappears?

No.

### Q: Exactly once automatically solves duplicate business effects?

No. Downstream side effects still need careful design.

---

# 41. STRUCTURED STREAMING

Spark Structured Streaming treats streaming computation using DataFrame/Spark SQL semantics.

```text
Kafka
  │
  ▼
readStream
  │
  ▼
Transformations
  │
  ▼
writeStream
  │
  ▼
Delta
```

Example:

```python
events = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", brokers)
         .option("subscribe", "vehicle_events")
         .load()
)
```

---

# 42. CHECKPOINTING

Checkpoint stores progress/state needed for recovery.

```text
Kafka
  │
  ▼
Spark Streaming
  │
  ├── checkpoint
  │
  ▼
Delta
```

If the streaming job fails:

```text
failure
  ↓
restart
  ↓
checkpoint
  ↓
resume
```

### Never put checkpoint data casually inside your table data directory when governance/storage conventions require a managed external location.

---

# 43. EVENT TIME vs PROCESSING TIME

### Event time

When event actually happened.

### Processing time

When system processed it.

Example:

```text
Vehicle sensor event:
event_time = 10:00

Network delay = 2 minutes

processing_time = 10:02
```

For analytics, event time often matters more.

---

# 44. LATE DATA

An event generated at 10:00 arrives at 10:10.

Streaming systems need a strategy.

Tools/concepts:

- watermarks
- event-time windows
- deduplication
- state management

### Interview answer

> I would define an allowed lateness policy based on the business SLA and event-time distribution, then use watermarking/stateful logic where appropriate.

---

# 45. WATERMARKING

Watermark tells the engine how much event-time history it expects to continue receiving.

Conceptually:

```text
Events:
10:01
10:02
10:05
10:03  ← late

Watermark = 10:04

State older than allowed threshold can eventually be cleaned.
```

### Trick

Watermark does not magically guarantee no late records.

It controls state/event-time handling.

---

# 46. STREAMING OUTPUT MODES

Know the concepts:

- append
- update
- complete

Use based on query/state semantics.

---

# 47. BATCH vs STREAMING

| Batch | Streaming |
|---|---|
| Bounded data | Unbounded/continuous |
| Simpler | More operationally complex |
| Higher latency | Lower latency |
| Easier recovery | Requires state/checkpoints |
| Often cheaper | Can be always-on |

### Key interview rule

Do **not** say:

> "Streaming is always better."

Ask:

1. What freshness SLA?
2. Is source event-driven?
3. Does business need seconds/minutes?
4. Can hourly/daily work?
5. What is the operational cost?

---

# 48. AUTO LOADER

Auto Loader incrementally ingests new files from cloud object storage.

```text
ADLS
 │
 ├── file1.json
 ├── file2.json
 └── file3.json
       │
       ▼
 Auto Loader
       │
       ▼
 Bronze Delta
```

It is designed to avoid repeatedly scanning/listing all files as the dataset grows.

### Typical use

```text
SFTP / applications
       ↓
ADLS landing
       ↓
Auto Loader
       ↓
Bronze Delta
```

---

# 49. CDC

Change Data Capture captures:

```text
INSERT
UPDATE
DELETE
```

Example:

```text
OLTP DB
  │
  ▼
CDC
  │
  ▼
Kafka
  │
  ▼
Spark
  │
  ▼
Delta MERGE
```

### Why CDC?

Instead of extracting a 2 TB table every hour:

```text
Full extract → 2 TB
```

process only:

```text
changes → 50 GB
```

---

# 50. REAL-WORLD ARCHITECTURE: INDUSTRIAL DIGITAL TWIN

This is the architecture you should be able to explain verbally.

```text
┌─────────────────────────────────────────────────────────────┐
│                  INDUSTRIAL ENVIRONMENT                     │
│                                                             │
│ Sensors | Machines | PLCs | ERP | MES | GPS | APIs        │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Kafka / Event   │
                    │ Streaming       │
                    └────────┬────────┘
                             │
                             ▼
                 ┌──────────────────────┐
                 │ Databricks Structured│
                 │ Streaming / Spark    │
                 └──────────┬───────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ Bronze Delta │
                    │ Raw events   │
                    └──────┬───────┘
                           │
                    validation
                    deduplication
                    enrichment
                           │
                           ▼
                    ┌──────────────┐
                    │ Silver Delta │
                    │ Operational  │
                    │ state        │
                    └──────┬───────┘
                           │
                  aggregations/features
                           │
                           ▼
                    ┌──────────────┐
                    │  Gold Delta  │
                    │ KPIs/features│
                    └──────┬───────┘
                           │
             ┌─────────────┼──────────────┐
             ▼             ▼              ▼
          Power BI        ML          Operational API
```

### Governance across everything

```text
                 Unity Catalog
                       │
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
     Tables         Lineage         Access
       │                               │
     Metadata                         RBAC
```

---

# 51. DIGITAL TWIN DATA MODEL

Think in terms of **entities + state + events + time**.

Example:

```text
Asset
 ├── asset_id
 ├── asset_type
 ├── location
 └── model

Telemetry Event
 ├── asset_id
 ├── event_time
 ├── temperature
 ├── vibration
 ├── pressure
 └── status

Operational State
 ├── asset_id
 ├── current_state
 ├── state_start_time
 └── updated_at
```

### Why this matters

A digital twin is not just a historical table.

It needs:

- current state
- event history
- temporal context
- relationships
- derived metrics

---

# 52. UNITY CATALOG

Unity Catalog is Databricks' governance layer.

Current namespace:

```text
metastore
   │
   └── catalog
         │
         └── schema
               │
               ├── table
               ├── view
               ├── volume
               ├── function
               └── model
```

Common identifier:

```text
catalog.schema.table
```

---

# 53. UNITY CATALOG DOES WHAT?

- access control
- metadata
- discovery
- lineage
- auditing/governance
- centralized organization
- fine-grained permissions

### Interview trap

**Q: Is Unity Catalog just a replacement for Azure Entra ID?**

No.

Entra ID is identity/authentication infrastructure.

Unity Catalog is a data/AI governance layer.

They work together.

---

# 54. UNITY CATALOG ACCESS

Think:

```text
WHO?
  ↓
Principal / group / service principal
  ↓
WHAT?
  ↓
catalog.schema.table
  ↓
WHICH PRIVILEGE?
  ↓
SELECT / MODIFY / USE CATALOG / USE SCHEMA ...
```

Use least privilege.

---

# 55. MANAGED vs EXTERNAL TABLES

### Managed

Databricks/Unity Catalog manages table storage lifecycle according to its managed-table model.

### External

Storage location remains explicitly controlled.

Use external when storage must remain at a specific path or external systems need direct access.

### Trick

**Q: External table means external to Databricks?**

No.

It means the underlying storage location is externally managed.

---

# 56. GOVERNANCE MENTAL MODEL

```text
Data
 │
 ├── Who owns it?
 ├── Who can read it?
 ├── Who can modify it?
 ├── Where did it come from?
 ├── Where is it used?
 ├── What schema does it have?
 ├── Is it sensitive?
 ├── How long is it retained?
 └── What is its SLA?
```

---

# 57. LAKEFLOW PIPELINES / DLT

### Important 2026 terminology

**Delta Live Tables (DLT)** is the former product name.

Current Databricks terminology is **Lakeflow pipelines**.

Existing DLT code remains relevant, so interviewers may still say "DLT."

### What it provides

Declarative pipeline development.

```text
Raw
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

with:

- dependencies
- quality expectations
- pipeline management
- streaming tables
- materialized views
- observability

---

# 58. DATA QUALITY EXPECTATIONS

Think:

```text
Incoming record
      │
      ▼
quality rules
 ┌────┼────┐
 ▼    ▼    ▼
valid  bad  quarantine
```

Example:

```text
customer_id IS NOT NULL
amount >= 0
event_time IS NOT NULL
```

### Good production strategy

Do not simply delete bad records.

Prefer:

```text
valid records → main pipeline
invalid records → quarantine/DQ table
```

with:

- reason
- timestamp
- source
- pipeline version
- raw payload/reference

---

# 59. LAKEFLOW JOBS

Used for workflow orchestration.

```text
Job
 │
 ├── Task A: ingest
 │
 ├── Task B: validate
 │
 ├── Task C: transform
 │
 ├── Task D: publish
 │
 └── Task E: notify
```

Supports:

- dependencies
- scheduling
- retries
- branching/control flow
- monitoring

### Airflow vs Lakeflow Jobs

Use Lakeflow Jobs when orchestration is primarily within Databricks.

Use Airflow when you need broader cross-platform orchestration.

Both can coexist.

---

# 60. AIRFLOW

![Apache Airflow](https://cdn.simpleicons.org/apacheairflow)

Airflow:

```text
DAG
 │
 ├── extract
 ├── transform
 ├── validate
 └── publish
```

Airflow is an orchestrator, not a distributed processing engine.

### Trick

**Q: Does Airflow process 10 TB itself?**

No.

It schedules/coordinates tasks that invoke systems capable of processing the data.

---

# 61. PHOTON

Photon is Databricks' native vectorized execution engine.

Conceptually:

```text
Spark API
   │
   ▼
Catalyst planning
   │
   ▼
Photon execution where supported
   │
   ▼
Fallback to Spark execution where needed
```

Benefits can include:

- faster SQL
- faster DataFrame operations
- optimized joins
- faster writes
- vectorized processing

### Trick

Photon is not a replacement for Spark's API.

It accelerates execution for supported workloads.

---

# 62. DATABRICKS COMPUTE

Know the distinction:

### All-purpose / interactive

For development/exploration.

### Jobs compute

For automated production workloads.

### SQL Warehouse

For SQL/BI.

### Serverless

Managed compute where available.

### Interview answer

> I would choose compute based on workload type, isolation, latency, concurrency, governance, reliability and cost—not simply because one option is faster.

---

# 63. SPARK MEMORY PROBLEMS

### Driver OOM

Common causes:

```python
df.collect()
df.toPandas()
```

on huge datasets.

### Executor OOM

Possible causes:

- large joins
- skew
- oversized partitions
- caching too much
- huge broadcast
- wide transformations

### Fix

Don't just increase memory.

First identify:

- skew
- partition size
- shuffle
- broadcast
- unnecessary data
- caching

---

# 64. SMALL FILE PROBLEM

```text
Bad:
1 MB × 1,000,000 files

Better:
appropriately sized files
```

Small files increase:

- metadata overhead
- task scheduling
- storage operations
- query latency

Causes:

- excessive streaming micro-batches
- over-partitioning
- many small writes

Solutions:

- compaction
- optimize write strategy
- appropriate trigger/batch size
- avoid high-cardinality partitioning

---

# 65. SQL — MUST KNOW

## Ranking

```sql
ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY order_date DESC
)
```

## Second highest

Know at least:

- `DENSE_RANK`
- `ROW_NUMBER`
- correlated/subquery alternatives

## Deduplication

```sql
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY business_key
             ORDER BY updated_at DESC
           ) AS rn
    FROM source
)
SELECT *
FROM ranked
WHERE rn = 1;
```

## Running total

```sql
SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY event_time
)
```

## Gaps and islands

Know the general window-function approach.

## Slowly changing dimensions

Know Type 1 vs Type 2.

---

# 66. SQL TRAPS

### WHERE vs HAVING

`WHERE` filters before aggregation.

`HAVING` filters after aggregation.

### LEFT JOIN trap

This:

```sql
SELECT *
FROM A
LEFT JOIN B
  ON A.id = B.id
WHERE B.status = 'ACTIVE';
```

can effectively eliminate unmatched rows and behave like an inner join for that condition.

If you want to preserve A rows:

```sql
LEFT JOIN B
  ON A.id = B.id
 AND B.status = 'ACTIVE'
```

---

# 67. DATA MODELING

## OLTP

Optimized for transactions.

Examples:

- orders
- payments
- inventory

## OLAP

Optimized for analytics.

## Star schema

```text
          Dim Customer
               │
               ▼
Dim Product → Fact Sales ← Dim Date
               ▲
               │
          Dim Store
```

### Fact

Measurements/events.

### Dimension

Descriptive context.

---

# 68. FACT GRAIN

One of the most important interview concepts.

Before designing a fact table, state:

> "The grain of this table is one row per ______."

Example:

> One row per vehicle telemetry event.

Without defining grain, duplicate/aggregation problems become likely.

---

# 69. NORMALIZATION vs DENORMALIZATION

Normalization:

- reduce redundancy
- protect consistency
- common in OLTP

Denormalization:

- faster reads
- simpler analytical queries
- potentially duplicated data

Don't blindly normalize everything.

---

# 70. IDEMPOTENCY

Definition:

> Running the same operation multiple times produces the same correct final state.

Example:

```text
Batch ID = 2026-09-09

First run → writes batch
Retry     → detects same batch ID
           → does not duplicate
```

Techniques:

- deterministic keys
- MERGE
- deduplication
- staging
- run IDs
- checkpoints
- transaction boundaries

---

# 71. EXACTLY-ONCE — THE INTERVIEW ANSWER

Do not say:

> "Spark gives exactly once."

Better:

> Exactly-once is an end-to-end property. I need to reason about source semantics, offsets/checkpoints, processing state, sink commit behavior and idempotent business effects.

This answer separates experienced engineers from people who memorized buzzwords.

---

# 72. FAILURE HANDLING

Assume:

```text
Source fails
Network fails
Executor fails
Driver fails
Kafka replays
Duplicate arrives
Schema changes
Late event arrives
Target is unavailable
```

Production design:

```text
Retry transient failures
        │
        ▼
Checkpoint progress
        │
        ▼
Idempotent writes
        │
        ▼
Quarantine bad data
        │
        ▼
Alert / observe
```

---

# 73. BACKFILL

A backfill processes historical data.

Example:

```text
Normal:
today → today

Backfill:
Jan 1 → Sep 8
```

Design pipeline with parameters:

```text
start_date
end_date
```

Avoid hard-coding "today".

Backfills should be:

- idempotent
- isolated
- observable
- restartable

---

# 74. REPLAY

Kafka:

```text
offset 100
   ↓
consumer failed
   ↓
reset/restart
   ↓
offset 100
   ↓
reprocess
```

Delta:

```text
historical version
   ↓
read/rebuild
   ↓
reprocess downstream
```

Replayability is a major advantage of event-driven/lakehouse systems.

---

# 75. OBSERVABILITY

Monitor four layers.

## Pipeline

- success/failure
- duration
- retries

## Data

- row counts
- duplicates
- nulls
- freshness
- schema drift

## Compute

- CPU
- memory
- shuffle
- spill
- task duration

## Business

- transaction count
- revenue
- sensor volume
- anomaly rates

### Important

A technically successful job can still be operationally broken if data is stale or wrong.

---

# 76. DATA FRESHNESS

Suppose:

```text
Pipeline status = SUCCESS
```

but:

```text
latest source event = 09:00
current time = 14:00
```

The pipeline is operationally unhealthy.

Monitor freshness SLA.

---

# 77. DATA QUALITY

Dimensions:

```text
Completeness
Accuracy
Consistency
Validity
Uniqueness
Timeliness
```

Example:

```text
Completeness → customer_id not null
Validity     → temperature within plausible range
Uniqueness   → event_id unique
Timeliness   → event arrives within SLA
```

---

# 78. CI/CD

Recommended pattern:

```text
Developer
   │
   ▼
Git branch
   │
   ▼
Pull Request
   │
   ├── unit tests
   ├── lint
   ├── SQL/PySpark tests
   └── security checks
   │
   ▼
Build
   │
   ▼
Deploy Dev
   │
   ▼
Integration tests
   │
   ▼
Deploy QA/Staging
   │
   ▼
Approval
   │
   ▼
Production
```

Tools:

- Git
- GitLab/Jenkins/Azure DevOps
- Terraform
- Docker
- Databricks Bundles/CLI/API where appropriate

---

# 79. TERRAFORM

Infrastructure as Code.

Instead of manually creating:

```text
cluster
storage
permissions
network
jobs
```

define them as code.

Benefits:

- repeatability
- version control
- review
- environment consistency
- rollback

---

# 80. SECRETS

Never:

```python
kafka_password = "..."
```

Use:

- secret management
- managed identity
- service principal
- Key Vault
- governed credentials

---

# 81. SECURITY

Production checklist:

```text
Identity
  ↓
Least privilege
  ↓
Encryption in transit
  ↓
Encryption at rest
  ↓
Secret management
  ↓
Network isolation
  ↓
Data access controls
  ↓
Audit / monitoring
```

---

# 82. REAL-WORLD PIPELINE #1 — E-COMMERCE

```text
Website / Mobile
       │
       ▼
     Kafka
       │
       ▼
Structured Streaming
       │
       ▼
Bronze Delta
       │
       ├── dedupe
       ├── validate
       └── parse
       ▼
Silver Delta
       │
       ├── customer state
       ├── product metrics
       └── sessionization
       ▼
Gold Delta
       │
       ├── Power BI
       ├── ML features
       └── recommendation system
```

### Why Kafka?

Events arrive continuously.

### Why Delta?

Reliable storage + replay/history + batch/stream unification.

### Why Spark?

Distributed transformation.

### Why Unity Catalog?

Governance/lineage/access.

---

# 83. REAL-WORLD PIPELINE #2 — MANUFACTURING

```text
PLC / Sensor
    │
    ▼
IoT Gateway
    │
    ▼
Kafka
    │
    ├──────────────► Alert service
    │
    ▼
Databricks Streaming
    │
    ▼
Bronze
    │
    ▼
Silver
    │
    ├── current machine state
    ├── anomaly detection
    └── maintenance events
    │
    ▼
Gold
    │
    ├── OEE
    ├── downtime
    ├── defect rate
    └── predictive maintenance features
```

---

# 84. REAL-WORLD PIPELINE #3 — CDC FROM DATABASE

```text
OLTP Database
     │
     ▼
CDC Connector
     │
     ▼
Kafka Topic
     │
     ▼
Spark Structured Streaming
     │
     ▼
Bronze Delta
     │
     ▼
MERGE
     │
     ▼
Silver Dimension
     │
     ├── Type 1
     └── Type 2
```

---

# 85. REAL-WORLD PIPELINE #4 — BATCH

```text
ERP / CRM / Files
       │
       ▼
ADF / ingestion
       │
       ▼
ADLS
       │
       ▼
Auto Loader / batch read
       │
       ▼
Bronze
       ▼
Silver
       ▼
Gold
       ▼
Databricks SQL / BI
```

---

# 86. KAFKA + DATABRICKS INTERVIEW SCENARIO

### Question

"Design a pipeline that processes 100,000 vehicle events/sec and provides operational state within 30 seconds."

### Answer structure

1. Clarify SLA.
2. Identify event schema.
3. Choose Kafka topic.
4. Choose partition key.
5. Estimate partitions.
6. Configure replication.
7. Consume using Structured Streaming.
8. Define checkpoint.
9. Deduplicate using event ID.
10. Use event time.
11. Handle late data.
12. Write Bronze.
13. Transform to Silver operational state.
14. Aggregate Gold.
15. Govern with Unity Catalog.
16. Monitor lag/freshness/errors.
17. Design replay/backfill.
18. Define failure behavior.
19. Cost-optimize.
20. Explain scaling strategy.

---

# 87. KAFKA PARTITION DESIGN

If events are:

```text
vehicle_id
event_time
payload
```

A reasonable key candidate:

```text
vehicle_id
```

Why?

Because stateful processing often needs ordering per vehicle.

But ask:

- Is vehicle_id evenly distributed?
- Are some vehicles much more active?
- Do we need global ordering?
- How many partitions?
- What throughput per partition?
- How will consumers scale?

---

# 88. REAL-TIME OPERATIONAL STATE

This is likely important for digital-twin work.

You may receive:

```text
Vehicle 101
10:00 speed=40
10:01 speed=42
10:02 speed=38
```

Current state:

```text
vehicle_id = 101
current_speed = 38
updated_at = 10:02
```

Historical events remain in Bronze/Silver.

Gold/current-state tables can represent the latest state.

---

# 89. TIME-SERIES CONCEPTS

Know:

- event time
- processing time
- window
- watermark
- late arrival
- interpolation
- resampling
- rolling average
- moving window
- anomaly detection
- retention
- high-cardinality time-series keys

Example:

```text
5-minute rolling average

10:00 → 40
10:01 → 42
10:02 → 41
10:03 → 45
10:04 → 47

AVG(10:00 ... 10:04)
```

---

# 90. SYSTEM DESIGN FRAMEWORK

When asked to design a data system:

```text
1. Requirements
       ↓
2. Scale
       ↓
3. Sources
       ↓
4. Ingestion
       ↓
5. Storage
       ↓
6. Processing
       ↓
7. Serving
       ↓
8. Governance
       ↓
9. Reliability
       ↓
10. Observability
       ↓
11. Security
       ↓
12. Cost
```

---

# 91. REQUIREMENTS QUESTIONS

Ask:

### Functional

- What data?
- What transformations?
- Who consumes it?

### Non-functional

- latency?
- throughput?
- availability?
- retention?
- consistency?
- cost?

### Data

- volume/day?
- events/sec?
- schema?
- cardinality?
- duplicates?
- late events?

### Business

- what happens if data is delayed?
- what happens if one day's data is wrong?
- how quickly must corrections appear?

---

# 92. CAPACITY ESTIMATION

Example:

```text
100,000 events/sec
Average event = 1 KB

Raw throughput:
100,000 × 1 KB
≈ 100 MB/sec

Per hour:
≈ 360 GB

Per day:
≈ 8.64 TB raw
```

Then account for:

- replication
- compression
- Delta overhead
- retention
- multiple layers
- processing overhead

Interviewers care more about your reasoning than exact arithmetic.

---

# 93. AVAILABILITY vs CONSISTENCY

Do not blindly say "we need exactly everything."

Ask:

> What happens if the system is temporarily unavailable?

For operational state:

- freshness may be critical

For financial transactions:

- correctness/consistency may dominate

Architecture follows business requirements.

---

# 94. LAMBDA vs KAPPA

## Lambda

Separate:

```text
Batch path
+
Streaming path
```

then combine.

Problem:

> duplicated logic.

## Kappa

Streaming-first architecture:

```text
Events → stream processing
```

Historical replay can rebuild state.

### Interview answer

Modern lakehouse systems often reduce the need for duplicated batch/stream paths, but the correct architecture depends on replayability, retention and workload.

---

# 95. DATA LAKE vs DATA WAREHOUSE vs LAKEHOUSE

| | Data Lake | Warehouse | Lakehouse |
|---|---|---|---|
| Storage | Object storage | Managed warehouse | Object storage + table layer |
| Data | Raw + varied | Structured | Raw to curated |
| ACID | Traditional lake: limited | Strong | Delta/Iceberg layer |
| BI | Possible | Excellent | Excellent |
| ML | Excellent | Possible | Excellent |
| Schema | Often flexible | Strict | Governed + flexible where appropriate |
| Cost | Often lower storage | Often higher | Hybrid |

---

# 96. DELTA vs PARQUET

Parquet:

> Columnar file format.

Delta:

> Table/storage protocol built around data files + transaction log.

```text
Parquet = file format

Delta = table/storage layer
         ├── Parquet
         └── transaction log
```

### Trick question

**Q: Is Delta a replacement file format for Parquet?**

That answer is incomplete.

Delta tables commonly use Parquet data files plus the Delta transaction log.

---

# 97. DATABRICKS vs SPARK

Spark:

> Open-source distributed processing engine.

Databricks:

> Managed/commercial data + AI platform built around Spark and additional technologies.

Databricks provides things such as:

- managed compute
- Delta Lake integration
- Unity Catalog
- SQL
- workflows
- pipelines
- Photon
- governance
- platform tooling

---

# 98. PYSPARK vs SPARK

Spark:

> Engine/framework.

PySpark:

> Python API for Spark.

```text
Spark Engine
   ▲
   │
PySpark
```

PySpark does not mean "a different Spark engine."

---

# 99. KAFKA vs SPARK

Kafka:

> transports/stores events.

Spark:

> processes data.

```text
Kafka
  │
  ▼
Spark
  │
  ▼
Delta
```

They solve different problems and work together.

---

# 100. DELTA vs KAFKA

Kafka:

> event log / transport.

Delta:

> durable analytical table/storage layer.

```text
Kafka
 └── event stream

Delta
 └── table/storage history
```

Kafka can feed Delta.

Delta can also be used as a streaming source.

---

# 101. UNITY CATALOG vs DELTA

Delta:

> table/storage reliability.

Unity Catalog:

> governance/metadata/access.

```text
Unity Catalog
     │
     ▼
Delta table
     │
     ▼
Parquet + _delta_log
```

---

# 102. ADF vs AIRFLOW vs LAKEFLOW JOBS

| Tool | Primary role |
|---|---|
| ADF | Azure integration/orchestration |
| Airflow | General workflow orchestration |
| Lakeflow Jobs | Databricks workflow orchestration |
| Spark | Distributed processing |
| Kafka | Event streaming |

### Trick

No single one is "the ETL engine."

---

# 103. DATABRICKS SQL

Databricks SQL provides SQL analytics capabilities over lakehouse data.

Know:

- SQL warehouses
- dashboards
- BI connectivity
- SQL queries
- performance
- permissions/governance

Power BI can consume governed datasets.

---

# 104. DATA LINEAGE

Example:

```text
ERP.orders
    │
    ▼
Bronze.orders
    │
    ▼
Silver.orders_clean
    │
    ▼
Gold.daily_sales
    │
    ▼
Power BI dashboard
```

Lineage answers:

> Where did this number come from?

and:

> What breaks if I change this column?

---

# 105. DATA CONTRACTS

A producer/consumer agreement covering:

- schema
- field meaning
- types
- nullability
- allowed values
- compatibility
- ownership
- SLA

Especially important in Kafka ecosystems.

---

# 106. SCHEMA DRIFT

Example:

Yesterday:

```json
{
  "temperature": 37.5
}
```

Today:

```json
{
  "temperature": "37.5",
  "humidity": 50
}
```

Potential problems:

- type mismatch
- downstream failure
- silent corruption

Solution:

- schema validation
- evolution rules
- quarantine
- producer contracts
- monitoring

---

# 107. RETRIES

Retry:

- transient network failure
- temporary service unavailable
- executor issue

Do not endlessly retry:

- invalid schema
- bad data
- permission failure
- deterministic logic bug

Use bounded retries and alert.

---

# 108. DEAD LETTER / QUARANTINE

```text
Incoming
   │
   ▼
Validation
 ┌─┴──────┐
 ▼        ▼
Valid    Invalid
 │        │
 ▼        ▼
Silver   Quarantine
          │
          ▼
      Investigation
```

Store enough context to replay.

---

# 109. INCIDENT: PIPELINE DUPLICATED DATA

Investigate:

1. Did source replay?
2. Did job retry?
3. Was checkpoint lost?
4. Was sink idempotent?
5. Was MERGE key correct?
6. Was consumer offset committed correctly?
7. Did parallel jobs overlap?
8. Was batch/run ID missing?

Fix:

- deterministic key
- idempotent MERGE
- checkpoint
- concurrency control
- monitoring

---

# 110. INCIDENT: SPARK JOB SUDDENLY SLOW

Checklist:

```text
Input volume ↑ ?
     ↓
Data skew ?
     ↓
Shuffle ↑ ?
     ↓
Small files ↑ ?
     ↓
Join strategy changed ?
     ↓
Partition count ?
     ↓
Schema/data distribution changed ?
     ↓
Cluster/compute changed ?
```

Do not immediately "increase cluster size."

---

# 111. INCIDENT: KAFKA CONSUMER LAG

Possible causes:

- consumer processing slower than producer
- too few partitions
- too few consumers
- expensive transformations
- downstream sink slow
- GC/memory issues
- network problems
- hot partition

Metrics:

```text
consumer lag
records/sec
processing latency
partition distribution
broker health
```

---

# 112. INCIDENT: DELTA TABLE HAS MILLIONS OF SMALL FILES

Causes:

- excessive micro-batches
- too many partitions
- frequent tiny writes

Solutions:

- optimize/compaction
- tune write frequency
- avoid overpartitioning
- tune streaming trigger
- improve ingestion batching

---

# 113. INCIDENT: BAD SCHEMA DEPLOYED

Response:

1. stop propagation if necessary
2. identify affected version
3. inspect Delta history
4. inspect source event/schema
5. quarantine bad data
6. restore/rebuild affected layer
7. correct schema
8. replay
9. validate
10. document root cause

---

# 114. TERRADATA

JD includes Teradata.

Know enough to discuss:

- enterprise warehouse
- fact/dimension modeling
- SQL analytics
- workload management conceptually
- migration/integration
- serving BI

Typical architecture:

```text
Operational Sources
      │
      ▼
Lakehouse / Databricks
      │
      ├── ML
      ├── Streaming
      └── Curated datasets
             │
             ▼
          Teradata
             │
             ▼
             BI
```

Don't pretend Teradata is just another data lake.

---

# 115. ML FEATURE PIPELINE

```text
Raw events
   ↓
Bronze
   ↓
Silver
   ↓
Feature engineering
   ↓
Feature tables
   ↓
Training
   ↓
Model
   ↓
Prediction
```

Avoid leakage.

Example:

Bad:

```text
feature = customer's future purchase
```

Good:

```text
feature uses only data available at prediction time
```

---

# 116. DIGITAL TWIN / ML TRICK

### Question

"Why not just train the model on all historical data?"

Because:

- temporal leakage
- changing distributions
- future information
- operational latency
- feature freshness

Time-series ML requires temporal validation.

---

# 117. PYTHON INTERVIEW TOPICS

Know:

- lists vs tuples
- dict/set
- generators
- iterators
- decorators
- exceptions
- context managers
- classes
- mutable vs immutable
- shallow vs deep copy
- comprehensions
- time complexity
- multiprocessing vs threading conceptually
- memory efficiency

For data engineering, prioritize:

- generators
- dict/set complexity
- exception handling
- file processing
- JSON
- API ingestion
- testable functions

---

# 118. PYTHON DATA PIPELINE PATTERN

```python
def extract():
    ...

def validate(data):
    ...

def transform(data):
    ...

def load(data):
    ...

def run():
    data = extract()
    validate(data)
    data = transform(data)
    load(data)
```

Keep extraction, transformation and loading separable/testable.

---

# 119. TESTING

### Unit tests

Test transformations/functions.

### Integration tests

Test pipeline components together.

### Data quality tests

Validate:

- nulls
- uniqueness
- referential integrity
- ranges
- freshness
- row counts

### Regression tests

Ensure changes don't break previous behavior.

---

# 120. PRODUCTION CODE QUALITY

Good pipeline code should be:

- modular
- parameterized
- testable
- observable
- idempotent
- documented
- configurable
- version controlled

Avoid giant notebooks containing everything.

---

# 121. 30 HIGH-VALUE TRICK QUESTIONS

### 1. Spark vs PySpark?
Spark = engine; PySpark = Python API.

### 2. Databricks vs Spark?
Databricks = platform; Spark = distributed engine.

### 3. Delta vs Parquet?
Delta adds table/storage transaction semantics around data files.

### 4. Kafka guarantees global ordering?
No, partition-level ordering.

### 5. More Kafka consumers always increase throughput?
No, bounded by partitions.

### 6. Replication factor 3 means 3 consumers?
No, 3 replicas.

### 7. Kafka removes consumed messages?
No, retention controls deletion.

### 8. Kafka is a queue?
It can support queue-like consumption patterns, but its core abstraction is a distributed log/event stream.

### 9. Exactly-once is automatic?
No.

### 10. Spark transformations execute immediately?
No, Spark is lazy.

### 11. groupBy is narrow?
No, usually wide/shuffle.

### 12. collect() is safe for large datasets?
No.

### 13. More partitions always improve Spark performance?
No.

### 14. Broadcast always improves joins?
No. Too-large broadcasts can cause memory problems.

### 15. More cluster memory fixes data skew?
Not necessarily.

### 16. Unity Catalog replaces Entra ID?
No.

### 17. DLT is still the current product name?
No. It is the former name; current terminology is Lakeflow pipelines.

### 18. ADF is a Spark engine?
No.

### 19. Airflow processes the data?
No; it orchestrates processing.

### 20. VACUUM is the same as OPTIMIZE?
No.

### 21. OPTIMIZE deletes old files?
No, it primarily improves file layout/compaction; VACUUM handles obsolete-file cleanup.

### 22. Time travel works forever?
No. Retention/cleanup matters.

### 23. Schema evolution accepts anything?
No.

### 24. Bronze should contain business aggregates?
Usually no; Bronze should preserve source/raw semantics.

### 25. Silver is always normalized?
No. It should be clean/conformed for downstream use.

### 26. Gold is always one table?
No.

### 27. Streaming is always better than batch?
No.

### 28. Delta Lake is a database?
Not in the traditional OLTP sense. It is a lakehouse storage/table layer.

### 29. Kafka replaces Delta?
No. They solve different problems.

### 30. A successful pipeline means healthy data?
No. Freshness, completeness and correctness matter.

---

# 122. TOP SYSTEM DESIGN QUESTION

## "Design a real-time vehicle monitoring platform."

### Strong answer

```text
Vehicles
   │
   ▼
Kafka
   │
   ├── raw event retention/replay
   │
   ▼
Spark Structured Streaming
   │
   ├── parse
   ├── validate
   ├── dedupe
   ├── watermark
   └── enrich
   │
   ▼
Bronze Delta
   │
   ▼
Silver Delta
   │
   ├── current state
   ├── telemetry history
   └── anomalies
   │
   ▼
Gold
   │
   ├── KPIs
   ├── ML features
   └── alerts
   │
   ├────────► BI
   ├────────► ML
   └────────► operational applications
```

Cross-cutting:

```text
Unity Catalog
Security
Monitoring
Lineage
DQ
CI/CD
```

---

# 123. WHAT HAPPENS WHEN KAFKA GOES DOWN?

Good answer:

> Producers may fail or buffer depending on producer/application behavior. Consumers stop receiving new events. Once Kafka recovers, retained events can be consumed. I would design the downstream pipeline with checkpoints and idempotent writes so recovery does not create incorrect duplicates.

Bad answer:

> "Kafka automatically fixes everything."

---

# 124. WHAT HAPPENS WHEN SPARK FAILS?

```text
Spark failure
     ↓
restart
     ↓
checkpoint
     ↓
resume from recoverable progress
     ↓
idempotent sink
```

Explain that exact behavior depends on source/sink/query semantics.

---

# 125. WHAT HAPPENS WHEN DELTA WRITE FAILS?

The transaction should not expose a partial committed table state.

But the application may need:

- retry
- checkpoint handling
- idempotency
- alerting

---

# 126. HOW TO HANDLE DUPLICATE EVENTS?

Use:

```text
event_id
+
deduplication
+
checkpoint
+
idempotent sink
```

For Delta:

```sql
MERGE INTO target
USING source
ON target.event_id = source.event_id
```

Do not use timestamp alone as a unique key unless business semantics guarantee uniqueness.

---

# 127. HOW TO HANDLE OUT-OF-ORDER EVENTS?

Use:

- event time
- watermarks
- stateful processing
- deterministic keys
- correction/reconciliation jobs

Example:

```text
10:05 arrives
10:03 arrives later
```

The pipeline must not assume arrival order equals event order.

---

# 128. HOW TO CHOOSE KAFKA PARTITIONS?

Consider:

```text
required throughput
consumer parallelism
ordering requirements
key distribution
broker capacity
future growth
```

Do not choose:

> "100 partitions because that's a round number."

---

# 129. HOW TO CHOOSE DELTA PARTITIONING?

Ask:

1. How is data queried?
2. What filters are common?
3. Cardinality?
4. Data volume?
5. Write pattern?
6. Is partition pruning useful?

Do not partition by every column.

---

# 130. PERFORMANCE TRIAGE FRAMEWORK

When asked "How do you optimize Spark?":

```text
Measure
  ↓
Reduce data scanned
  ↓
Reduce shuffle
  ↓
Fix skew
  ↓
Optimize joins
  ↓
Fix partition sizing
  ↓
Fix small files
  ↓
Use appropriate compute
  ↓
Cache only when justified
  ↓
Validate improvement
```

Never lead with:

> "Increase cluster size."

---

# 131. COST OPTIMIZATION

Consider:

- right-size compute
- autoscaling
- job/serverless compute where appropriate
- avoid idle clusters
- optimize files
- reduce scanned data
- avoid unnecessary shuffles
- efficient storage lifecycle
- appropriate retention
- avoid repeated full refreshes
- incremental processing
- efficient SQL

---

# 132. SECURITY TRICK QUESTIONS

### Q: Can a user who can access Azure storage automatically access every Databricks table?

No. Access paths and governance must be designed; Unity Catalog permissions and cloud identity/storage configuration interact.

### Q: Should pipelines run as your personal user?

Prefer service principals/managed identities or appropriate workload identities.

### Q: Can secrets live in Git?

No.

---

# 133. ARCHITECTURE DIAGRAM — COMPLETE AZURE + DATABRICKS

```text
                         MICROSOFT AZURE
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│  Entra ID     Key Vault      VNet      Monitor      DevOps         │
│                                                                    │
│  Sources                                                        │
│  ┌────────┐  ┌──────────┐  ┌─────────┐  ┌──────────┐            │
│  │ ERP/DB │  │ IoT/APIs │  │  Files  │  │ Events   │            │
│  └───┬────┘  └────┬─────┘  └────┬────┘  └────┬─────┘            │
│      │             │             │             │                  │
│      │             │             │             ▼                  │
│      │             │             │           Kafka                 │
│      │             │             │             │                  │
│      └─────────────┴─────────────┴─────────────┘                  │
│                            │                                       │
│                            ▼                                       │
│                    ┌─────────────────┐                             │
│                    │   DATABRICKS    │                             │
│                    │                 │                             │
│                    │ Spark / PySpark │                             │
│                    │ Structured      │                             │
│                    │ Streaming       │                             │
│                    └────────┬────────┘                             │
│                             ▼                                      │
│                       Delta Lake                                   │
│                 ┌──────────────┐                                   │
│                 │    Bronze    │                                   │
│                 └──────┬───────┘                                   │
│                        ▼                                            │
│                 ┌──────────────┐                                   │
│                 │    Silver    │                                   │
│                 └──────┬───────┘                                   │
│                        ▼                                            │
│                 ┌──────────────┐                                   │
│                 │     Gold     │                                   │
│                 └──────┬───────┘                                   │
│                        │                                            │
│             ┌──────────┼──────────┐                                 │
│             ▼          ▼          ▼                                 │
│            BI         ML       Operational Apps                     │
│                                                                    │
│                 Unity Catalog                                      │
│        Governance / Access / Lineage / Metadata                    │
└────────────────────────────────────────────────────────────────────┘
```

---

# 134. THE "EXPLAIN THIS PROJECT" TEMPLATE

Use this sequence:

### 1. Business problem

> "We needed to..."

### 2. Sources

> "Data came from..."

### 3. Scale

> "Approximately X events/GB/day..."

### 4. Ingestion

> "We used Kafka/ADF/Auto Loader because..."

### 5. Processing

> "Spark/PySpark performed..."

### 6. Storage

> "Delta provided..."

### 7. Data quality

> "We validated..."

### 8. Governance

> "Unity Catalog handled..."

### 9. Orchestration

> "Lakeflow Jobs/Airflow/ADF handled..."

### 10. Reliability

> "We used checkpoints/idempotency/retries..."

### 11. Performance

> "The bottleneck was... We fixed it by..."

### 12. Outcome

> "This reduced latency/cost/failures by..."

---

# 135. HOW TO ANSWER "WHY DID YOU CHOOSE KAFKA?"

Strong:

> "The source produced continuous events and downstream consumers needed decoupled, replayable, scalable event delivery. Kafka gave us partitioned throughput, consumer groups, retention and replay. If the requirement were only scheduled batch ingestion, Kafka would add unnecessary operational complexity."

This is much stronger than:

> "Kafka is fast."

---

# 136. HOW TO ANSWER "WHY DELTA?"

Strong:

> "We needed reliable lake storage with ACID transactions, schema controls, versioning, MERGE/upsert support and a common table layer for batch and streaming. Delta provided those semantics on cloud object storage."

---

# 137. HOW TO ANSWER "WHY DATABRICKS?"

Strong:

> "We wanted one platform that combined distributed Spark processing, Delta tables, governance, streaming, SQL, orchestration and ML capabilities instead of operating many disconnected systems."

---

# 138. HOW TO ANSWER "WHY STREAMING?"

Strong:

> "The business SLA required fresh operational state within minutes/seconds. Batch would not satisfy that freshness requirement."

---

# 139. HOW TO ANSWER "WHY NOT STREAMING?"

Strong:

> "If the business can tolerate hourly or daily latency, batch is simpler and often cheaper. I would not introduce always-on streaming without a freshness requirement."

---

# 140. TOP 20 QUESTIONS TO PRACTICE OUT LOUD

1. Explain Spark architecture.
2. Explain lazy evaluation.
3. What causes a shuffle?
4. How do you optimize a slow Spark job?
5. Explain data skew.
6. Explain broadcast join.
7. Explain Delta Lake.
8. Delta vs Parquet?
9. Explain ACID in Delta.
10. Explain time travel.
11. MERGE vs INSERT?
12. Explain Kafka architecture.
13. Topic vs partition?
14. Consumer group?
15. How does Kafka maintain ordering?
16. At least once vs exactly once?
17. Explain Structured Streaming.
18. Explain checkpointing.
19. Design Kafka → Databricks → Delta pipeline.
20. Design a real-time digital-twin data platform.

---

# 141. TOP 20 "SENIOR ENGINEER" QUESTIONS

1. How do you handle schema evolution in production?
2. How do you handle late events?
3. How do you handle duplicate Kafka messages?
4. How do you recover from checkpoint corruption?
5. How do you perform a backfill without breaking production?
6. How do you prevent duplicate writes during retries?
7. How do you handle Kafka consumer lag?
8. How do you handle data skew?
9. How do you choose partition count?
10. How do you optimize Delta tables?
11. How do you detect small-file problems?
12. How do you design governance?
13. How do you implement lineage?
14. How do you secure production pipelines?
15. How do you design CI/CD?
16. How do you monitor data quality?
17. How do you estimate infrastructure capacity?
18. How do you reduce cloud cost?
19. When would you choose batch over streaming?
20. How would you design disaster recovery?

---

# 142. RED FLAGS IN YOUR ANSWERS

Avoid these:

❌ "Kafka is a database."

❌ "Spark is faster because it is in-memory."

❌ "Databricks is Spark."

❌ "Delta is just Parquet."

❌ "Exactly once means no duplicates ever."

❌ "More partitions always improve performance."

❌ "More memory fixes skew."

❌ "Streaming is always better."

❌ "Unity Catalog is Azure AD."

❌ "Airflow processes data."

❌ "ADF transforms everything."

❌ "OPTIMIZE and VACUUM are the same."

❌ "DLT is the current product name."

---

# 143. WHAT A STRONG DE II ANSWER SOUNDS LIKE

Weak:

> "We used Kafka because it is scalable."

Strong:

> "Kafka was appropriate because the source generated continuous events, multiple downstream consumers needed the same stream, and we required replayability. We partitioned by the entity key to preserve per-entity ordering, monitored consumer lag, and designed the downstream Delta writes to be idempotent because retries can cause duplicate processing."

The difference is **reasoning**.

---

# 144. FINAL 24-HOUR CRAM ORDER

If time is extremely limited:

## Hour 1–2
Spark architecture

## Hour 3
PySpark

## Hour 4
Spark optimization

## Hour 5
Delta Lake

## Hour 6
Kafka

## Hour 7
Structured Streaming

## Hour 8
Databricks + Unity Catalog

## Hour 9
Lakeflow Pipelines + Jobs

## Hour 10
SQL

## Hour 11
System design

## Hour 12
Real-world architecture

## Hour 13
Data quality/reliability

## Hour 14
CI/CD + Azure

## Hour 15
Trick questions

## Hour 16
Mock interview aloud

---

# 145. FINAL CHEAT SHEET

```text
AZURE
  ↓
Cloud foundation

KAFKA
  ↓
Event transport + durable log + replay

SPARK
  ↓
Distributed processing

PYSPARK
  ↓
Python interface to Spark

DELTA
  ↓
Reliable lakehouse tables

DATABRICKS
  ↓
Integrated data + AI platform

UNITY CATALOG
  ↓
Governance + metadata + access + lineage

LAKEFLOW PIPELINES
  ↓
Declarative data pipelines + quality

LAKEFLOW JOBS
  ↓
Orchestration

PHOTON
  ↓
Accelerated Databricks execution

ADLS
  ↓
Cloud object storage

ADF / AIRFLOW
  ↓
External/general orchestration

TERADATA
  ↓
Enterprise warehouse / BI

POWER BI
  ↓
Analytics / visualization
```

---

# 146. OFFICIAL REFERENCE LINKS

## Databricks

- Lakehouse: https://docs.databricks.com/aws/en/lakehouse/
- Delta Lake: https://docs.databricks.com/aws/en/delta
- Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
- Lakeflow Jobs: https://docs.databricks.com/aws/en/jobs/
- Lakeflow Pipelines: https://docs.databricks.com/aws/en/ldp/
- Photon: https://docs.databricks.com/aws/en/compute/photon

## Delta Lake

- https://docs.delta.io/

## Apache Kafka

- https://kafka.apache.org/documentation/
- https://kafka.apache.org/design/

## Apache Spark

- https://spark.apache.org/docs/latest/

## Azure

- https://learn.microsoft.com/azure/

---

# 147. FINAL INTERVIEW RULE

When given any architecture question, mentally walk through:

```text
SOURCE
  ↓
INGEST
  ↓
PARTITION
  ↓
PROCESS
  ↓
STORE
  ↓
SERVE
  ↓
GOVERN
  ↓
MONITOR
  ↓
RECOVER
  ↓
SCALE
  ↓
CONTROL COST
```

If you can explain every box **and the failure mode between every two boxes**, you are no longer answering at a tool-user level.

You are answering like a Data Engineer.

---

## Current terminology note

Databricks documentation in 2026 uses **Lakeflow pipelines** for what was formerly called **Delta Live Tables (DLT)**. Interviewers may still use "DLT," so recognize both names.

Kafka 4.x uses **KRaft** rather than ZooKeeper.

---

## Sources used to validate current terminology and architecture

- Databricks Lakehouse documentation
- Databricks Delta Lake documentation
- Databricks Unity Catalog documentation
- Databricks Lakeflow Jobs/Pipelines documentation
- Databricks Photon documentation
- Apache Kafka documentation and Kafka 4.x/KRaft documentation
- Delta Lake documentation
- Apache Spark documentation

