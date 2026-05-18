# Confluent Kafka Isotope

An example of using Kafka **consumer and producer interceptors** to tag every message with a tracer — an *isotope* — that carries through every hop of a multi-topic pipeline. Apache Flink consumes the tagged records and reports on what the isotopes reveal: **end-to-end latency**, **hop topology**, **drop/duplication rates**, and **pipeline coverage**.

A **portability requirement** runs through this project: the same isotope mechanism must work against both **Confluent Cloud for Apache Flink (CCAF)** (managed) and **Confluent Platform for Apache Flink** (self-managed) — both used here via Table API SQL plus uploaded UDF/PTF JARs (no DataStream code on either side). Three decisions follow from that: tagging happens in the Kafka **producer/consumer interceptors** (the one extension point both runtimes share via the broker), the on-wire **header** format is **JSON** (so Flink SQL can read the scalar fields with `CAST(headers[…] AS STRING)` and no UDF), and the optional stateful reports (`LatencyPercentilesUDAF`, `StuckTracePTF`) ship as a single JAR that registers identically on either runtime.

One asymmetry the runtimes don't share: **Flink-native SR-Protobuf**. CCAF supports SR-framed Protobuf as a Flink sink format via its topic catalog; Apache Flink open-source (the CP Flink runtime) ships `avro-confluent` but no SR-Protobuf counterpart. So Flink *report sinks* land on **Avro+SR on CP** and can be **Protobuf+SR on CCAF** — a runtime constraint, not a project preference. The demo *event* topics (next paragraph) are unaffected because they're written by the Kafka producer client, not by Flink.

Message **values** on the demo topics are **SR-framed Protobuf** (`ai.signalroom.kafka.isotope.proto.DemoEvent`) — the standard Confluent value format. The interceptors and reports are agnostic to value format because the isotope rides in headers; the Protobuf choice just gives the integration tests and the demo CLI a typed payload to work with.

---

**Table of Contents**
<!-- toc -->
- [1.0 How the isotope is carried](#10-how-the-isotope-is-carried)
- [2.0 Repo layout](#20-repo-layout)
- [3.0 Running](#30-running)
  - [3.1. Unit tests (no broker, instant)](#31-unit-tests-no-broker-instant)
  - [3.2 Demo CLI — see one trace propagate live](#32-demo-cli--see-one-trace-propagate-live)
  - [3.3 Integration tests (live Kafka via Minikube)](#33-integration-tests-live-kafka-via-minikube)
  - [3.4 Flink SQL reports on Minikube](#34-flink-sql-reports-on-minikube)
  - [3.4.1 Format-by-domain](#341-format-by-domain)
- [3.5 Flink SQL reports on Confluent Cloud (CCAF)](#35-flink-sql-reports-on-confluent-cloud-ccaf)
- [3.6 Recommended path the first time through](#36-recommended-path-the-first-time-through)
<!-- tocstop -->

---

## **1.0 How the isotope is carried**

- **Header `x-isotope`** (JSON bytes) carries the full hop history, forwarded by every hop:
  - `t` — 16-byte **UUIDv7** trace ID (RFC 9562): 48-bit ms timestamp in the high bits + 74 bits random. Stable for the life of the trace,and lexicographic byte order matches creation order — sort trace IDs and you get chronological order for free.
  - `o` — origin timestamp (ms) — same value as the timestamp embedded in the UUIDv7 trace ID; kept as its own field for typed access from Flink SQL without needing to decode the UUID bytes.
  - `s` — origin service name (set once, never reassigned)
  - `h` — ordered list of hops, each `{s: service, t: topic, m: tsMs}`
  - `x` — `true` if the hop list exceeded `MAX_HOPS = 32` and the oldest hop was evicted
- **Six scalar headers** (UTF-8 strings) carry the most-recent-hop view so
  Flink SQL can read them via `CAST(headers['x-isotope-…'] AS STRING)` without parsing the JSON array (no UDF required on either CCAF or CP Flink). See [flink/README.md](flink/README.md) for the full header table.

A producer with the isotope interceptor loaded appends one hop on every `send()`. A consume-then-produce service calls `IsotopeContext.adoptFromRecord(record)` between consume and produce so the trace ID and origin survive the hop.

## **2.0 Repo layout**

```
app/                                    isotope JVM library + demo CLI + tests
  src/main/proto/                       DemoEvent.proto (Protobuf message value)
  src/main/java/ai/signalroom/kafka/isotope/
    Isotope.java                        POJO + JSON codec + Hop + fromHeaders()
                                        + UUIDv7 helpers (uuidV7Bytes / uuidV7String)
    IsotopeContext.java                 ThreadLocal + adoptFromRecord()
    IsotopeProducerInterceptor.java     stamps/appends x-isotope + 6 scalar
                                        reporting headers on send()
    IsotopeConsumerInterceptor.java     batch-aware logging; no auto-propagation
    App.java                            demo CLI — send / hop / sink modes
  src/test/java/.../                    IsotopeCodecTest (no broker needed)
  src/integrationTest/java/.../         live-broker tests; produce/consume
                                        DemoEvent via SR-framed Protobuf
                                        (need Minikube CP + SR port-forwarded)
ptf/                                    Phase 2 — Flink PTF + UDAF shadow JAR
  src/main/java/ai/signalroom/kafka/isotope/flink/
    LatencyPercentilesUDAF.java         T-Digest p50/p95/p99 aggregate
    StuckTracePTF.java                  per-trace state + event-time timer
    Percentiles.java, StuckTraceAlert.java  return-type DTOs
  src/test/java/.../                    LatencyPercentilesUDAFTest
flink/sql/                              Flink SQL reports — identical on
                                        CCAF and CP Flink except source DDL
  cp/                                   CP Flink: 00_source_table, 01_register_functions,
                                        05_report_sinks (avro-confluent), 99_teardown
  cc/                                   CCAF: 00_source_table (view over auto-registered
                                        topic), 01_register_functions (confluent-artifact:// JAR)
  shared/                               05_isotope_view + 6 INSERT INTO reports
                                        (10_latency, 20_topology, 30_hop_distribution,
                                        40_coverage, 60_stuck_trace, 70_latency_percentiles)
k8s/base/                               CFK Kafka/SR/Connect/ksqlDB/C3 manifests
scripts/                                port-forward helpers, deploy-flink-reports.sh
Makefile                                cp-up / flink-up / kafka-pf-up / ...
```

## **3.0 Running**

### **3.1. Unit tests (no broker, instant)**

```bash
./gradlew test                       # both subprojects
# or scoped:
./gradlew :app:test                  # IsotopeCodecTest        — JSON roundtrip, hop eviction, header size, UUIDv7 properties (10 tests)
./gradlew :ptf:test                  # LatencyPercentilesUDAFTest — T-Digest accumulator semantics
```

### **3.2 Demo CLI — see one trace propagate live**

The fastest way to watch the isotope mechanic. Requires the cluster to be up and the Kafka + SR forwards running (see step 3 below for the bring-up commands). The CLI has three modes:

| Mode | Args | What it does |
|---|---|---|
| `send` | `<topic> <service> <payload>` | Produces one isotope-tagged `DemoEvent` to `<topic>`, then exits. Auto-creates the topic. |
| `hop`  | `<in-topic> <out-topic> <service>` | Consumes records from `<in-topic>`, adopts the isotope into thread-local, re-produces the same `DemoEvent` to `<out-topic>` as `<service>` (which appends a new hop). Runs until Ctrl-C. |
| `sink` | `<topic>` | Subscribes to `<topic>` and pretty-prints the full isotope trail for every arriving record. Runs until Ctrl-C. |

**A 3-stage chain in four terminals:**

```bash
# Terminal A — terminal sink (will print the full 3-hop trail)
./gradlew :app:run --args="sink iso-final" -q

# Terminal B — middle stage: iso-mid → iso-final as svc-C
./gradlew :app:run --args="hop iso-mid iso-final svc-C" -q

# Terminal C — first stage: iso-start → iso-mid as svc-B
./gradlew :app:run --args="hop iso-start iso-mid svc-B" -q

# Terminal D — kick the chain off (run repeatedly to send more)
./gradlew :app:run --args="send iso-start svc-A 'hello world'" -q
```

Terminal A's output for each record shows the same `trace_id` across all three hops, `origin = svc-A` (never reassigned), and `hops[]` listing `svc-A → svc-B → svc-C` in order with per-hop timestamps. Override endpoints via `-Dkafka.bootstrap=…` / `-Dschema.registry.url=…` if you're not on the default Minikube layout.

### **3.3 Integration tests (live Kafka via Minikube)**

Bring up the local Confluent Platform stack and port-forward Kafka + SR:

```bash
make minikube-start                  # one-time
make cp-up                           # CFK Operator + Kafka/SR/Connect/ksqlDB/C3 (~5 min)
make kafka-pf-up                     # localhost:30092 → Kafka, localhost:8081 → Schema Registry
```

Then run the suite:

```bash
./gradlew :app:integrationTest                                          # all 5 tests
./gradlew :app:integrationTest --tests '*ProducerInterceptorIT'         # just one
```

Override the endpoints if needed:

```bash
./gradlew :app:integrationTest \
    -PkafkaBootstrap=localhost:30092 \
    -PschemaRegistryUrl=http://localhost:8081
```

Tear down forwards when done:

```bash
make kafka-pf-down
```

The integration tests cover:

| Test | What it verifies |
|---|---|
| `BrokerSmokeIT` | AdminClient can create/list/delete a topic via the NodePort port-forward |
| `ProducerInterceptorIT` | A bare consumer sees the `x-isotope` JSON header + all 6 scalar reporting headers with the expected origin/hop values, and the Protobuf round-trip preserves `DemoEvent.source` / `payload` |
| `ConsumerInterceptorIT` | `IsotopeContext.adoptFromRecord` extracts isotope into thread-local on tagged records; clears the thread-local for untagged records |
| `ThreeStageHopPropagationIT` | `svc-A → topic-AB → svc-B → topic-BC → svc-C` produces a stable trace ID, 2-hop trail in send order, and correct scalar headers (origin = `svc-A`, this = `svc-B`, hop count = 2) at the terminal |

### **3.4 Flink SQL reports on Minikube**

The four Phase-1 reports plus the Phase-2 PTF and UDAF reports — six in total — run against a Flink session cluster managed by the Confluent Flink Kubernetes Operator. Same FQL files deploy to Confluent Cloud for Apache Flink — see **[§ 3.5](#35-flink-sql-reports-on-confluent-cloud-ccaf)** for that path; this section is the local-Minikube path.

**Bring up Flink:**

```bash
make flink-up                # cert-manager → CFK Flink Operator → CMF → session cluster
                             # (~5 min the first time)
```

**Deploy the reports** — all 6 reports run on the cp-flink session cluster (Flink 2.1.2). Sink topics use Apache Flink's [`avro-confluent`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/avro-confluent/) format — SR-framed Avro, auto-registered on first write — so Control Center renders the report rows natively.

```bash
make flink-reports-up
```

`flink-reports-up` builds the PTF/UDAF shadow JAR if missing, copies it into the JobManager pod, pre-creates the 6 sink Kafka topics, then applies the source + view + sink DDL and submits 6 `INSERT INTO` streaming jobs (one per report). On first write to each sink, Apache Flink's `flink-sql-avro-confluent-registry` format registers a fresh Avro schema in SR under subject `<topic>-value`. **Control Center deserializes all 6 report topics natively** — no `.proto` files in the repo for the reports, no hand-installed format jars beyond the one init-container download.

#### **3.4.1 Format-by-domain**

The demo *event* topics (`iso-start`, `iso-mid`, `iso-final`) still ride **Protobuf+SR** via the Java app's `DemoEvent` schema — that's unchanged. The *report* topics ride **Avro+SR** because cp-flink doesn't ship an SR-integrated Protobuf format and CMF (which does) disallows the UDAFs the percentiles report needs. Events from the app are Protobuf; aggregates from Flink are Avro. Two formats by domain — a clean split, not a defect.

### **3.5 Flink SQL reports on Confluent Cloud (CCAF)**

CCAF parallel of [§ 3.4](#34-flink-sql-reports-on-minikube), driven by Terraform under [terraform/](terraform/). One `make` target spins up a fresh Confluent Cloud environment, Kafka cluster, 9 topics (3 isotope event + 6 report sinks), a Flink compute pool, a rotating service-account API key pair, the PTF/UDAF JAR uploaded as a Flink artifact, and 16 long-lived `confluent_flink_statement` resources (source view, typed view, 6 sinks, 2 `CREATE FUNCTION`, 6 streaming `INSERT INTO`). The Terraform shape mirrors [`apache_flink-kickstarter-ii`](https://github.com/j3-signalroom/apache_flink-kickstarter-ii) — same provider version, same `iac-confluent-api_key_rotation-tf_module`, same DROP-then-CREATE statement pattern.

**Prereqs:**

- [Terraform](https://developer.hashicorp.com/terraform/install) `>= 1.5` installed locally.
- A Confluent Cloud API key (Cloud-level, not cluster-scoped) with permissions to create environments, Kafka clusters, Flink compute pools, service accounts, role bindings, Flink artifacts, and statements. Generate via Console → Settings → Cloud API keys.

**Deploy:**

```bash
export CONFLUENT_API_KEY=...
export CONFLUENT_API_SECRET=...
make cc-flink-reports-up CONFLUENT_API_KEY=$CONFLUENT_API_KEY CONFLUENT_API_SECRET=$CONFLUENT_API_SECRET
```

The wrapper script ([scripts/deploy-cc-flink-reports.sh](scripts/deploy-cc-flink-reports.sh)) builds the PTF/UDAF shadow JAR if missing, then runs `terraform apply -auto-approve` in [terraform/](terraform/). First-run takes ~6–8 minutes (Kafka cluster provisioning dominates). Re-applies are idempotent — `CREATE … IF NOT EXISTS` plus `lifecycle { ignore_changes = [compute_pool] }` on every statement.

**What gets created** (see [terraform/setup-confluent-flink.tf](terraform/setup-confluent-flink.tf) for the full graph):

| Resource | Name | Notes |
|---|---|---|
| `confluent_environment` | `confluent-kafka-isotope` | ESSENTIALS stream-governance package |
| `confluent_kafka_cluster` | `kafka-isotope` | Standard, single-zone, AWS us-east-1 by default |
| `confluent_kafka_topic` × 9 | `iso-{start,mid,final}`, `isotope-report-*-1m` | Explicit so `destroy` cleans them up |
| `confluent_service_account` + 5 role bindings | `isotope-flink-sql-runner` | FlinkDeveloper + ResourceOwner-topic + Assigner + SR-subject + transactional |
| `confluent_flink_compute_pool` | `isotope-flink-statement-runner` | 10 CFU; comfortable headroom for 6 INSERTs + ad-hoc SELECTs |
| `confluent_flink_artifact` | `isotope-flink-udf` | Uploads `ptf/build/libs/isotope-flink-udf.jar` |
| `confluent_flink_statement` × 16 | (see file) | View + typed view + 6 sinks + 2 functions + 6 INSERTs |

**Useful outputs:**

```bash
terraform -chdir=terraform output environment_id
terraform -chdir=terraform output kafka_bootstrap_servers
terraform -chdir=terraform output schema_registry_url
terraform -chdir=terraform output -raw kafka_api_key     # sensitive
terraform -chdir=terraform output -raw kafka_api_secret  # sensitive
```

**Format-by-runtime (not -by-domain).** The CP runtime ships `avro-confluent` but no SR-Protobuf, so [§ 3.4](#34-flink-sql-reports-on-minikube)'s reports land on Avro+SR. CCAF ships SR-Protobuf as a first-class Flink format (`protobuf-registry`), so the CCAF sinks land on **Protobuf+SR**. The shared `INSERT INTO` statements in [flink/sql/shared/](flink/sql/shared/) are byte-identical across CP and CCAF — only the per-environment sink DDL differs in format. See [flink/sql/cc/05_report_sinks.fql](flink/sql/cc/05_report_sinks.fql) for the CCAF sink shapes.

**Driving traffic.** The current demo CLI (App.java) defaults to localhost-no-auth Kafka, so pointing it at CCAF needs SASL_SSL + SR basic auth wired into [`App.java`](app/src/main/java/ai/signalroom/kafka/isotope/App.java) — explicitly out of scope for this CC deploy. For now you can drive the reports by either (a) the [Confluent CLI](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_produce.html) `kafka topic produce` against the iso-* topics, or (b) any Kafka producer with the isotope JAR on its classpath and the SASL config above.

**Teardown:**

```bash
make cc-flink-reports-down CONFLUENT_API_KEY=$CONFLUENT_API_KEY CONFLUENT_API_SECRET=$CONFLUENT_API_SECRET
```

Runs `terraform destroy -auto-approve` — deletes every resource above, including the environment itself. Safe to run repeatedly.

### **3.6 Recommended path the first time through**

1. `./gradlew test` — proves the codec + UDAF logic without any cluster.
2. `make cp-up && make kafka-pf-up && ./gradlew :app:integrationTest` —
   proves the broker + SR + interceptor + Protobuf path end-to-end.
3. The 3-stage demo CLI walkthrough above — visually shows the trace
   accumulating hops.
4. `make flink-up && make flink-reports-up && make flink-sql` — reports
   populate as you drive traffic via the demo CLI (see § 3.2).
5. (Optional) `make cc-flink-reports-up` — the CCAF parallel; see § 3.5
   for prereqs and the SASL-config caveat for the demo CLI.
