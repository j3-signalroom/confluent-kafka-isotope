# Flink SQL reports

Five reports total, deployed across **two Flink paths**:

| Reports | Path | Format | Where the SQL lives |
|---|---|---|---|
| `latency_report_1m`, `topology_report_1m`, `hop_distribution_1m`, `coverage_report_1m` | **CMF Statements** (canonical) | SR-Protobuf (auto-registered as `proto-registry` format; Control Center renders natively) | `flink/sql/cmf/` |
| `latency_percentiles_flat_1m` | **cp-flink session cluster** (UDAF-only) | Apache Flink's open-source `protobuf` format (raw Protobuf, no SR magic byte) | `flink/sql/cp/` + `flink/sql/shared/70_*.fql` |

The split exists because **CMF disallows user-defined functions** in
Statements ([features-support](https://docs.confluent.io/platform/current/flink/jobs/sql-statements/features-support.html)),
and the percentiles report depends on a custom T-Digest UDAF. The 4
pure-SQL reports migrate cleanly to CMF; the percentiles report stays
on the session cluster where UDAFs work.

> **Why isn't everything CMF?** Two reasons. (1) CMF disallows UDAFs
> in Statements — the percentiles report's `LATENCY_PERCENTILES`
> T-Digest aggregate can't run there. (2) CMF's compute-pool image
> (`confluentinc/cp-flink-sql`) is currently published only for Flink
> 1.19 — Confluent has not yet shipped a 2.x build of cp-flink-sql.
> The session cluster runs Flink 2.1.2; CMF reports run Flink 1.19.
> They're isolated (different pods, different JVMs, only share Kafka
> + SR), so the mismatch is ergonomic, not load-bearing.

## CMF path (canonical)

`make cmf-reports-up` creates a CMF `KafkaCatalog` (wraps SR), a
`KafkaDatabase` (wraps Kafka), a `ComputePool`, then submits 4
Statements per report (one DDL `CREATE TABLE`, one streaming
`INSERT INTO`). CMF auto-creates each `isotope-report-*-1m` Kafka
topic and auto-registers a Protobuf schema for the `*-value` subject.
The 4 DDL Statements use `'value.format' = 'proto-registry'`; the
INSERTs decode `x-isotope-*` headers via a CTE (CMF doesn't support
`CREATE VIEW` so we can't have a shared `isotope` view).

## Session-cluster path (hybrid only)

`make flink-reports-up` is now scoped to **just the latency-percentiles
report**. It still uses Apache Flink's open-source `protobuf` format
(no SR magic byte) and the `LatencyPercentilesUDAF` shadow JAR. The
wire schema for that one report lives in
[ptf/src/main/proto/.../reports.proto](src/main/proto/ai/signalroom/kafka/isotope/proto/reports/reports.proto)
and is compiled into `isotope-flink-udf.jar`. Control Center can't
deserialize that topic without the `.proto`; query via `make flink-sql`
or `kafkacat | protoc --decode`.

## Layout

```
flink/sql/
  cp/
    00_source_table.fql              # CREATE TABLE with 'connector' = 'kafka'
    01_register_functions.fql        # Phase 2 — CREATE FUNCTION … USING JAR
    05_report_sinks.fql              # CREATE TABLE for each isotope-report-*-1m Kafka sink (Flink protobuf, no SR)
    99_teardown.fql                  # DROP TABLE/VIEW/FUNCTION (companion to flink-reports-down)
  cc/
    00_source_table.fql              # CREATE VIEW over CCAF's auto-registered topic
    01_register_functions.fql        # Phase 2 — confluent-artifact:// JAR reference
  shared/
    05_isotope_view.fql              # typed view; decodes header scalars
    10_latency_report.fql            # INSERT INTO: avg/min/max latency by origin × topic
    20_topology_report.fql           # INSERT INTO: produce-edge counts per minute
    30_hop_distribution.fql          # INSERT INTO: hop-count buckets per topic per minute
    40_coverage_report.fql           # INSERT INTO: distinct traces per topic per minute
    60_stuck_trace_report.fql        # Phase 2 — stuck-trace alerts via STUCK_TRACE_PTF
    70_latency_percentiles_report.fql # Phase 2 — INSERT INTO: p50/p95/p99 via LATENCY_PERCENTILES UDAF

ptf/src/main/proto/
  ai/signalroom/kafka/isotope/proto/reports/
    reports.proto                    # 5 messages: one per sink. Compiled into
                                     # isotope-flink-udf.jar via the protobuf
                                     # Gradle plugin in ptf/build.gradle.
```

`window_start` / `window_end` are emitted as `BIGINT` epoch millis on
the wire (not `TIMESTAMP_LTZ`) because Flink's protobuf format maps
`google.protobuf.Timestamp` to a nested `ROW(seconds, nanos)` rather
than to a Flink `TIMESTAMP` — we sidestep the impedance mismatch by
declaring `BIGINT` columns and `CAST(window_start AS BIGINT)`-ing
inside the INSERT statements. Consumers can rehydrate with
`TO_TIMESTAMP_LTZ(window_start, 3)`.

The `shared/` `INSERT INTO` files don't reference `'connector'` or any
environment-specific configs — they target the table names declared by
`cp/05_report_sinks.fql` (on CP) or by CCAF's managed topic catalog
(on CCAF), and they read from the `isotope` view declared by
`shared/05_isotope_view.fql`, which in turn reads from `isotope_raw`.

## How the reports read the isotope

`IsotopeProducerInterceptor` writes seven headers on every record:

| Header | Type | Purpose |
|---|---|---|
| `x-isotope`                | JSON bytes | Full hop array — carried forward for propagation; not read by these reports |
| `x-isotope-trace-id`       | UTF-8 hex (32 chars) | Per-trace identity |
| `x-isotope-origin-ts`      | UTF-8 decimal | Epoch ms when the trace started |
| `x-isotope-origin-service` | UTF-8 string | Service that started the trace |
| `x-isotope-this-service`   | UTF-8 string | Service that produced this record |
| `x-isotope-this-topic`     | UTF-8 string | Topic this record was produced to |
| `x-isotope-hop-count`      | UTF-8 decimal | Hop count including this hop |

The six scalar `x-isotope-*` headers are what `shared/05_isotope_view.fql`
decodes — `CAST(headers['x-isotope-…'] AS STRING)` with one more cast to
`BIGINT`/`INT` for the numeric ones. No UDF, no JSON-array unnesting, works
identically on both Flink flavors.

### Message value format

The demo topics carry **SR-framed Protobuf** values
(`ai.signalroom.kafka.isotope.proto.DemoEvent`). The Phase 1 reports
declare `'value.format' = 'raw'` in their source DDL because they only
read headers — Flink doesn't need to decode the value at all. If you
want to project value fields too, switch the source DDL to
`'value.format' = 'protobuf-confluent'` (CCAF) or
`'value.format' = 'protobuf'` with `'value.protobuf.schema-registry.url'`
(CP) and add the column projections to `00_source_table.fql`.

## Deploying on CP Flink (Minikube)

```bash
# Bring up the stack
make minikube-start
make cp-up
make flink-up

# Apply the source table for your topic(s).
# Edit cp/00_source_table.fql first: set 'topic' = '<your-topic>'.
kubectl cp flink/sql/cp/00_source_table.fql confluent/<flink-jm-pod>:/tmp/
kubectl exec -n confluent <flink-jm-pod> -- sql-client.sh -f /tmp/00_source_table.fql

# Then apply the shared views and reports
for f in flink/sql/shared/*.fql; do
    kubectl cp "$f" confluent/<flink-jm-pod>:/tmp/
    kubectl exec -n confluent <flink-jm-pod> -- sql-client.sh -f "/tmp/$(basename "$f")"
done
```

(Substitute `<flink-jm-pod>` with the actual JobManager pod name, e.g.
from `kubectl get pods -n confluent -l component=jobmanager`.)

Then query interactively:
```sql
SELECT * FROM latency_report_1m;
SELECT * FROM topology_report_1m WHERE window_start > NOW() - INTERVAL '10' MINUTE;
```

## Deploying on CCAF

```bash
# Each .fql file becomes one statement (or statement set)
confluent flink statement create isotope-source --sql-file flink/sql/cc/00_source_table.fql
confluent flink statement create isotope-view   --sql-file flink/sql/shared/05_isotope_view.fql
confluent flink statement create lat-1m         --sql-file flink/sql/shared/10_latency_report.fql
confluent flink statement create top-1m         --sql-file flink/sql/shared/20_topology_report.fql
confluent flink statement create hop-1m         --sql-file flink/sql/shared/30_hop_distribution.fql
confluent flink statement create cov-1m         --sql-file flink/sql/shared/40_coverage_report.fql
```

The CCAF source file is a `CREATE VIEW` because CCAF auto-registers topics
as tables. Edit `cc/00_source_table.fql` to point at your actual topic
name (`FROM "<your-topic>"`).

## Phase 2 — PTF / UDAF JAR

Two of the Phase-1 caveats (no percentiles, no per-trace state for
stuck-trace detection) are addressed by a separate JAR built from the
`ptf/` subproject:

- `LatencyPercentilesUDAF` — T-Digest aggregate. Used by
  `shared/70_latency_percentiles_report.fql` to emit p50/p95/p99 per
  (origin_service × topic) per minute.
- `StuckTracePTF` — Process Table Function with per-trace state and
  event-time timers. Used by `shared/60_stuck_trace_report.fql` to emit
  one alert per trace that doesn't progress for `staleness_seconds`.

### Building the JAR

```bash
./gradlew :ptf:shadowJar
# → ptf/build/libs/isotope-flink-udf.jar
```

The shadow JAR bundles `com.tdunning:t-digest:3.3` (the only non-Flink
runtime dependency). The Flink table-api jars are `compileOnly` because
they're provided by the Flink runtime.

The JAR is compiled against Apache Flink 2.1.2, matching
`confluentinc/cp-flink:2.1.2-cp1-java21` (the Minikube image). CCAF
runs a managed Flink version, but accepts JARs compiled against 2.x.

### Deploying on CP Flink (Minikube)

```bash
# Copy JAR into the JobManager pod (and the TaskManagers, via the same
# image's /opt/flink/lib if you want it on a persistent path).
JM_POD=$(kubectl get pods -n confluent -l component=jobmanager -o jsonpath='{.items[0].metadata.name}')
kubectl cp ptf/build/libs/isotope-flink-udf.jar confluent/$JM_POD:/opt/flink/lib/

# Then register the functions:
kubectl exec -n confluent $JM_POD -- sql-client.sh -f /path/to/01_register_functions.fql
```

Alternatively, drop the `USING JAR` clauses in `cp/01_register_functions.fql`
and start `sql-client.sh` with `-j /path/to/isotope-flink-udf.jar` to
load it into the session classpath instead of as a registered artifact.

### Deploying on CCAF

CCAF requires uploading the JAR as a "Flink artifact" via the Confluent
CLI before SQL `CREATE FUNCTION` can reference it. The
`cc/01_register_functions.fql` file expects you to substitute the
returned artifact ID:

```bash
ARTIFACT_ID=$(confluent flink artifact create isotope-flink-udf \
    --cloud AWS --region us-east-1 \
    --environment <env-id> \
    --artifact-file ptf/build/libs/isotope-flink-udf.jar \
    --output json | jq -r '.id')

# Substitute <ARTIFACT_ID> in cc/01_register_functions.fql, then:
confluent flink statement create register-functions \
    --sql-file flink/sql/cc/01_register_functions.fql
```

## Remaining caveats

- **Consume-side blindness.** Reports show *produce* edges only — there's
  no consumer interceptor emitting trace events today. To detect "trace
  produced but never consumed end-to-end," you can either (a) instrument
  the consumer side, or (b) use `STUCK_TRACE_PTF` with a staleness
  threshold that bounds expected end-to-end latency.
- **Coverage is approximate.** Bucketing by 1-minute tumbling windows
  misattributes records that cross the boundary. The `StuckTracePTF`
  approach handles this correctly (event-time timers); the older
  `40_coverage_report.fql` does not.
- **Single-topic source.** Both `00_source_table.fql` files wire one
  topic. For multi-topic pipelines, either set `topic-pattern` (CP only)
  or define one source table per topic and `UNION ALL` them in
  `05_isotope_view.fql`.
- **PTF API binding.** `StuckTracePTF` uses the Flink 2.1.x PTF API
  (`@StateHint`-parameter state, `ctx.timeContext(Long.class)` for
  event-time timers). If you upgrade to a newer Flink, re-verify the
  annotations against the target version's `ProcessTableFunction`
  contract before redeploying.
