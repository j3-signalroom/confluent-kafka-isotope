# Flink SQL reports

Four reports that read the isotope-tagged stream and surface what's flowing
where, how fast, and how reliably. The SQL is identical between Confluent
Cloud for Apache Flink (CCAF) and Confluent Platform Flink (CP Flink on
Minikube) — only the source-table DDL differs.

## Layout

```
flink/sql/
  cp/
    00_source_table.fql              # CREATE TABLE with 'connector' = 'kafka'
    01_register_functions.fql        # Phase 2 — CREATE FUNCTION … USING JAR
  cc/
    00_source_table.fql              # CREATE VIEW over CCAF's auto-registered topic
    01_register_functions.fql        # Phase 2 — confluent-artifact:// JAR reference
  shared/
    05_isotope_view.fql              # typed view; decodes header scalars
    10_latency_report.fql            # avg/min/max latency by origin × topic
    20_topology_report.fql           # produce-edge counts per minute
    30_hop_distribution.fql          # hop-count buckets per topic per minute
    40_coverage_report.fql           # distinct traces per topic per minute
    60_stuck_trace_report.fql        # Phase 2 — stuck-trace alerts via STUCK_TRACE_PTF
    70_latency_percentiles_report.fql # Phase 2 — p50/p95/p99 via LATENCY_PERCENTILES UDAF
```

The `shared/` files don't reference `'connector'` or any environment-specific
configs — they work against the `isotope_raw` table/view that each `00_…` file
creates.

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

The JAR is compiled against Apache Flink 2.1.1, matching
`confluentinc/cp-flink:2.1.1-cp1-java21` (the Minikube image). CCAF
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
- **PTF API binding.** `StuckTracePTF` uses the Flink 2.1.1 PTF API
  (`@StateHint`-parameter state, `ctx.timeContext(Long.class)` for
  event-time timers). If you upgrade to a newer Flink, re-verify the
  annotations against the target version's `ProcessTableFunction`
  contract before redeploying.
