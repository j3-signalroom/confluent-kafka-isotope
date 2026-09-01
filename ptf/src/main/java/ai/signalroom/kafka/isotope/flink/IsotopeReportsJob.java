package ai.signalroom.kafka.isotope.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Entry point for the isotope reports as a <b>Confluent Manager for Apache Flink
 * (CMF) Application</b> — a single Flink 2.x job that runs all seven reports.
 *
 * <p>Why an application instead of CMF SQL statements: CMF's SQL-statement runtime
 * ({@code io.confluent.flink.FlinkCompiledPlanExecutor}) ships only in the
 * {@code cp-flink-sql} image, which exists only at Flink 1.19 — and the two
 * percentile / stuck-trace reports are {@link org.apache.flink.table.functions.ProcessTableFunction}s,
 * a Flink 2.x feature. Running the reports as an application on {@code cp-flink:2.1.x}
 * keeps everything on one Flink 2.1 runtime, runs the PTFs natively, reuses the
 * existing open-source-dialect {@code .fql} verbatim (avro-confluent, CREATE VIEW,
 * TIMESTAMPDIFF(MILLISECOND) all work here), and still surfaces in CMF — as a
 * managed application rather than seven statements.
 *
 * <p>The SQL is read from the {@code sql/*.fql} classpath resources (copied from
 * {@code scripts/flink/sql/cp/} at build time — single source of truth with the
 * former sql-client path). DDL is applied first, then the seven {@code INSERT INTO}
 * statements execute together as one {@link StatementSet} (one JobGraph, seven sinks).
 *
 * <p>Optional fan-in provenance: passing {@value #MERGE_PROVENANCE_FLAG} adds a
 * merge collector and its merge-edge sideband (see {@code docs/flink-collector.md}
 * section 3.1). Off — the default — no extra DDL is applied and no extra INSERT
 * joins the StatementSet, so the deployed job is identical to one built without
 * the feature.
 *
 * <p>The two PTFs are registered programmatically from their on-classpath classes,
 * so {@code 01_register_functions.fql} (which does {@code CREATE FUNCTION ... USING JAR})
 * is intentionally skipped.
 */
public final class IsotopeReportsJob {

    /** DDL files, applied in dependency order. */
    private static final List<String> DDL_FILES = List.of(
            "00_source_table.fql",        // 4 source tables + isotope_raw view
            "05_isotope_view.fql",        // isotope (produced-record) view
            "06_consume_events_view.fql", // consume_events view
            "05_report_sinks.fql",        // 7 avro-confluent sink tables
            "07_flink_collector_sink.fql"); // orders.flink_enriched (writable headers)

    /** Report files — each contributes exactly one INSERT to the StatementSet. */
    private static final List<String> REPORT_FILES = List.of(
            "10_latency_report.fql",
            "20_topology_report.fql",
            "25_bipartite_topology_report.fql",
            "30_hop_distribution.fql",
            "40_coverage_report.fql",
            "60_stuck_trace_report.fql",         // STUCK_TRACE_PTF
            "70_latency_percentiles_report.fql", // LATENCY_PERCENTILES
            "75_flink_collector.fql");           // ISOTOPE_APPEND_HOP — collector, not a report

    /** Enables the optional merge-provenance stage. */
    private static final String MERGE_PROVENANCE_FLAG = "--merge-provenance";

    /**
     * Merge-provenance DDL, applied only when {@value #MERGE_PROVENANCE_FLAG} is
     * passed. Kept out of {@link #DDL_FILES} so that off, this job creates
     * nothing extra — no tables, no topics, no schemas.
     */
    private static final List<String> MERGE_PROVENANCE_DDL_FILES = List.of(
            "08_merge_provenance_sinks.fql");

    /**
     * The merge-provenance INSERT pair. Both or neither: the edge rows in
     * {@code 81} are meaningless without the merged records in {@code 80}, and
     * the merged records are unattributable without the edges.
     */
    private static final List<String> MERGE_PROVENANCE_FILES = List.of(
            "80_merge_collector.fql",
            "81_merge_edge_markers.fql");

    private IsotopeReportsJob() {
    }

    public static void main(String[] args) throws Exception {
        // Opt-in fan-in provenance (docs/flink-collector.md 3.1). Off by default:
        // it adds a second collector stage and an edge topic that writes one
        // record per record entering the merge, which is not something every
        // deployment should pay for. CCAF's equivalent switch is the Terraform
        // variable var.enable_merge_provenance.
        final boolean mergeProvenance = List.of(args).contains(MERGE_PROVENANCE_FLAG);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Streaming report jobs aggregate over event-time tumbling windows and
        // write to Kafka sinks — checkpointing must be on.
        env.enableCheckpointing(30_000L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Register the two JAR-backed PTFs from their on-classpath classes (this
        // jar bundles them), so the report SQL can call STUCK_TRACE_PTF /
        // LATENCY_PERCENTILES without a CREATE FUNCTION ... USING JAR step.
        tableEnv.createTemporarySystemFunction("STUCK_TRACE_PTF", StuckTracePTF.class);
        tableEnv.createTemporarySystemFunction("LATENCY_PERCENTILES", LatencyPercentilesPTF.class);
        // Collector-side (in-band propagation): appends a Flink hop to the
        // headers of every record 75_flink_collector.fql forwards. See
        // docs/flink-collector.md.
        tableEnv.createTemporarySystemFunction("ISOTOPE_APPEND_HOP", IsotopeAppendHop.class);
        // Merge-collector side (fan-in provenance). Registered unconditionally —
        // registration is inert until a statement calls it, and the CCAF side
        // registers its functions the same way regardless of the feature flag.
        tableEnv.createTemporarySystemFunction("ISOTOPE_MERGE_TRACE", IsotopeMergeTrace.class);
        tableEnv.createTemporarySystemFunction("ISOTOPE_MERGE_TRACE_ID", IsotopeMergeTraceId.class);

        List<String> ddlFiles = new ArrayList<>(DDL_FILES);
        List<String> insertFiles = new ArrayList<>(REPORT_FILES);
        if (mergeProvenance) {
            ddlFiles.addAll(MERGE_PROVENANCE_DDL_FILES);
            insertFiles.addAll(MERGE_PROVENANCE_FILES);
        }

        // DDL: source tables, views, sinks (each statement applied individually).
        for (String file : ddlFiles) {
            for (String stmt : statements(readResource("sql/" + file))) {
                tableEnv.executeSql(stmt);
            }
        }

        // Every INSERT runs together as a single job — the seven reports plus
        // the collector (75_flink_collector.fql), which is not a report but
        // shares the same source scan, plus the two merge-provenance INSERTs
        // when that feature is on. CCAF's equivalent is EXECUTE STATEMENT
        // SET; it currently submits these as separate statements instead, so
        // each carries its own compute-pool floor.
        StatementSet reports = tableEnv.createStatementSet();
        for (String file : insertFiles) {
            List<String> stmts = statements(readResource("sql/" + file));
            // Each report file is a single INSERT after SET/comment stripping.
            reports.addInsertSql(stmts.get(stmts.size() - 1));
        }
        reports.execute();
    }

    /** Reads a UTF-8 classpath resource, failing loudly if it is missing. */
    private static String readResource(String path) {
        ClassLoader cl = IsotopeReportsJob.class.getClassLoader();
        try (InputStream in = cl.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing bundled SQL resource: " + path);
            }
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                return r.lines().collect(Collectors.joining("\n"));
            }
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Failed reading SQL resource: " + path, e);
        }
    }

    /**
     * Splits an .fql file into individual SQL statements: drops full-line
     * {@code --} comments and standalone {@code SET '...'=...} directives (invalid
     * in Table API), then splits on {@code ;}. Mirrors the deploy-script splitter.
     */
    static List<String> statements(String fql) {
        StringBuilder sb = new StringBuilder();
        for (String line : fql.split("\n", -1)) {
            if (line.strip().startsWith("--")) {
                continue;
            }
            sb.append(line).append('\n');
        }
        List<String> out = new ArrayList<>();
        for (String part : sb.toString().split(";")) {
            String s = part.strip();
            if (s.isEmpty() || s.toUpperCase(java.util.Locale.ROOT).startsWith("SET ")) {
                continue;
            }
            out.add(s);
        }
        return out;
    }
}
