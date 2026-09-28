package ai.signalroom.kafka.isotope.flink;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Entry point for the opt-in <b>lake</b>: a second CMF Application that copies
 * the isotope lineage streams into Apache Iceberg (see {@code docs/lake.md}).
 *
 * <p>Why a separate application rather than more INSERTs in
 * {@link IsotopeReportsJob}: the reports share one StatementSet, which is one
 * failure domain. An Iceberg sink there would let a catalog or object-store
 * fault stop all seven reports. This job reads the same Kafka topics on its own,
 * so the two share nothing at runtime. It ships in the same shadow JAR and is
 * selected by the application's {@code entryClass}.
 *
 * <p>Always writes {@code lake.isotope.hops} (one row per produced hop). With
 * {@value #STATE_PROVENANCE_FLAG} it also copies the {@code isotope_state_provenance}
 * topic into {@code lake.isotope.state_provenance} — where, unlike in Flink SQL,
 * a recursive query can walk the {@code parents} chain end to end.
 */
public final class IsotopeLakeJob {

    /** DDL files, applied in dependency order. */
    private static final List<String> DDL_FILES = List.of(
            "00_source_table.fql",   // source tables + isotope_raw view
            "05_isotope_view.fql",   // isotope (produced-record) view
            "90_lake_catalog.fql");  // lake group id, Iceberg catalog, hops table

    /** Lake INSERT files — each contributes exactly one INSERT to the StatementSet. */
    private static final List<String> LAKE_FILES = List.of(
            "95_lake_hops.fql");

    /** Adds the state-provenance copy; the deploy script passes it when both features are on. */
    private static final String STATE_PROVENANCE_FLAG = "--state-provenance";

    private static final List<String> STATE_PROVENANCE_DDL_FILES = List.of(
            "09_state_provenance_sinks.fql",       // isotope_state_provenance (Kafka)
            "91_lake_state_provenance_sink.fql");  // lake.isotope.state_provenance

    private static final List<String> STATE_PROVENANCE_FILES = List.of(
            "96_lake_state_provenance.fql");

    /**
     * Iceberg commits on each checkpoint, so this interval is both the lake's
     * freshness and its file size. Slower than the reports' 30s: every commit
     * writes a data file per table, and at demo volume a longer interval is
     * what keeps those files from being tiny.
     */
    private static final long CHECKPOINT_INTERVAL_MS = 60_000L;

    private IsotopeLakeJob() {
    }

    public static void main(String[] args) throws Exception {
        final boolean stateProvenance = List.of(args).contains(STATE_PROVENANCE_FLAG);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // Same pin as IsotopeReportsJob, so event_time lands in the lake as the
        // same instant the reports saw.
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        List<String> ddlFiles = new ArrayList<>(DDL_FILES);
        List<String> insertFiles = new ArrayList<>(LAKE_FILES);
        if (stateProvenance) {
            ddlFiles.addAll(STATE_PROVENANCE_DDL_FILES);
            insertFiles.addAll(STATE_PROVENANCE_FILES);
        }

        for (String file : ddlFiles) {
            for (String stmt : IsotopeReportsJob.statements(IsotopeReportsJob.readResource("sql/" + file))) {
                tableEnv.executeSql(stmt);
            }
        }

        StatementSet lake = tableEnv.createStatementSet();
        for (String file : insertFiles) {
            List<String> stmts = IsotopeReportsJob.statements(IsotopeReportsJob.readResource("sql/" + file));
            lake.addInsertSql(stmts.get(stmts.size() - 1));
        }
        lake.execute();
    }
}
