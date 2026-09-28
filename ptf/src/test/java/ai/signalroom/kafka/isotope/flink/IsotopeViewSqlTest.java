package ai.signalroom.kafka.isotope.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.TreeMap;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Runs the shipped {@code 05_isotope_view.fql} — read from the same classpath
 * resource {@link IsotopeReportsJob} deploys — on a local MiniCluster, over an
 * in-memory {@code isotope_raw}.
 *
 * <p>Pins {@code latency_ms} to the millisecond. The view once computed it with
 * {@code TIMESTAMPDIFF(MILLISECOND, ...)}, which Flink 2.1 truncates to whole
 * seconds, so every sub-second hop reported 0 and the latency report's
 * min/avg/max were all zero. Run under a non-UTC zone too: the replacement must
 * not depend on {@code table.local-time-zone}.
 */
class IsotopeViewSqlTest {

    private static final long ORIGIN_MS = 1_790_543_203_423L;

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "America/New_York"})
    void latencyIsExactToTheMillisecond(String zone) throws Exception {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        t.getConfig().set("table.local-time-zone", zone);

        // Event times relative to the origin: sub-second (the demo's case), a
        // hop that crosses a second boundary with a LOWER millisecond field
        // than the origin's, and multi-second.
        long[] offsets = {0L, 177L, 1_677L, 42_001L};
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < offsets.length; i++) {
            if (i > 0) rows.append(", ");
            rows.append("(MAP['x-isotope-trace-id', CAST('t").append(i).append("' AS BYTES), ")
                .append("'x-isotope-origin-ts', CAST('").append(ORIGIN_MS).append("' AS BYTES)], ")
                .append("TO_TIMESTAMP_LTZ(").append(ORIGIN_MS + offsets[i]).append(", 3))");
        }
        t.executeSql("CREATE TEMPORARY VIEW isotope_raw AS SELECT * FROM (VALUES "
                + rows + ") AS r(`headers`, `event_time`)");

        for (String stmt : IsotopeReportsJob.statements(
                IsotopeReportsJob.readResource("sql/05_isotope_view.fql"))) {
            t.executeSql(stmt);
        }

        // Keyed by trace_id and compared as a map: a streaming query cannot
        // ORDER BY a non-time attribute.
        Map<String, Long> latencies = new TreeMap<>();
        try (CloseableIterator<Row> it =
                     t.executeSql("SELECT trace_id, latency_ms FROM isotope").collect()) {
            it.forEachRemaining(r -> latencies.put(
                    r.getFieldAs("trace_id"), r.<Number>getFieldAs("latency_ms").longValue()));
        }
        assertEquals(Map.of("t0", 0L, "t1", 177L, "t2", 1_677L, "t3", 42_001L), latencies, "zone " + zone);
    }
}
