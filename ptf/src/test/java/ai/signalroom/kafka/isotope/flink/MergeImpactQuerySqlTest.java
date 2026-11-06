package ai.signalroom.kafka.isotope.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Runs the shipped {@code 82_merge_impact_query.fql} on a local MiniCluster, over
 * in-memory stand-ins for the alert topic and the merge edge topic.
 *
 * <p>The fixture has three merged records: {@code m1} fed by a mix of stuck and
 * healthy traces, {@code m2} fed only by a stuck trace, and {@code m3} unaffected.
 * Trace {@code t2} alerts twice and has its edge replayed, because neither may
 * inflate a count. Trace {@code t9} alerts without having fed any merge.
 */
class MergeImpactQuerySqlTest {

    private static final long M1 = 60_000L;
    private static final long M2 = 120_000L;
    private static final long M3 = 180_000L;

    private static TableEnvironment tables() {
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        t.executeSql("CREATE TEMPORARY VIEW stuck_trace_alerts_1m AS SELECT * FROM (VALUES "
                + alert("t2", "orders.placed", 1_000L) + ", "
                + alert("t2", "orders.enriched", 2_000L) + ", "
                + alert("t4", "orders.placed", 3_000L) + ", "
                + alert("t9", "orders.placed", 4_000L)
                + ") AS r(trace_id, last_service, last_topic, last_seen_ts_ms)");
        t.executeSql("CREATE TEMPORARY VIEW isotope_merge_edge_markers AS SELECT * FROM (VALUES "
                + edge("m1", M1, "t1") + ", "
                + edge("m1", M1, "t2") + ", "
                + edge("m1", M1, "t2") + ", "
                + edge("m1", M1, "t3") + ", "
                + edge("m2", M2, "t4") + ", "
                + edge("m3", M3, "t5")
                + ") AS r(merge_trace_id, window_start, window_end, contributing_trace_id, contributing_records)");
        return t;
    }

    private static String alert(String trace, String topic, long lastSeenMs) {
        return "('" + trace + "', 'svc', '" + topic + "', CAST(" + lastSeenMs + " AS BIGINT))";
    }

    private static String edge(String merge, long windowEnd, String trace) {
        return "('" + merge + "', CAST(" + (windowEnd - 60_000L) + " AS BIGINT), CAST(" + windowEnd + " AS BIGINT), '"
                + trace + "', CAST(1 AS BIGINT))";
    }

    private static List<String> queries() throws Exception {
        return IsotopeReportsJob.statements(IsotopeReportsJob.readResource("sql/82_merge_impact_query.fql"));
    }

    @Test
    void shipsExactlyTheTwoQueries() throws Exception {
        assertEquals(2, queries().size());
    }

    @Test
    void namesTheMergedRecordsEachStuckTraceFedAndWhereItStopped() throws Exception {
        List<String> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tables().executeSql(queries().get(0)).collect()) {
            it.forEachRemaining(r -> rows.add(r.getFieldAs("trace_id") + "@" + r.getFieldAs("last_topic")
                    + "->" + r.getFieldAs("merge_trace_id") + "/" + r.getFieldAs("window_end")));
        }
        rows.sort(null);

        // t2 is two alerts times two delivered copies of its edge. t9 fed no merge.
        assertEquals(List.of(
                "t2@orders.enriched->m1/60000",
                "t2@orders.enriched->m1/60000",
                "t2@orders.placed->m1/60000",
                "t2@orders.placed->m1/60000",
                "t4@orders.placed->m2/120000"), rows);
    }

    @Test
    void countsEachTraceOnceAndKeepsUnaffectedMergedRecords() throws Exception {
        // The LEFT JOIN and aggregate emit updates, so replay them to the final answer.
        Map<String, String> impact = new TreeMap<>();
        try (CloseableIterator<Row> it = tables().executeSql(queries().get(1)).collect()) {
            it.forEachRemaining(r -> {
                String merge = r.getFieldAs("merge_trace_id");
                if (r.getKind() == RowKind.INSERT || r.getKind() == RowKind.UPDATE_AFTER) {
                    impact.put(merge, r.getFieldAs("stuck_traces") + " of " + r.getFieldAs("contributing_traces"));
                } else if (r.getKind() == RowKind.DELETE) {
                    impact.remove(merge);
                }
            });
        }

        assertEquals(Map.of("m1", "1 of 3", "m2", "1 of 1", "m3", "0 of 1"), impact);
    }
}
