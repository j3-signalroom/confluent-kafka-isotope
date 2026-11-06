package ai.signalroom.kafka.isotope.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Runs {@link StuckTracePTF} on a local MiniCluster, fed in a controlled
 * <em>arrival</em> order with a real watermark.
 *
 * <p>Two regressions are pinned here. A trace that reached the end of the
 * pipeline used to alert as "stuck" 60s later, because nothing told the function
 * that going quiet is what a finished trace does. And {@code isotope_raw} is a
 * {@code UNION ALL} of one table per topic, so a trace's hops arrive in arbitrary
 * order: the function used to treat the last-<em>processed</em> hop as the latest
 * one, which misreported where the trace stopped and could pull its timer back.
 *
 * <p>The source is bounded, so the final watermark fires every timer still armed
 * when the input ends: a trace with no alert here has no armed timer to fire.
 */
class StuckTracePTFTest {

    private static final long T0 = 1_790_543_203_423L;

    /** One `isotope` view row, reduced to the columns the function reads. */
    private static Row hop(String trace, String service, String topic, int hopCount, long eventMs) {
        return Row.of(trace, "order-intake-service", "orders", service, topic, hopCount, eventMs);
    }

    private static Row placed(String trace, long eventMs) {
        return hop(trace, "order-intake-service", "orders.placed", 1, eventMs);
    }

    private static Row enriched(String trace, long eventMs) {
        return hop(trace, "order-enrichment-service", "orders.enriched", 2, eventMs);
    }

    private static Row fulfilled(String trace, long eventMs) {
        return hop(trace, "order-fulfillment-service", "orders.fulfilled", 3, eventMs);
    }

    private static List<Row> run(List<Row> arrivals) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // One reader, so the list order IS the arrival order the function sees.
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);
        t.getConfig().set("table.local-time-zone", "UTC");

        RowTypeInfo type = new RowTypeInfo(
                new TypeInformation<?>[] {
                        Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.LONG},
                new String[] {
                        "trace_id", "origin_service", "pipeline", "this_service", "this_topic", "hop_count", "event_ms"});
        // The same 5s bounded out-of-orderness the Kafka source tables declare.
        DataStream<Row> stream = env.fromData(type, arrivals.toArray(new Row[0]))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((r, ts) -> r.<Long>getFieldAs("event_ms")));
        t.createTemporaryView("isotope", t.fromDataStream(stream, Schema.newBuilder()
                .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                .watermark("event_time", "SOURCE_WATERMARK()")
                .build()));
        t.createTemporarySystemFunction("STUCK_TRACE_PTF", StuckTracePTF.class);

        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = t.executeSql(
                "SELECT trace_id, last_service, last_topic, last_hop_count, last_seen_ts_ms, stuck_for_ms "
                        + "FROM TABLE(STUCK_TRACE_PTF("
                        + "input => TABLE isotope PARTITION BY trace_id, "
                        + "on_time => DESCRIPTOR(event_time), uid => 'stuck-trace-test'))")
                .collect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }

    private static void assertAlert(Row alert, String trace, String service, String topic, int hopCount, long lastSeenMs) {
        assertEquals(trace, alert.getFieldAs("trace_id"));
        assertEquals(service, alert.getFieldAs("last_service"));
        assertEquals(topic, alert.getFieldAs("last_topic"));
        assertEquals(hopCount, (int) alert.<Integer>getFieldAs("last_hop_count"));
        assertEquals(lastSeenMs, (long) alert.<Long>getFieldAs("last_seen_ts_ms"));
        assertEquals(60_000L, (long) alert.<Long>getFieldAs("stuck_for_ms"));
    }

    @Test
    void aTraceThatReachesTheTerminalTopicDoesNotAlert() throws Exception {
        assertEquals(List.of(), run(List.of(
                placed("done", T0),
                enriched("done", T0 + 100),
                fulfilled("done", T0 + 200))));
    }

    @Test
    void aTraceThatStopsShortOfTheTerminalTopicAlertsWithWhereItStopped() throws Exception {
        List<Row> out = run(List.of(
                placed("stuck", T0),
                enriched("stuck", T0 + 100)));

        assertEquals(1, out.size());
        assertAlert(out.get(0), "stuck", "order-enrichment-service", "orders.enriched", 2, T0 + 100);
    }

    @Test
    void onlyTheUnfinishedTraceAlertsWhenTheyAreInterleaved() throws Exception {
        List<Row> out = run(List.of(
                placed("done", T0),
                placed("stuck", T0 + 10),
                enriched("done", T0 + 100),
                fulfilled("done", T0 + 200)));

        assertEquals(1, out.size());
        assertAlert(out.get(0), "stuck", "order-intake-service", "orders.placed", 1, T0 + 10);
    }

    /** The terminal hop can be read first: a union has no order across its topics. */
    @Test
    void aFinishedTraceStaysFinishedWhenItsEarlierHopsArriveAfterTheTerminalHop() throws Exception {
        assertEquals(List.of(), run(List.of(
                fulfilled("done", T0 + 200),
                placed("done", T0),
                enriched("done", T0 + 100))));
    }

    /** A retried hop can carry a later timestamp than the terminal hop it precedes in the pipeline. */
    @Test
    void theTerminalHopFinishesTheTraceEvenBehindARetriedHop() throws Exception {
        assertEquals(List.of(), run(List.of(
                placed("done", T0),
                enriched("done", T0 + 300),
                fulfilled("done", T0 + 200))));
    }

    /** A hop behind the latest one is not where the trace stopped. */
    @Test
    void aLateHopDoesNotRewriteWhereTheTraceStopped() throws Exception {
        List<Row> out = run(List.of(
                enriched("stuck", T0 + 100),
                placed("stuck", T0)));

        assertEquals(1, out.size());
        assertAlert(out.get(0), "stuck", "order-enrichment-service", "orders.enriched", 2, T0 + 100);
    }
}
