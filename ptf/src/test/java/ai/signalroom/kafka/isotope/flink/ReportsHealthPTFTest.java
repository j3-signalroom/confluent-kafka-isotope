package ai.signalroom.kafka.isotope.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
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
 * Runs {@link ReportsHealthPTF} on a local MiniCluster with a watermark after every
 * row, so a timer fires at its point in the sequence and not all at once at the end
 * of the input.
 *
 * <p>The source is bounded, and its final watermark fires any timer still armed. So
 * a window still waiting on its grace period when a list ends alerts here, and each
 * "no alert" case below has to end with nothing overdue.
 */
class ReportsHealthPTFTest {

    private static final long MINUTE = 60_000L;
    private static final long GRACE = 2 * MINUTE;

    /** The start of a one-minute window, so offsets below read as minutes past it. */
    private static final long W0 = 1_790_543_160_000L;

    private static Row traffic(long eventMs) {
        return Row.of("isotope-reports", ReportsHealthPTF.TRAFFIC, null, eventMs);
    }

    /** A report row for a window ending at {@code windowEnd}, written at {@code writtenMs}. */
    private static Row report(long windowEnd, long writtenMs) {
        return Row.of("isotope-reports", ReportsHealthPTF.REPORT, windowEnd, writtenMs);
    }

    /** Emits a watermark per row, with the 5s allowance the Kafka source tables declare. */
    private static final class EveryRow implements WatermarkGenerator<Row> {
        @Override
        public void onEvent(Row row, long eventMs, WatermarkOutput output) {
            output.emitWatermark(new Watermark(eventMs - 5_000L));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
        }
    }

    private static List<Row> run(List<Row> arrivals) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // One reader, so the list order IS the arrival order the function sees.
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);
        t.getConfig().set("table.local-time-zone", "UTC");

        RowTypeInfo type = new RowTypeInfo(
                new TypeInformation<?>[] {Types.STRING, Types.STRING, Types.LONG, Types.LONG},
                new String[] {"monitored_job", "kind", "window_end", "event_ms"});
        WatermarkStrategy<Row> strategy = WatermarkStrategy.<Row>forGenerator(ctx -> new EveryRow())
                .withTimestampAssigner((r, ts) -> r.<Long>getFieldAs("event_ms"));
        DataStream<Row> stream = env.fromData(type, arrivals.toArray(new Row[0]))
                .assignTimestampsAndWatermarks(strategy);
        t.createTemporaryView("health_signals", t.fromDataStream(stream, Schema.newBuilder()
                .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                .watermark("event_time", "SOURCE_WATERMARK()")
                .build()));
        t.createTemporarySystemFunction("REPORTS_HEALTH", ReportsHealthPTF.class);

        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = t.executeSql(
                "SELECT monitored_job, status, overdue_window_end, last_reported_window_end, "
                        + "last_traffic_ts_ms, event_ts_ms, stalled_for_ms "
                        + "FROM TABLE(REPORTS_HEALTH("
                        + "input => TABLE health_signals PARTITION BY monitored_job, "
                        + "on_time => DESCRIPTOR(event_time), uid => 'health-test'))")
                .collect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }

    private static List<String> statuses(List<Row> alerts) {
        return alerts.stream().map(a -> a.<String>getFieldAs("status")).toList();
    }

    /** Traffic every 10s from {@code fromMs} up to and including {@code toMs}. */
    private static List<Row> steadyTraffic(long fromMs, long toMs) {
        List<Row> rows = new ArrayList<>();
        for (long t = fromMs; t <= toMs; t += 10_000L) {
            rows.add(traffic(t));
        }
        return rows;
    }

    @Test
    void reportsThatKeepUpNeverAlert() throws Exception {
        List<Row> rows = new ArrayList<>();
        for (int minute = 0; minute < 6; minute++) {
            rows.addAll(steadyTraffic(W0 + minute * MINUTE, W0 + minute * MINUTE + 50_000L));
            rows.add(traffic(W0 + (minute + 1) * MINUTE + 6_000L));
            rows.add(report(W0 + (minute + 1) * MINUTE, W0 + (minute + 1) * MINUTE + 8_000L));
        }
        // The last window hasn't closed, so nothing is overdue when the input ends.
        rows.add(traffic(W0 + 6 * MINUTE + 10_000L));
        assertEquals(List.of(), run(rows));
    }

    @Test
    void trafficWithNoReportsAlertsOnceHoweverLongItLasts() throws Exception {
        List<Row> alerts = run(steadyTraffic(W0 + 30_000L, W0 + 20 * MINUTE));

        assertEquals(List.of(ReportsHealthPTF.STALLED), statuses(alerts));
        Row stalled = alerts.get(0);
        assertEquals(W0 + MINUTE, (long) stalled.<Long>getFieldAs("overdue_window_end"));
        // The first row at or past the window's end + 5s is the one at +70s.
        assertEquals(W0 + MINUTE + 10_000L + GRACE, (long) stalled.<Long>getFieldAs("event_ts_ms"));
        assertEquals(GRACE, (long) stalled.<Long>getFieldAs("stalled_for_ms"));
    }

    @Test
    void aReportCatchingUpRecoversTheOpenAlert() throws Exception {
        long end = W0 + 10 * MINUTE;
        List<Row> rows = steadyTraffic(W0 + 30_000L, end);
        rows.add(report(W0 + 9 * MINUTE, end + 1_000L));

        List<Row> alerts = run(rows);

        assertEquals(List.of(ReportsHealthPTF.STALLED, ReportsHealthPTF.RECOVERED), statuses(alerts));
        Row recovered = alerts.get(1);
        assertEquals(W0 + MINUTE, (long) recovered.<Long>getFieldAs("overdue_window_end"));
        assertEquals(W0 + 9 * MINUTE, (long) recovered.<Long>getFieldAs("last_reported_window_end"));
        assertEquals(end + 1_000L, (long) recovered.<Long>getFieldAs("event_ts_ms"));
        assertEquals(end + 1_000L - (W0 + MINUTE + 10_000L + GRACE),
                (long) recovered.<Long>getFieldAs("stalled_for_ms"));
    }

    @Test
    void aReportOlderThanTheOverdueWindowDoesNotClearIt() throws Exception {
        // Traffic starts at minute 5, so minute 6 is the overdue window. A row for
        // minute 3 is backlog, and says nothing about the current window.
        List<Row> rows = steadyTraffic(W0 + 5 * MINUTE, W0 + 15 * MINUTE);
        rows.add(report(W0 + 3 * MINUTE, W0 + 15 * MINUTE + 1_000L));

        assertEquals(List.of(ReportsHealthPTF.STALLED), statuses(run(rows)));
    }

    @Test
    void sparseTrafficIsNotMistakenForAStall() throws Exception {
        // The second record is what lets the reports job close the first's window, so
        // the report can't appear any sooner, however long the gap between them was.
        assertEquals(List.of(), run(List.of(
                traffic(W0 + 30_000L),
                traffic(W0 + 30 * MINUTE),
                report(W0 + MINUTE, W0 + 30 * MINUTE + 2_000L))));
    }

    @Test
    void aReportReadBeforeTheTrafficThatClosesItsWindowStillCounts() throws Exception {
        // Reports and traffic are separate topics, so nothing fixes their read order.
        assertEquals(List.of(), run(List.of(
                traffic(W0 + 30_000L),
                report(W0 + MINUTE, W0 + MINUTE + 3_000L),
                traffic(W0 + MINUTE + 6_000L))));
    }

    @Test
    void aSecondStallAfterRecoveringAlertsAgain() throws Exception {
        List<Row> rows = steadyTraffic(W0 + 30_000L, W0 + 5 * MINUTE);
        rows.add(report(W0 + 5 * MINUTE, W0 + 5 * MINUTE + 1_000L));
        rows.addAll(steadyTraffic(W0 + 5 * MINUTE + 10_000L, W0 + 15 * MINUTE));

        assertEquals(
                List.of(ReportsHealthPTF.STALLED, ReportsHealthPTF.RECOVERED, ReportsHealthPTF.STALLED),
                statuses(run(rows)));
    }

    @Test
    void reportsAloneNeverAlert() throws Exception {
        assertEquals(List.of(), run(List.of(
                report(W0 + MINUTE, W0 + MINUTE + 8_000L),
                report(W0 + 2 * MINUTE, W0 + 2 * MINUTE + 8_000L))));
    }

    @Test
    void trafficThatStopsInsideAWindowHasNothingOverdue() throws Exception {
        // The documented limit: with no later record, no window is known to be closed.
        assertEquals(List.of(), run(List.of(traffic(W0 + 10_000L), traffic(W0 + 20_000L))));
    }
}
