package ai.signalroom.kafka.isotope.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Does a quiet topic stop event time for the reports that read the union?
 *
 * <p>Same shape as the CP deployment: one unbounded single-partition source per
 * topic, each with the DDL's {@code event_time - INTERVAL '5' SECOND} watermark,
 * joined by {@code UNION ALL}. The sources stay open, because a source that
 * finishes emits a final watermark and hides the behaviour under test.
 *
 * <p>In every case {@code orders.placed} carries 3 minutes of event time, which
 * is far more than the 60s staleness threshold and the 1-minute windows need.
 *
 * <p>The stall cases set no idle timeout, so they pin Flink's default behaviour:
 * they are why {@link IsotopeReportsJob#SOURCE_IDLE_TIMEOUT} exists.
 */
class QuietTopicWatermarkTest {

    private static final long T0 = 1_790_543_203_423L;

    /** Long enough for the control, which fires within a second or two. */
    private static final long WAIT_SECONDS = 15;

    private static final String STUCK_QUERY =
            "SELECT trace_id FROM TABLE(STUCK_TRACE_PTF("
                    + "input => TABLE isotope PARTITION BY trace_id, "
                    + "on_time => DESCRIPTOR(event_time), uid => 'quiet-topic-test'))";

    private static final String WINDOW_QUERY =
            "SELECT window_end, COUNT(*) AS hops "
                    + "FROM TABLE(TUMBLE(TABLE isotope, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)) "
                    + "GROUP BY window_start, window_end";

    private static Row hop(String trace, String topic, int hopCount, long eventMs) {
        return Row.of(trace, "origin", "orders", "svc", topic, hopCount, eventMs);
    }

    /** One record every 5s of event time for 3 minutes, as the runbook's traffic loop does. */
    private static ArrayList<Row> steadyTraffic(String topic, int hopCount) {
        ArrayList<Row> rows = new ArrayList<>();
        for (int i = 0; i <= 36; i++) {
            rows.add(hop("burst-" + i, topic, hopCount, T0 + i * 5_000L));
        }
        return rows;
    }

    /** Emits its rows, then stays open and silent, like a topic nobody is writing to. */
    private static final class OpenTopic implements SourceFunction<Row> {
        private final ArrayList<Row> rows;
        private volatile boolean running = true;

        OpenTopic(ArrayList<Row> rows) {
            this.rows = rows;
        }

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            for (Row row : rows) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(row);
                }
                // Slow enough for the 200ms periodic watermark to see the progress.
                Thread.sleep(50);
            }
            while (running) {
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** Runs the query over the two topics; true if it emits a row within {@link #WAIT_SECONDS}. */
    private static boolean emitsARow(
            String query, ArrayList<Row> placed, ArrayList<Row> enriched, String idleTimeout) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);
        t.getConfig().set("table.local-time-zone", "UTC");
        if (idleTimeout != null) {
            t.getConfig().set("table.exec.source.idle-timeout", idleTimeout);
        }

        register(env, t, "placed_topic", placed);
        register(env, t, "enriched_topic", enriched);
        t.executeSql("CREATE TEMPORARY VIEW isotope AS "
                + "SELECT trace_id, origin_service, pipeline, this_service, this_topic, hop_count, event_time "
                + "FROM placed_topic UNION ALL "
                + "SELECT trace_id, origin_service, pipeline, this_service, this_topic, hop_count, event_time "
                + "FROM enriched_topic");
        t.createTemporarySystemFunction("STUCK_TRACE_PTF", StuckTracePTF.class);

        TableResult result = t.executeSql(query);
        ExecutorService waiter = Executors.newSingleThreadExecutor();
        try (CloseableIterator<Row> it = result.collect()) {
            Future<Boolean> firstRow = waiter.submit(it::hasNext);
            try {
                return firstRow.get(WAIT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException stalled) {
                return false;
            }
        } finally {
            waiter.shutdownNow();
            result.getJobClient().ifPresent(job -> job.cancel());
        }
    }

    private static void register(
            StreamExecutionEnvironment env, StreamTableEnvironment t, String name, ArrayList<Row> rows) {
        RowTypeInfo type = new RowTypeInfo(
                new TypeInformation<?>[] {
                        Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.LONG},
                new String[] {
                        "trace_id", "origin_service", "pipeline", "this_service", "this_topic", "hop_count", "event_ms"});
        DataStream<Row> stream = env.addSource(new OpenTopic(rows), name, type);
        t.createTemporaryView(name, t.fromDataStream(stream, Schema.newBuilder()
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(event_ms, 3)")
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .build()));
    }

    private static ArrayList<Row> oneOldRecord() {
        return new ArrayList<>(List.of(hop("old", "orders.enriched", 2, T0)));
    }

    // ---- control: every topic has traffic, so event time advances ----

    @Test
    void stuckAlertFiresWhenEveryTopicHasTraffic() throws Exception {
        assertEquals(true, emitsARow(
                STUCK_QUERY, steadyTraffic("orders.placed", 1), steadyTraffic("orders.enriched", 2), null));
    }

    @Test
    void windowFiresWhenEveryTopicHasTraffic() throws Exception {
        assertEquals(true, emitsARow(
                WINDOW_QUERY, steadyTraffic("orders.placed", 1), steadyTraffic("orders.enriched", 2), null));
    }

    // ---- the suspected stall: traffic on one topic, the other quiet ----

    @Test
    void stuckAlertWithATopicThatWentQuiet() throws Exception {
        assertEquals(false, emitsARow(STUCK_QUERY, steadyTraffic("orders.placed", 1), oneOldRecord(), null));
    }

    @Test
    void stuckAlertWithATopicNeverWrittenTo() throws Exception {
        assertEquals(false, emitsARow(STUCK_QUERY, steadyTraffic("orders.placed", 1), new ArrayList<>(), null));
    }

    @Test
    void windowWithATopicThatWentQuiet() throws Exception {
        assertEquals(false, emitsARow(WINDOW_QUERY, steadyTraffic("orders.placed", 1), oneOldRecord(), null));
    }

    // ---- the fix: 1s here only to keep the test fast ----

    /** The job sets this in code and the SQL Client in DDL, so nothing else holds them together. */
    @Test
    void theJobAndTheSqlClientUseTheSameIdleTimeout() throws Exception {
        assertEquals(true, IsotopeReportsJob.readResource("sql/00_source_table.fql").contains(
                "SET 'table.exec.source.idle-timeout' = '" + IsotopeReportsJob.SOURCE_IDLE_TIMEOUT + "';"));
    }

    @Test
    void stuckAlertWithAQuietTopicAndAnIdleTimeout() throws Exception {
        assertEquals(true, emitsARow(STUCK_QUERY, steadyTraffic("orders.placed", 1), oneOldRecord(), "1 s"));
    }

    @Test
    void windowWithAQuietTopicAndAnIdleTimeout() throws Exception {
        assertEquals(true, emitsARow(WINDOW_QUERY, steadyTraffic("orders.placed", 1), oneOldRecord(), "1 s"));
    }
}
