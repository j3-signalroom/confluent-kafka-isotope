package ai.signalroom.kafka.isotope.flink;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * Runs {@link StateProvenancePTF} on a local MiniCluster, fed in a controlled
 * <em>arrival</em> order with a real watermark, and checks the chain it builds.
 *
 * <p>The regression this pins: the function once made each version's parent
 * the entity's last-<em>processed</em> version. {@code entity_log} is a
 * {@code UNION ALL} of one table per topic, so arrival order across topics is
 * arbitrary, and on the cluster the demo's chains came out as
 * {@code enriched <- fulfilled <- placed} and every other permutation. The
 * parent must be the previous version in <em>event time</em>.
 */
class StateProvenancePTFTest {

    private static final long T0 = 1_790_543_203_423L;

    /** One entity_log row: (entity_key, source_name, content, op, event_ms). */
    private static Row log(String entity, String source, long eventMs) {
        return Row.of(entity, source, ("payload-" + entity).getBytes(StandardCharsets.UTF_8), "UPSERT", eventMs);
    }

    private static List<Row> run(List<Row> arrivals) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // One reader, so the list order IS the arrival order the function sees.
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);
        t.getConfig().set("table.local-time-zone", "UTC");

        RowTypeInfo type = new RowTypeInfo(
                new TypeInformation<?>[] {
                        Types.STRING, Types.STRING, Types.PRIMITIVE_ARRAY(Types.BYTE), Types.STRING, Types.LONG},
                new String[] {"entity_key", "source_name", "content", "op", "event_ms"});
        // The same 5s bounded out-of-orderness the Kafka source tables declare.
        DataStream<Row> stream = env.fromData(type, arrivals.toArray(new Row[0]))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((r, ts) -> r.<Long>getFieldAs("event_ms")));
        t.createTemporaryView("entity_log", t.fromDataStream(stream, Schema.newBuilder()
                .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                .watermark("event_time", "SOURCE_WATERMARK()")
                .build()));
        t.createTemporarySystemFunction("STATE_PROVENANCE", StateProvenancePTF.class);

        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = t.executeSql(
                "SELECT version_id, entity_key, source_name, op, parents, emitted_at "
                        + "FROM TABLE(STATE_PROVENANCE("
                        + "input => TABLE entity_log PARTITION BY entity_key, "
                        + "on_time => DESCRIPTOR(event_time), uid => 'state-provenance-test'))")
                .collect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }

    /** Asserts one entity's versions form a single chain in the given source order. */
    private static void assertChain(List<Row> out, String entity, String... sourcesInEventOrder) {
        List<Row> mine = out.stream().filter(r -> entity.equals(r.getFieldAs("entity_key"))).toList();
        assertEquals(sourcesInEventOrder.length, mine.size(), entity + " version count");

        Map<String, Row> bySource = new HashMap<>();
        mine.forEach(r -> bySource.put(r.getFieldAs("source_name"), r));
        String previous = null;
        for (String source : sourcesInEventOrder) {
            Row v = bySource.get(source);
            String[] expected = previous == null ? new String[0] : new String[] {previous};
            assertArrayEquals(expected, v.<String[]>getFieldAs("parents"), entity + " parents of " + source);
            previous = v.getFieldAs("version_id");
        }
    }

    @Test
    void chainFollowsEventTimeNotArrivalOrder() throws Exception {
        // The arrival order the cluster actually produced for one trace:
        // enriched first, then fulfilled, then placed.
        List<Row> out = run(List.of(
                log("order-1", "orders.enriched", T0 + 177),
                log("order-1", "orders.fulfilled", T0 + 186),
                log("order-1", "orders.placed", T0)));

        assertChain(out, "order-1", "orders.placed", "orders.enriched", "orders.fulfilled");
        // Published oldest first, too — not just linked correctly.
        assertEquals(List.of(T0, T0 + 177, T0 + 186),
                out.stream().map(r -> r.<Long>getFieldAs("emitted_at")).toList());
    }

    @Test
    void redeliveredRecordIsOneVersion() throws Exception {
        // At-least-once: the same record twice, the second copy arriving while
        // the first is still waiting for its timer.
        List<Row> out = run(List.of(
                log("order-1", "orders.placed", T0),
                log("order-1", "orders.enriched", T0 + 177),
                log("order-1", "orders.placed", T0)));

        assertChain(out, "order-1", "orders.placed", "orders.enriched");
    }

    @Test
    void entitiesChainIndependently() throws Exception {
        List<Row> out = run(List.of(
                log("order-2", "orders.enriched", T0 + 1_100),
                log("order-1", "orders.fulfilled", T0 + 186),
                log("order-2", "orders.placed", T0 + 1_000),
                log("order-1", "orders.placed", T0),
                log("order-1", "orders.enriched", T0 + 177)));

        assertChain(out, "order-1", "orders.placed", "orders.enriched", "orders.fulfilled");
        assertChain(out, "order-2", "orders.placed", "orders.enriched");
    }
}
