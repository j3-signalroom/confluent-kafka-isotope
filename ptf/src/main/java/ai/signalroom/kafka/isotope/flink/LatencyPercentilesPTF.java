/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import com.tdunning.math.stats.MergingDigest;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.time.Instant;

/**
 * Streaming p50/p95/p99 over {@code latency_ms} in 1-minute tumbling windows,
 * implemented as a {@link ProcessTableFunction} so it runs on BOTH Confluent
 * Platform Flink and Confluent Cloud for Apache Flink (CCAF).
 *
 * <h2>Why a PTF and not the UDAF</h2>
 *
 * {@link LatencyPercentilesUDAF} computes the same percentiles, but as an
 * {@code AggregateFunction} it is <b>CP-only</b> — CCAF rejects every UDAF
 * registration with "aggregate functions are not supported". PTFs have no such
 * restriction (see {@code StuckTracePTF}), so re-expressing the percentile
 * report as a PTF closes the "no portable percentiles" gap. The sketch math is
 * shared via {@link TDigests}, so this function is a true drop-in for the UDAF
 * report: same sink, same Avro subject, same numbers (within T-Digest error).
 *
 * <h2>Windowing</h2>
 *
 * The UDAF leans on {@code TABLE(TUMBLE(...))} for window assignment and
 * firing. A PTF folds the tumble in by hand:
 * <ul>
 *   <li>Each input row is bucketed into its 1-minute window by event time.</li>
 *   <li>Per-window T-Digest accumulators live in a {@link MapView} keyed by
 *       {@code window_start} (epoch ms), so out-of-order rows that straddle a
 *       boundary — within the source's watermark slack — land in the right
 *       window. A few windows can be "open" at once per key.</li>
 *   <li>A named event-time timer at {@code window_end} fires when the watermark
 *       passes it (i.e. after the source's 5s allowance), matching the firing
 *       semantics of the {@code TUMBLE} TVF. {@code onTimer} emits one row for
 *       that window and evicts it from the map.</li>
 * </ul>
 *
 * <h2>Partitioning</h2>
 *
 * The call partitions the input by {@code (pipeline, origin_service,
 * this_topic)} — the same grain as the UDAF report's {@code GROUP BY}. All
 * three dimensions are therefore constant within a partition; they are stashed
 * on the accumulator so {@code onTimer} (which has no input row) can emit them.
 *
 * <h2>Flink 2.1.2 PTF constraints (shared with StuckTracePTF)</h2>
 * <ul>
 *   <li>Output is declared as {@code ROW} (not a POJO): the sink-write codegen
 *       on the {@code INSERT INTO} path hard-casts function output to
 *       {@code Row}, and a POJO return fails that cast at Janino-compile time.</li>
 *   <li>The window length is a hard-coded constant, not a scalar arg: the 2.1.2
 *       PTF framework rejects user-declared scalar args alongside the implicit
 *       {@code on_time} / {@code uid} args. Make it configurable when that
 *       limitation lifts.</li>
 *   <li>The call MUST live in {@code INSERT INTO ... SELECT ... FROM TABLE(ptf(...))},
 *       not {@code CREATE VIEW} — see {@code 71_latency_percentiles_ptf_report.fql}.</li>
 * </ul>
 *
 * SQL invocation:
 * <pre>{@code
 *   INSERT INTO latency_percentiles_flat_1m
 *   SELECT window_start, window_end, pipeline, origin_service, this_topic,
 *          p50_ms, p95_ms, p99_ms, sample_count
 *   FROM TABLE(
 *       LATENCY_PERCENTILES_PTF(
 *           input   => TABLE isotope PARTITION BY (pipeline, origin_service, this_topic),
 *           on_time => DESCRIPTOR(event_time),
 *           uid     => 'latency-pcts-v1'));
 * }</pre>
 */
@FunctionHint(output = @DataTypeHint("ROW<"
    + "window_start BIGINT, window_end BIGINT, pipeline STRING, origin_service STRING, "
    + "this_topic STRING, p50_ms DOUBLE, p95_ms DOUBLE, p99_ms DOUBLE, sample_count BIGINT>"))
public class LatencyPercentilesPTF extends ProcessTableFunction<Row> {

    /** Tumbling-window length. Hard-coded — see the 2.1.2 scalar-arg note in the class Javadoc. */
    private static final long WINDOW_MS = 60_000L;

    private static final String TIMER_PREFIX = "w:";

    /**
     * Per-window accumulator stored in the {@link MapView}, keyed by
     * {@code window_start}. Holds the serialized T-Digest plus the dimension
     * values, which are constant per partition but must be re-emitted from
     * {@code onTimer} where no input row is available.
     */
    public static class WindowAccumulator {
        /** Serialized {@link MergingDigest} (via {@link TDigests#save}). Null = empty. */
        public byte[] digestBytes;
        public long sampleCount = 0L;
        public String pipeline;
        public String originService;
        public String thisTopic;
    }

    public void eval(
            Context ctx,
            @StateHint MapView<Long, WindowAccumulator> windows,
            @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                Row input) throws Exception {

        // Read as Number, not Long: the `isotope` view derives latency_ms via
        // TIMESTAMPDIFF(MILLISECOND, ...), which Flink types as INT, so the field
        // arrives boxed as Integer. (The UDAF path dodges this because Flink
        // coerces INT->BIGINT when matching the accumulate(..., Long) signature;
        // a raw getFieldAs(Long) here would throw ClassCastException.)
        Number latency = input.getFieldAs("latency_ms");
        if (latency == null) return;

        Instant eventTime = ctx.timeContext(Instant.class).time();
        if (eventTime == null) return;

        long eventMs     = eventTime.toEpochMilli();
        long windowStart = Math.floorDiv(eventMs, WINDOW_MS) * WINDOW_MS;
        long windowEnd   = windowStart + WINDOW_MS;

        WindowAccumulator acc = windows.get(windowStart);
        if (acc == null) {
            acc = new WindowAccumulator();
            acc.pipeline      = input.getFieldAs("pipeline");
            acc.originService = input.getFieldAs("origin_service");
            acc.thisTopic     = input.getFieldAs("this_topic");
        }

        MergingDigest d = TDigests.load(acc.digestBytes);
        d.add(latency.doubleValue());
        acc.digestBytes = TDigests.save(d);
        acc.sampleCount++;
        windows.put(windowStart, acc);

        // Re-registering the same-named timer replaces the prior one — the
        // window's close time is fixed, so this is idempotent across its rows.
        ctx.timeContext(Instant.class)
           .registerOnTime(TIMER_PREFIX + windowEnd, Instant.ofEpochMilli(windowEnd));
    }

    public void onTimer(OnTimerContext ctx, MapView<Long, WindowAccumulator> windows) throws Exception {
        // The firing timer's timestamp IS the window_end we registered for.
        long windowEnd   = ctx.timeContext(Instant.class).time().toEpochMilli();
        long windowStart = windowEnd - WINDOW_MS;

        WindowAccumulator acc = windows.get(windowStart);
        if (acc == null || acc.sampleCount == 0L) return;

        MergingDigest d = TDigests.load(acc.digestBytes);
        collect(Row.of(
            windowStart,
            windowEnd,
            acc.pipeline,
            acc.originService,
            acc.thisTopic,
            d.quantile(0.50),
            d.quantile(0.95),
            d.quantile(0.99),
            acc.sampleCount
        ));

        // Window emitted — evict it so per-key state stays bounded.
        windows.remove(windowStart);
    }
}
