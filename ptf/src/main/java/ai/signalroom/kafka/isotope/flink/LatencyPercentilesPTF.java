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
 * registered in SQL as {@code LATENCY_PERCENTILES}.
 *
 * <h2>Why a PTF</h2>
 *
 * Percentiles are a natural fit for an {@code AggregateFunction}, but CCAF
 * rejects every user-defined aggregate registration with "aggregate functions
 * are not supported". A {@link ProcessTableFunction} has no such restriction
 * (see {@code StuckTracePTF}), so expressing the percentile report as a PTF is
 * what makes it portable: it registers and runs identically on both Confluent
 * Platform Flink and Confluent Cloud for Apache Flink (CCAF). The trade-off is
 * that the PTF must do its own windowing (below) instead of leaning on the
 * {@code TABLE(TUMBLE(...))} TVF.
 *
 * <h2>Windowing</h2>
 *
 * The PTF folds the tumble in by hand:
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
 *   <li>Late rows (those whose window already fired) are dropped via the {@link
 *       Progress} high-water mark rather than re-opening the evicted window —
 *       which would otherwise re-register a now-in-the-past timer and emit a
 *       spurious second row. This mirrors {@code TUMBLE}'s drop-late-data
 *       behavior, so each (window, partition) is emitted at most once.</li>
 * </ul>
 *
 * <h2>Partitioning</h2>
 *
 * The call partitions the input by {@code (pipeline, origin_service,
 * this_topic)} — the aggregation grain. All three dimensions are therefore
 * constant within a partition; they are stashed on the accumulator so {@code
 * onTimer} (which has no input row) can emit them.
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
 *       not {@code CREATE VIEW} — see {@code 70_latency_percentiles_report.fql}.</li>
 * </ul>
 *
 * SQL invocation:
 * <pre>{@code
 *   INSERT INTO latency_percentiles_flat_1m
 *   SELECT window_start, window_end, pipeline, origin_service, this_topic,
 *          p50_ms, p95_ms, p99_ms, sample_count
 *   FROM TABLE(
 *       LATENCY_PERCENTILES(
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

    /**
     * Per-partition late-data guard. Tracks the {@code window_end} of the most
     * recently fired window for this key. Timers fire in ascending {@code
     * window_end} order as the watermark advances, so this value is monotonic:
     * any incoming row whose own {@code window_end} is at or below it belongs to
     * a window that already closed and must be dropped, not re-opened. Without
     * this, a late row would re-create the evicted accumulator, re-register a
     * now-in-the-past timer, and emit a spurious second row for that window.
     */
    public static class Progress {
        /** Largest fired {@code window_end} (epoch ms), or null before the first firing. */
        public Long lastFiredWindowEnd;
    }

    public void eval(
            Context ctx,
            @StateHint MapView<Long, WindowAccumulator> windows,
            @StateHint Progress progress,
            @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                Row input) throws Exception {

        // Read as Number, not Long: the `isotope` view derives latency_ms via
        // TIMESTAMPDIFF(MILLISECOND, ...), which Flink types as INT, so the field
        // arrives boxed as Integer — a raw getFieldAs(Long) would throw
        // ClassCastException.
        Number latency = input.getFieldAs("latency_ms");
        if (latency == null) return;

        Instant eventTime = ctx.timeContext(Instant.class).time();
        if (eventTime == null) return;

        long eventMs     = eventTime.toEpochMilli();
        long windowStart = Math.floorDiv(eventMs, WINDOW_MS) * WINDOW_MS;
        long windowEnd   = windowStart + WINDOW_MS;

        // Late-data guard: if this row's window already fired, drop it rather
        // than re-opening an evicted window (which would emit a spurious second
        // row). Mirrors TUMBLE's drop-late-data semantics.
        if (progress.lastFiredWindowEnd != null && windowEnd <= progress.lastFiredWindowEnd) {
            return;
        }

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

    public void onTimer(OnTimerContext ctx, MapView<Long, WindowAccumulator> windows, Progress progress)
            throws Exception {
        // The firing timer's timestamp IS the window_end we registered for.
        long windowEnd   = ctx.timeContext(Instant.class).time().toEpochMilli();
        long windowStart = windowEnd - WINDOW_MS;

        // Advance the late-data high-water mark even if the window is empty, so
        // the eval() guard knows this window has closed. Timers fire in order,
        // so windowEnd is monotonic, but max() keeps it defensive.
        if (progress.lastFiredWindowEnd == null || windowEnd > progress.lastFiredWindowEnd) {
            progress.lastFiredWindowEnd = windowEnd;
        }

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
