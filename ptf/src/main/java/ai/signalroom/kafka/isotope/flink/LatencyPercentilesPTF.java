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
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

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
 *   <li>Per-window T-Digest accumulators live in a single value-state {@link
 *       State} object — a plain {@code Map<window_start, packed-accumulator>}
 *       field — so out-of-order rows that straddle a boundary (within the
 *       source's watermark slack) land in the right window. A few windows can
 *       be "open" at once per key. CCAF's PTF runtime rejects {@code MapView}/
 *       {@code ListView} state types, and the documented replacement is a
 *       plain {@code Map}/{@code List} field in the {@code @StateHint} POJO
 *       (Flink serializes it in full on each access). The map <b>value</b> is a
 *       Base64 {@code String}, not {@code byte[]}: an earlier {@code Map<Long,
 *       byte[]>} was rejected at {@code CREATE FUNCTION} with the {@code
 *       MapView}/{@code ListView} error even though the field was a plain
 *       {@code Map}, so the {@code BYTES} value type is the Open Preview
 *       trigger — every supported example in Confluent's PTF docs uses a scalar
 *       map value. Encoding each packed accumulator (sample count + T-Digest)
 *       as Base64 keeps the value scalar; a handful of open windows per key
 *       keeps the full-map (de)serialization cheap.</li>
 *   <li>A named event-time timer at {@code window_end} fires when the watermark
 *       passes it (i.e. after the source's 5s allowance), matching the firing
 *       semantics of the {@code TUMBLE} TVF. {@code onTimer} emits one row for
 *       that window and evicts it from the map.</li>
 *   <li>Late rows (those whose window already fired) are dropped via the {@code
 *       State.lastFiredWindowEnd} high-water mark rather than re-opening the
 *       evicted window — which would otherwise re-register a now-in-the-past
 *       timer and emit a spurious second row. This mirrors {@code TUMBLE}'s
 *       drop-late-data behavior, so each (window, partition) is emitted at most
 *       once.</li>
 * </ul>
 *
 * <h2>Partitioning</h2>
 *
 * The call partitions the input by {@code (pipeline, origin_service,
 * this_topic)} — the aggregation grain. All three dimensions are therefore
 * constant within a partition; they are stashed in {@link State} (set once) so
 * {@code onTimer} (which has no input row) can emit them.
 *
 * <h2>Runtime constraints</h2>
 * <ul>
 *   <li>Output is declared as {@code ROW} (not a POJO): the sink-write codegen
 *       on the {@code INSERT INTO} path hard-casts function output to
 *       {@code Row}, and a POJO return fails that cast at Janino-compile time.</li>
 *   <li>State follows CCAF's PTF rules: a plain {@code Map} field (no {@code
 *       MapView}/{@code ListView}, which CCAF rejects), public fields with
 *       default values, and scalar map values only — Base64 {@code String},
 *       not {@code byte[]} (see the windowing note for why a {@code BYTES}
 *       value type was rejected).</li>
 *   <li>The window length is a hard-coded constant, not a scalar arg: the
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
 *           uid     => 'latency-pcts-v2'));
 * }</pre>
 */
@FunctionHint(output = @DataTypeHint("ROW<"
    + "window_start BIGINT, window_end BIGINT, pipeline STRING, origin_service STRING, "
    + "this_topic STRING, p50_ms DOUBLE, p95_ms DOUBLE, p99_ms DOUBLE, sample_count BIGINT>"))
public class LatencyPercentilesPTF extends ProcessTableFunction<Row> {

    /** Tumbling-window length. Hard-coded — see the scalar-arg note in the class Javadoc. */
    private static final long WINDOW_MS = 60_000L;

    private static final String TIMER_PREFIX = "w:";

    /**
     * Per-partition state. A single value-state object (plain structured type,
     * fully (de)serialized on each call):
     * <ul>
     *   <li>{@code windows} — open windows, keyed by {@code window_start}
     *       (epoch ms). Each value is the Base64 encoding of the packed
     *       accumulator: an 8-byte sample count followed by the serialized
     *       T-Digest (see {@link #pack}). A plain {@code Map<Long, String>}
     *       field — the documented CCAF replacement for {@code MapView} — with
     *       a scalar {@code String} value rather than {@code byte[]} (the
     *       {@code BYTES} map value was the rejected case). Defaulted to an
     *       empty map: CCAF requires public state fields to have defaults.</li>
     *   <li>{@code pipeline}/{@code originService}/{@code thisTopic} — the
     *       partition-constant dimensions, captured once for {@code onTimer} to
     *       re-emit.</li>
     *   <li>{@code lastFiredWindowEnd} — late-data high-water mark; timers fire
     *       in ascending {@code window_end} order, so this is monotonic and a
     *       row at or below it belongs to an already-closed window.</li>
     * </ul>
     */
    public static class State {
        public Map<Long, String> windows = new HashMap<>();
        public String pipeline;
        public String originService;
        public String thisTopic;
        public Long lastFiredWindowEnd;
    }

    public void eval(
            Context ctx,
            @StateHint State state,
            @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                Row input) {

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
        if (state.lastFiredWindowEnd != null && windowEnd <= state.lastFiredWindowEnd) {
            return;
        }

        if (state.pipeline == null) {
            state.pipeline      = input.getFieldAs("pipeline");
            state.originService = input.getFieldAs("origin_service");
            state.thisTopic     = input.getFieldAs("this_topic");
        }

        byte[] packed   = decode(state.windows.get(windowStart));
        long   count    = packed == null ? 0L : unpackCount(packed);
        MergingDigest d = TDigests.load(packed == null ? null : unpackDigest(packed));
        d.add(latency.doubleValue());
        state.windows.put(windowStart, encode(pack(count + 1, TDigests.save(d))));

        // Re-registering the same-named timer replaces the prior one — the
        // window's close time is fixed, so this is idempotent across its rows.
        ctx.timeContext(Instant.class)
           .registerOnTime(TIMER_PREFIX + windowEnd, Instant.ofEpochMilli(windowEnd));
    }

    public void onTimer(OnTimerContext ctx, State state) {
        // The firing timer's timestamp IS the window_end we registered for.
        long windowEnd   = ctx.timeContext(Instant.class).time().toEpochMilli();
        long windowStart = windowEnd - WINDOW_MS;

        // Advance the late-data high-water mark even if the window is empty, so
        // the eval() guard knows this window has closed. Timers fire in order,
        // so windowEnd is monotonic, but max() keeps it defensive.
        if (state.lastFiredWindowEnd == null || windowEnd > state.lastFiredWindowEnd) {
            state.lastFiredWindowEnd = windowEnd;
        }

        if (state.windows == null) return;
        String packedStr = state.windows.remove(windowStart);
        if (packedStr == null) return;

        byte[] packed = decode(packedStr);
        long count = unpackCount(packed);
        if (count == 0L) return;

        MergingDigest d = TDigests.load(unpackDigest(packed));
        collect(Row.of(
            windowStart,
            windowEnd,
            state.pipeline,
            state.originService,
            state.thisTopic,
            d.quantile(0.50),
            d.quantile(0.95),
            d.quantile(0.99),
            count
        ));
    }

    // -- map value <-> Base64 String: a scalar map-value type, the supported
    //    CCAF shape (a byte[] map value is rejected with the MapView error). --

    private static String encode(byte[] packed) {
        return Base64.getEncoder().encodeToString(packed);
    }

    private static byte[] decode(String encoded) {
        return encoded == null ? null : Base64.getDecoder().decode(encoded);
    }

    // -- packed accumulator: 8-byte sample count (big-endian) + T-Digest bytes --

    private static byte[] pack(long count, byte[] digest) {
        return ByteBuffer.allocate(Long.BYTES + digest.length).putLong(count).put(digest).array();
    }

    private static long unpackCount(byte[] packed) {
        return ByteBuffer.wrap(packed).getLong();
    }

    private static byte[] unpackDigest(byte[] packed) {
        byte[] digest = new byte[packed.length - Long.BYTES];
        System.arraycopy(packed, Long.BYTES, digest, 0, digest.length);
        return digest;
    }
}
