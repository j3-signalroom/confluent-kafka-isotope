/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;

/**
 * Detects traces that stop progressing.
 *
 * Behavior:
 *   - For each {@code trace_id} (partition key — supplied by SQL via the
 *     {@code PARTITION BY} clause on the input table argument), remembers
 *     the most-recent hop's event-time and producer/topic for triage.
 *   - Registers a named event-time timer at
 *     {@code lastSeen + stalenessSeconds}. Re-registering the same timer
 *     name on every fresh hop replaces the previous one — Flink handles
 *     the dedup for us.
 *   - When the timer fires, emits one {@link Row} matching the
 *     {@link FunctionHint} output schema below.
 *
 * Why we emit {@link Row} rather than a POJO: same reason as
 * {@code LatencyPercentilesUDAF}. Flink 2.1.2 / CCAF's sink-write codegen
 * generates a hard cast of the function output to {@code Row} in the
 * {@code INSERT INTO} path; if the function is parameterised on a POJO
 * the cast fails at runtime with
 * "class StuckTraceAlert cannot be cast to class org.apache.flink.types.Row".
 * Declaring the return type as {@code Row} keeps the codegen happy on
 * both the interactive-SELECT path and the sink-INSERT path.
 *
 * SQL invocation pattern follows Confluent's documented PTF call syntax —
 * all args named, table arg as bare {@code TABLE x PARTITION BY col} (no
 * backticks, no parens), plus the implicit {@code on_time} and {@code uid}
 * args:
 *
 *   SELECT * FROM STUCK_TRACE_PTF(
 *       input             => TABLE isotope PARTITION BY trace_id,
 *       staleness_seconds => CAST(60 AS BIGINT),
 *       on_time           => DESCRIPTOR(event_time),
 *       uid               => 'stuck-trace-v1'
 *   );
 */
@FunctionHint(output = @DataTypeHint("ROW<"
    + "trace_id STRING, origin_service STRING, last_service STRING, "
    + "last_topic STRING, last_hop_count INT, last_seen_ts_ms BIGINT, "
    + "stuck_for_ms BIGINT>"))
public class StuckTracePTF extends ProcessTableFunction<Row> {

    private static final String TIMER_NAME = "stuck";

    /**
     * Staleness threshold — fixed at 60s for the demo. Flink 2.1.2's PTF
     * framework rejects user-declared scalar args alongside the implicit
     * {@code on_time} / {@code uid} args (Calcite "DEFAULT only allowed
     * for optional parameters"); to keep the call site matching the
     * canonical InactivityAlert example from the Confluent docs we
     * hard-code the threshold here. Make it configurable via job config
     * or a separate args table if the demo grows.
     */
    private static final Duration STALENESS = Duration.ofSeconds(60);

    /**
     * Per-partition (per-trace) state. Mutated in place during
     * {@link #eval}; the framework persists it across invocations.
     */
    public static class TraceState {
        public String traceId;
        public String originService;
        public String lastService;
        public String lastTopic;
        public Integer lastHopCount;
        public Long lastSeenTsMs;
        /** Stored so {@link #onTimer} can compute the alert's stuck_for_ms duration. */
        public Long firingAtMs;
    }

    public void eval(
            Context ctx,
            @StateHint TraceState state,
            @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                Row input) {

        Instant eventTime = ctx.timeContext(Instant.class).time();

        state.traceId        = input.getFieldAs("trace_id");
        state.originService  = input.getFieldAs("origin_service");
        state.lastService    = input.getFieldAs("this_service");
        state.lastTopic      = input.getFieldAs("this_topic");
        state.lastHopCount   = input.getFieldAs("hop_count");
        state.lastSeenTsMs   = eventTime.toEpochMilli();

        Instant fireAt = eventTime.plus(STALENESS);
        state.firingAtMs = fireAt.toEpochMilli();

        // Re-registering the same-named timer replaces any prior one for
        // this partition key; the framework handles the dedup.
        ctx.timeContext(Instant.class).registerOnTime(TIMER_NAME, fireAt);
    }

    public void onTimer(TraceState state) {
        if (state == null || state.lastSeenTsMs == null) return;

        collect(Row.of(
            state.traceId,
            state.originService,
            state.lastService,
            state.lastTopic,
            state.lastHopCount,
            state.lastSeenTsMs,
            state.firingAtMs - state.lastSeenTsMs
        ));
    }
}
