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
 * Detects traces that stop progressing before reaching the end of the pipeline.
 *
 * Behavior:
 *   - For each {@code trace_id} (partition key — supplied by SQL via the
 *     {@code PARTITION BY} clause on the input table argument), remembers
 *     the latest hop's event-time and producer/topic for triage. Latest in
 *     <em>event time</em>, not arrival: {@code isotope_raw} is a
 *     {@code UNION ALL} of one table per topic, so a trace's hops reach this
 *     operator in arbitrary order. A hop behind the latest one is ignored.
 *   - Registers a named event-time timer at
 *     {@code lastSeen + STALENESS}. Re-registering the same timer
 *     name on every fresh hop replaces the previous one — Flink handles
 *     the dedup for us.
 *   - A hop on {@link #TERMINAL_TOPIC} finishes the trace: its timer is
 *     cleared and every later row for it is ignored. Going quiet is what a
 *     finished trace does, so it must not read as stuck.
 *   - When the timer fires, emits one {@link Row} matching the
 *     {@link FunctionHint} output schema below.
 *
 * Why we emit {@link Row} rather than a POJO: Flink 2.1.2 / CCAF's sink-write
 * codegen generates a hard cast of the function output to {@code Row} in the
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
 *       input   => TABLE isotope PARTITION BY trace_id,
 *       on_time => DESCRIPTOR(event_time),
 *       uid     => 'stuck-trace-v1'
 *   );
 *
 * Note there is no {@code staleness_seconds} argument: the threshold is the
 * compile-time constant {@link #STALENESS} below, which also documents why it
 * cannot currently be a named scalar arg. Passing one here fails to plan.
 */
@FunctionHint(output = @DataTypeHint("ROW<"
    + "trace_id STRING, origin_service STRING, pipeline STRING, last_service STRING, "
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
     * The last topic of the demo's {@code orders.*} pipeline. A compile-time
     * constant for the same reason as {@link #STALENESS}: the framework cannot
     * currently take it as a named scalar arg. Update it if the pipeline grows
     * another stage, or a trace that stalls past this topic will go undetected.
     */
    private static final String TERMINAL_TOPIC = "orders.fulfilled";

    /**
     * Per-partition (per-trace) state. Mutated in place during
     * {@link #eval}; the framework persists it across invocations.
     */
    public static class TraceState {
        public String traceId;
        public String originService;
        public String pipeline;
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
        long eventMs = eventTime.toEpochMilli();
        String thisTopic = input.getFieldAs("this_topic");
        boolean finishes = TERMINAL_TOPIC.equals(thisTopic);

        // Finished is final: an earlier hop read after the terminal one, or a
        // retried hop, must not rearm a trace that already reached the end.
        if (TERMINAL_TOPIC.equals(state.lastTopic)) return;

        // Behind the latest hop, so not where the trace stopped, and following it
        // would pull the timer back. The terminal hop is exempt: it finishes the
        // trace even when a retried hop carries a later timestamp.
        if (!finishes && state.lastSeenTsMs != null && eventMs < state.lastSeenTsMs) return;

        state.traceId        = input.getFieldAs("trace_id");
        state.originService  = input.getFieldAs("origin_service");
        state.pipeline       = input.getFieldAs("pipeline");
        state.lastService    = input.getFieldAs("this_service");
        state.lastTopic      = thisTopic;
        state.lastHopCount   = input.getFieldAs("hop_count");
        state.lastSeenTsMs   = eventMs;

        if (finishes) {
            ctx.timeContext(Instant.class).clearTimer(TIMER_NAME);
            return;
        }

        Instant fireAt = eventTime.plus(STALENESS);
        state.firingAtMs = fireAt.toEpochMilli();

        // Re-registering the same-named timer replaces any prior one for
        // this partition key; the framework handles the dedup.
        ctx.timeContext(Instant.class).registerOnTime(TIMER_NAME, fireAt);
    }

    public void onTimer(TraceState state) {
        if (state == null || state.lastSeenTsMs == null) return;

        // eval() clears the timer on the terminal hop, but state restored from
        // before that existed can hold a finished trace with its timer still armed.
        if (TERMINAL_TOPIC.equals(state.lastTopic)) return;

        collect(Row.of(
            state.traceId,
            state.originService,
            state.pipeline,
            state.lastService,
            state.lastTopic,
            state.lastHopCount,
            state.lastSeenTsMs,
            state.firingAtMs - state.lastSeenTsMs
        ));
    }
}
