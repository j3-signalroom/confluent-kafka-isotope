package com.life360.kafka.isotope.flink;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * Detects traces that stop progressing.
 *
 * Behavior:
 *   - For each {@code trace_id} (partition key — supplied by SQL via the
 *     {@code PARTITION BY} clause on the input table argument), remembers
 *     the most-recent hop's event-time and producer/topic for triage.
 *   - Schedules an event-time timer named {@code "stuck"} at
 *     {@code lastSeen + stalenessSeconds}. Re-registers on every fresh hop.
 *   - When the timer fires, emits one {@link StuckTraceAlert} and clears
 *     state — at-most-once per stuck episode. The trace can re-arm if it
 *     resumes later.
 *
 * Stale-fire defense: when the application replaces the timer on a fresh
 * hop, the prior timer may still be in Flink's queue. We persist the
 * currently-active timer's fire-time in state and discard any onTimer
 * invocation whose fire-time doesn't match.
 *
 * Why event-time timers (not processing-time): a wall-clock timer would
 * fire spuriously during catchup / replay even when source data is dense.
 * Event-time timers fire at a watermark boundary and so survive
 * checkpoint, replay, and back-pressure correctly.
 *
 * Usage in SQL:
 *   SELECT * FROM STUCK_TRACE_PTF(
 *       r => TABLE isotope PARTITION BY trace_id,
 *       staleness_seconds => 60);
 */
@FunctionHint(output = @DataTypeHint("ROW<"
    + "trace_id STRING, origin_service STRING, last_service STRING, "
    + "last_topic STRING, last_hop_count INT, last_seen_ts_ms BIGINT, "
    + "stuck_for_ms BIGINT>"))
public class StuckTracePTF extends ProcessTableFunction<StuckTraceAlert> {

    /** Name used for both the state slot and the event-time timer. */
    private static final String STATE_NAME = "trace";
    private static final String TIMER_NAME = "stuck";

    /**
     * Per-partition (per-trace) state. Mutated in place during {@code
     * eval}; the framework persists it across invocations.
     *
     * Public fields are required — Flink's reflection-based POJO type
     * inference treats public mutable fields as the persisted shape.
     */
    public static class TraceState {
        public String traceId;
        public String originService;
        public String lastService;
        public String lastTopic;
        public Integer lastHopCount;
        public Long lastSeenTsMs;
        /** Fire-time of the currently-armed timer; used to discard stale fires. */
        public Long activeTimerTsMs;
    }

    /**
     * Invoked once per arriving row from the input table (after
     * partitioning by {@code trace_id}).
     */
    public void eval(
            Context ctx,
            @StateHint(name = STATE_NAME) TraceState state,
            @ArgumentHint(
                value = {ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME},
                name  = "r")
                Row input,
            @ArgumentHint(name = "staleness_seconds") Long stalenessSeconds) {

        // Current row's event time, in epoch millis. REQUIRE_ON_TIME on the
        // table arg guarantees the framework binds the source table's
        // WATERMARK column here.
        Long eventTsMs = ctx.timeContext(Long.class).time();

        state.traceId        = input.getFieldAs("trace_id");
        state.originService  = input.getFieldAs("origin_service");
        state.lastService    = input.getFieldAs("this_service");
        state.lastTopic      = input.getFieldAs("this_topic");
        state.lastHopCount   = input.getFieldAs("hop_count");
        state.lastSeenTsMs   = eventTsMs;

        long fireAtMs = eventTsMs + stalenessSeconds * 1000L;
        state.activeTimerTsMs = fireAtMs;

        // Re-registering the same-named timer supersedes the previous one.
        // (Even if the prior timer remains queued, onTimer filters by
        // state.activeTimerTsMs.)
        ctx.timeContext(Long.class).registerOnTime(TIMER_NAME, fireAtMs);
    }

    /**
     * Invoked when a registered timer fires. Emits an alert if the firing
     * timer is the currently-armed one (not a stale leftover from a hop
     * that has since been replaced).
     */
    public void onTimer(
            OnTimerContext ctx,
            @StateHint(name = STATE_NAME) TraceState state) {

        if (!TIMER_NAME.equals(ctx.currentTimer())) return;
        if (state == null || state.lastSeenTsMs == null) return;

        Long firingAtMs = ctx.timeContext(Long.class).time();
        if (state.activeTimerTsMs == null || !state.activeTimerTsMs.equals(firingAtMs)) {
            // A stale fire: a fresher hop has already moved the active timer.
            return;
        }

        collect(new StuckTraceAlert(
            state.traceId,
            state.originService,
            state.lastService,
            state.lastTopic,
            state.lastHopCount,
            state.lastSeenTsMs,
            firingAtMs - state.lastSeenTsMs
        ));

        // Clear so the trace can re-arm if it resumes later.
        ctx.clearState(STATE_NAME);
    }
}
