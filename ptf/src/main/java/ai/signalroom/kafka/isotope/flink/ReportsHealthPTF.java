package ai.signalroom.kafka.isotope.flink;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * Detects that the reports job has stopped publishing while traffic keeps flowing.
 *
 * <p>The input interleaves two kinds of row: {@value #TRAFFIC} is a record on the
 * pipeline's first topic, and {@value #REPORT} is a row the reports job published,
 * carrying its {@code window_end}. Once traffic has moved past a one-minute window
 * by more than the source tables' watermark allowance, the reports job has what it
 * needs to close that window. If no report for that window or a later one appears
 * within {@link #GRACE}, this emits {@value #STALLED}, and then {@value #RECOVERED}
 * once one does. One episode produces one pair, however long it lasts.
 *
 * <p>This catches a dead reports job and a frozen reports watermark alike, because
 * both stop windows from closing. It cannot catch either one when traffic has
 * stopped too: nothing is overdue, and this function's own event time needs a record
 * to advance. Telling a dead job from a quiet pipeline at that point takes a
 * wall-clock check outside Flink.
 *
 * <p>Rows of both kinds must carry Kafka write time as their event time. Report rows
 * do, because the sink leaves the record timestamp to the producer, and that is what
 * makes them comparable with traffic.
 */
@FunctionHint(output = @DataTypeHint("ROW<"
    + "status STRING, overdue_window_end BIGINT, last_reported_window_end BIGINT, "
    + "last_traffic_ts_ms BIGINT, event_ts_ms BIGINT, stalled_for_ms BIGINT>"))
public class ReportsHealthPTF extends ProcessTableFunction<Row> {

    static final String TRAFFIC = "traffic";
    static final String REPORT = "report";
    static final String STALLED = "STALLED";
    static final String RECOVERED = "RECOVERED";

    private static final String TIMER_NAME = "overdue";

    /** The tumbling window of {@code 20_topology_report.fql}, the report being watched. */
    private static final long WINDOW_MS = Duration.ofMinutes(1).toMillis();

    /** The watermark allowance the reports' source tables declare. */
    private static final long WATERMARK_ALLOWANCE_MS = Duration.ofSeconds(5).toMillis();

    /**
     * How long an overdue window may stay unreported before alerting. Compile-time
     * for the same reason as {@code StuckTracePTF.STALENESS}. It has to outlast a
     * quiet topic holding the reports' watermark back for
     * {@link IsotopeReportsJob#SOURCE_IDLE_TIMEOUT}, plus a task restart, so it would
     * not read as an outage.
     */
    private static final long GRACE_MS = Duration.ofMinutes(2).toMillis();

    /** The job's state. A single partition, since it describes the job and not a trace. */
    public static class HealthState {
        /** End of the window holding the newest traffic seen. */
        public Long trafficWindowEnd;
        /** End of the window with traffic before that one. */
        public Long previousTrafficWindowEnd;
        public Long lastTrafficMs;
        public Long reportedWindowEnd;
        /** The window being waited on, or null when nothing is overdue. */
        public Long overdueWindowEnd;
        public Long firingAtMs;
        /** When the current episode alerted, or null when there is no open alert. */
        public Long stalledAtMs;
    }

    public void eval(
            Context ctx,
            @StateHint HealthState state,
            @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                Row input) {

        Instant eventTime = ctx.timeContext(Instant.class).time();
        long eventMs = eventTime.toEpochMilli();

        if (REPORT.equals(input.getFieldAs("kind"))) {
            onReport(ctx, state, input.getFieldAs("window_end"), eventMs);
        } else {
            onTraffic(ctx, state, eventTime, eventMs);
        }
    }

    private void onReport(Context ctx, HealthState state, Long windowEnd, long eventMs) {
        if (windowEnd == null) return;
        if (state.reportedWindowEnd == null || windowEnd > state.reportedWindowEnd) {
            state.reportedWindowEnd = windowEnd;
        }
        if (state.overdueWindowEnd == null || state.reportedWindowEnd < state.overdueWindowEnd) return;

        ctx.timeContext(Instant.class).clearTimer(TIMER_NAME);
        if (state.stalledAtMs != null) {
            collect(alert(RECOVERED, state, eventMs, eventMs - state.stalledAtMs));
        }
        state.overdueWindowEnd = null;
        state.stalledAtMs = null;
    }

    private void onTraffic(Context ctx, HealthState state, Instant eventTime, long eventMs) {
        if (state.lastTrafficMs == null || eventMs > state.lastTrafficMs) {
            state.lastTrafficMs = eventMs;
        }

        // Only one window is waited on at a time. Waiting on a newer one as well
        // would mean pushing the timer out on every minute of traffic, and it would
        // never fire. The cost: a window that closes while another is pending goes
        // unwatched, so a stall can take one window longer to notice.
        Long closed = newestClosedWindow(state, eventMs);
        boolean reported = closed != null && state.reportedWindowEnd != null
                && state.reportedWindowEnd >= closed;
        if (state.overdueWindowEnd == null && closed != null && !reported) {
            state.overdueWindowEnd = closed;
            state.firingAtMs = eventMs + GRACE_MS;
            ctx.timeContext(Instant.class).registerOnTime(TIMER_NAME, eventTime.plusMillis(GRACE_MS));
        }

        long windowEnd = eventMs - Math.floorMod(eventMs, WINDOW_MS) + WINDOW_MS;
        if (state.trafficWindowEnd == null || windowEnd > state.trafficWindowEnd) {
            state.previousTrafficWindowEnd = state.trafficWindowEnd;
            state.trafficWindowEnd = windowEnd;
        }
    }

    /**
     * The newest window with traffic that the watermark has passed by {@code eventMs}.
     * The previous window has to be checked too: a record in the first seconds of a
     * new window makes that window the newest before the old one has closed.
     */
    private static Long newestClosedWindow(HealthState state, long eventMs) {
        if (state.trafficWindowEnd != null && state.trafficWindowEnd + WATERMARK_ALLOWANCE_MS <= eventMs) {
            return state.trafficWindowEnd;
        }
        if (state.previousTrafficWindowEnd != null
                && state.previousTrafficWindowEnd + WATERMARK_ALLOWANCE_MS <= eventMs) {
            return state.previousTrafficWindowEnd;
        }
        return null;
    }

    public void onTimer(HealthState state) {
        if (state == null || state.overdueWindowEnd == null) return;
        state.stalledAtMs = state.firingAtMs;
        collect(alert(STALLED, state, state.firingAtMs, GRACE_MS));
    }

    private static Row alert(String status, HealthState state, long eventMs, long stalledForMs) {
        return Row.of(
            status,
            state.overdueWindowEnd,
            state.reportedWindowEnd,
            state.lastTrafficMs,
            eventMs,
            stalledForMs);
    }
}
