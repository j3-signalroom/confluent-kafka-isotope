/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

/**
 * Stamps a merged (fan-in) record with a fresh, deterministically derived trace
 * — the collector-side counterpart to {@link IsotopeAppendHop} for statements
 * that are <em>not</em> 1:1.
 *
 * <h2>When to use this instead of ISOTOPE_APPEND_HOP</h2>
 * {@code ISOTOPE_APPEND_HOP} continues an inbound trace, which is only truthful
 * when one input produced one output. A windowed aggregate has many parents, so
 * it mints a new identity here and records the many-to-one edges separately on
 * {@code isotope_merge_edge_markers}. Choosing the wrong one is not a style
 * question: forwarding an input's trace ID through an aggregate fabricates
 * provenance. See {@code docs/flink-collector.md} section 3.1.
 *
 * <h2>SQL usage</h2>
 * The target table's {@code headers} column must be persisted (writable) and
 * typed {@code MAP<STRING, STRING>}, exactly as for the 1:1 collector:
 *
 * <pre>
 *   INSERT INTO `orders.flink_batched`
 *   SELECT
 *       `pipeline`,
 *       UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000,
 *       UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000,
 *       COUNT(*),
 *       COUNT(DISTINCT `trace_id`),
 *       ISOTOPE_MERGE_TRACE(
 *           `pipeline`,
 *           'orders-batch',                                             -- operator
 *           UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000,
 *           UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000,
 *           `pipeline`,                                                 -- group key
 *           'flink-batch',                                              -- this_service
 *           'orders.flink_batched')                                     -- this_topic
 *   FROM TABLE(TUMBLE(TABLE `isotope`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE))
 *   GROUP BY `window_start`, `window_end`, `pipeline`;
 * </pre>
 *
 * The first five arguments must match those passed to
 * {@link IsotopeMergeTraceId} by the statement writing the edge rows — that
 * agreement is what joins a merged record to its parents.
 *
 * <h2>Determinism</h2>
 * Genuinely deterministic, unlike a naive merge collector: no clock read and no
 * random draw. Both the trace ID and the hop timestamp are functions of the
 * window. {@link MergeTrace} explains why that is required rather than merely
 * tidy.
 *
 * @see MergeTrace the derivation, and why the ID cannot be minted
 */
public class IsotopeMergeTrace extends ScalarFunction {

    /**
     * @param pipeline      pipeline name carried onto the merged record.
     * @param operator      logical name of this merging stage.
     * @param windowStartMs window start, epoch millis.
     * @param windowEndMs   window end, epoch millis.
     * @param groupKey      the grouping key beyond the window.
     * @param service       logical service name for this Flink statement.
     * @param topic         topic the merged record is produced to.
     * @return the complete header map for the merged record.
     */
    public @DataTypeHint("MAP<STRING, STRING>") Map<String, String> eval(
            String pipeline,
            String operator,
            Long windowStartMs,
            Long windowEndMs,
            String groupKey,
            String service,
            String topic) {

        return MergeTrace.mergeHeaders(
                pipeline,
                operator,
                windowStartMs == null ? 0L : windowStartMs,
                windowEndMs   == null ? 0L : windowEndMs,
                groupKey,
                service,
                topic);
    }
}
