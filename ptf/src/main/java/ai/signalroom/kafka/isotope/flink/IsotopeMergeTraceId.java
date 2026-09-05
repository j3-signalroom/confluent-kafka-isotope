/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Returns the merge trace ID for a window as a hex string, so the statement
 * writing merge-edge rows can label each contributing trace with the identity
 * of the merged record it fed.
 *
 * <h2>Why this exists as a separate function</h2>
 * The merged record and its edge rows cannot come from one statement: one
 * aggregates the window, the other projects every row in it. Emitting both from
 * a single {@code ProcessTableFunction} and splitting the output by a tag column
 * would need {@code CREATE VIEW} over a PTF, which the cp-flink 2.1.2 Expander
 * round-trip rejects (see {@code 60_stuck_trace_report.fql}). Two statements it
 * is — and the only thing holding them together is that both derive the same ID
 * from the same window.
 *
 * <p>Pass exactly the five arguments {@link IsotopeMergeTrace} receives first.
 * If the two drift, the edges silently stop joining to the records they
 * describe; {@code MergeTraceTest} pins the agreement.
 *
 * <h2>SQL usage</h2>
 * <pre>
 *   INSERT INTO `isotope_merge_edge_markers`
 *   SELECT
 *       ISOTOPE_MERGE_TRACE_ID(
 *           `pipeline`,
 *           'orders-batch',
 *           UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000,
 *           UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000,
 *           `pipeline`),
 *       ...,
 *       `trace_id`                                   -- the contributing parent
 *   FROM TABLE(TUMBLE(TABLE `isotope`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE));
 * </pre>
 *
 * No {@code GROUP BY} — the window table function already stamps every row with
 * its window bounds, which is exactly the fan-out an edge list wants.
 *
 * @see MergeTrace the derivation, shared with {@link IsotopeMergeTrace}
 */
public class IsotopeMergeTraceId extends ScalarFunction {

    /**
     * @param pipeline      pipeline name, as passed to {@link IsotopeMergeTrace}.
     * @param operator      logical name of the merging stage.
     * @param windowStartMs window start, epoch millis.
     * @param windowEndMs   window end, epoch millis.
     * @param groupKey      the grouping key beyond the window.
     * @return the merged record's trace ID in hex, matching its
     *         {@code x-isotope-trace-id} header exactly.
     */
    public String eval(String pipeline,
                       String operator,
                       Long windowStartMs,
                       Long windowEndMs,
                       String groupKey) {

        return MergeTrace.mergeTraceIdHex(
                pipeline,
                operator,
                windowStartMs == null ? 0L : windowStartMs,
                windowEndMs   == null ? 0L : windowEndMs,
                groupKey);
    }
}
