/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import ai.signalroom.kafka.isotope.Isotope;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The merge-provenance contract, pinned.
 *
 * <p>Every assertion here guards the same failure: the merged record and its
 * merge-edge rows are produced by two separate Flink statements, so if their
 * derived IDs ever disagree, the edges stop joining to the record they describe
 * and nothing at runtime says so. These tests are what make that a build
 * failure instead of a silent data-quality hole.
 */
class MergeTraceTest {

    private static final String PIPELINE = "orders";
    private static final String OPERATOR = "orders-batch";
    private static final long   WIN_START = 1_700_000_000_000L;
    private static final long   WIN_END   = 1_700_000_060_000L;
    private static final String GROUP_KEY = "orders";
    private static final String SERVICE   = "flink-batch";
    private static final String TOPIC     = "orders.flink_batched";

    @Test
    @DisplayName("same window derives the same ID every time (replay-stable)")
    void deterministic() {
        String first  = MergeTrace.mergeTraceIdHex(PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY);
        String second = MergeTrace.mergeTraceIdHex(PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY);

        assertEquals(first, second, "a re-evaluated or replayed window must derive the same ID");
        assertEquals(32, first.length(), "16 bytes, hex-encoded");
    }

    @Test
    @DisplayName("the two SQL functions agree — this is the join key")
    void functionsAgree() {
        Map<String, String> headers = new IsotopeMergeTrace()
                .eval(PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY, SERVICE, TOPIC);

        String edgeLabel = new IsotopeMergeTraceId()
                .eval(PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY);

        assertEquals(edgeLabel, headers.get(Isotope.HEADER_TRACE_ID),
                "ISOTOPE_MERGE_TRACE_ID must reproduce the merged record's trace ID exactly");
    }

    @Test
    @DisplayName("the derived ID survives into the isotope JSON, not just the scalar header")
    void jsonAndHeaderIdentityMatch() {
        Isotope iso = MergeTrace.mergeIsotope(
                PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY, SERVICE, TOPIC);

        String want = MergeTrace.mergeTraceIdHex(PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY);

        assertEquals(want, iso.traceIdHex(), "the JSON round-trip must carry the derived ID");
        // Re-parsing from the wire proves the substitution is not merely in memory.
        assertEquals(want, Isotope.fromJsonBytes(iso.toJsonBytes()).traceIdHex());
    }

    @Test
    @DisplayName("distinct windows, operators and group keys derive distinct IDs")
    void noCollisionsAcrossGrains() {
        String base = MergeTrace.mergeTraceIdHex(PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY);

        assertNotEquals(base, MergeTrace.mergeTraceIdHex(
                PIPELINE, OPERATOR, WIN_START + 60_000, WIN_END + 60_000, GROUP_KEY),
                "the next window is a different merged record");
        assertNotEquals(base, MergeTrace.mergeTraceIdHex(
                PIPELINE, "orders-dedupe", WIN_START, WIN_END, GROUP_KEY),
                "two merging stages over one window must not collide");
        assertNotEquals(base, MergeTrace.mergeTraceIdHex(
                PIPELINE, OPERATOR, WIN_START, WIN_END, "location"),
                "different group keys are different merged records");
        assertNotEquals(base, MergeTrace.mergeTraceIdHex(
                "location", OPERATOR, WIN_START, WIN_END, GROUP_KEY),
                "different pipelines are different merged records");
    }

    @Test
    @DisplayName("the ID is a well-formed UUIDv7 whose timestamp is the window end")
    void uuidV7Layout() {
        byte[] id = MergeTrace.mergeTraceId(PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY);

        assertEquals(Isotope.TRACE_ID_BYTES, id.length);
        assertEquals(0x70, id[6] & 0xF0, "version nibble must be 7");
        assertEquals(0x80, id[8] & 0xC0, "variant bits must be 10");

        long ts = 0L;
        for (int i = 0; i < 6; i++) {
            ts = (ts << 8) | (id[i] & 0xFFL);
        }
        assertEquals(WIN_END, ts, "the 48-bit timestamp field must be the window end");
    }

    @Test
    @DisplayName("merge IDs sort by event time, like every other isotope trace ID")
    void lexicographicOrderTracksTime() {
        String earlier = MergeTrace.mergeTraceIdHex(PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY);
        String later   = MergeTrace.mergeTraceIdHex(
                PIPELINE, OPERATOR, WIN_START + 60_000, WIN_END + 60_000, GROUP_KEY);

        assertTrue(earlier.compareTo(later) < 0,
                "UUIDv7 ordering is the reason the timestamp field is the window end");
    }

    @Test
    @DisplayName("a merged record carries a fresh trace with exactly one hop")
    void freshTraceOneHop() {
        Map<String, String> headers = MergeTrace.mergeHeaders(
                PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY, SERVICE, TOPIC);

        // hop_count = 1 is the load-bearing assertion: a merged record must NOT
        // continue a parent's hop chain. Its ancestry lives on the edge topic.
        assertEquals("1", headers.get(Isotope.HEADER_HOP_COUNT));
        assertEquals(SERVICE, headers.get(Isotope.HEADER_ORIGIN_SERVICE),
                "the merging stage is the origin of the new trace");
        assertEquals(SERVICE, headers.get(Isotope.HEADER_THIS_SERVICE));
        assertEquals(TOPIC,   headers.get(Isotope.HEADER_THIS_TOPIC));
        assertEquals(PIPELINE, headers.get(Isotope.HEADER_PIPELINE),
                "pipeline carries across the merge boundary even though identity does not");
        assertEquals(Long.toString(WIN_END), headers.get(Isotope.HEADER_ORIGIN_TS),
                "origin ts is the window end, never a clock read");
    }

    @Test
    @DisplayName("the header set matches what the 1:1 collector writes")
    void sameHeaderShapeAsAppendHop() {
        Map<String, String> merged = MergeTrace.mergeHeaders(
                PIPELINE, OPERATOR, WIN_START, WIN_END, GROUP_KEY, SERVICE, TOPIC);

        Map<String, String> appended = new IsotopeAppendHop()
                .eval(null, "flink-enrich", "orders.flink_enriched", PIPELINE, WIN_END);

        // Identical key sets are what let the typed views and all seven reports
        // read merged records without knowing merges exist.
        assertEquals(appended.keySet(), merged.keySet(),
                "a merged record must be indistinguishable in shape from any traced record");
    }

    @Test
    @DisplayName("null pipeline or group key is tolerated, not fatal")
    void nullsAreTolerated() {
        String withNulls = MergeTrace.mergeTraceIdHex(null, OPERATOR, WIN_START, WIN_END, null);

        assertEquals(32, withNulls.length());
        assertEquals(withNulls, MergeTrace.mergeTraceIdHex(null, OPERATOR, WIN_START, WIN_END, null),
                "still deterministic when the grouping columns are null");
    }
}
