/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IsotopeCodecTest {

    @Test
    void newTraceHasIdServiceAndEmptyHops() {
        Isotope iso = Isotope.newTrace("order-intake-service");
        assertEquals("order-intake-service", iso.originService());
        assertEquals(16, iso.traceId().length);
        assertEquals(0, iso.hops().size());
        assertFalse(iso.truncated());
        assertTrue(iso.originTsMs() > 0);
    }

    @Test
    void newTraceDefaultsPipelineToUnknownAndKeepsExplicitName() {
        assertEquals("unknown", Isotope.newTrace("order-intake-service").pipeline());
        assertEquals("orders",
            Isotope.newTrace("order-intake-service", "orders").pipeline());
    }

    // -- UUIDv7 --------------------------------------------------------

    @Test
    void traceIdIsUuidV7WithCorrectVersionAndVariantBits() {
        Isotope iso = Isotope.newTrace("order-intake-service");
        byte[] id = iso.traceId();
        // byte 6 high nibble must be 0111 (version 7)
        assertEquals(0x70, id[6] & 0xF0, "UUIDv7 version nibble");
        // byte 8 high 2 bits must be 10 (RFC 9562 variant)
        assertEquals(0x80, id[8] & 0xC0, "UUIDv7 variant bits");
    }

    @Test
    void traceIdEmbedsCreationTimestampInTopBits() {
        long before = System.currentTimeMillis();
        Isotope iso = Isotope.newTrace("order-intake-service");
        long after  = System.currentTimeMillis();

        byte[] id = iso.traceId();
        long embeddedMs =
              ((long) (id[0] & 0xFF) << 40)
            | ((long) (id[1] & 0xFF) << 32)
            | ((long) (id[2] & 0xFF) << 24)
            | ((long) (id[3] & 0xFF) << 16)
            | ((long) (id[4] & 0xFF) <<  8)
            |  (long) (id[5] & 0xFF);

        assertTrue(embeddedMs >= before && embeddedMs <= after,
            "UUIDv7 embedded ms " + embeddedMs + " not in [" + before + ", " + after + "]");
        assertEquals(iso.originTsMs(), embeddedMs,
            "embedded UUIDv7 ms should match Isotope.originTsMs()");
    }

    @Test
    void traceIdsCreatedLaterSortLexicographicallyAfter() throws Exception {
        Isotope first = Isotope.newTrace("order-intake-service");
        Thread.sleep(2);                                  // ensure ms advance
        Isotope second = Isotope.newTrace("order-intake-service");

        // Hex string comparison must follow chronological order.
        assertTrue(second.traceIdHex().compareTo(first.traceIdHex()) > 0,
            "second trace id " + second.traceIdHex()
                + " should sort after first " + first.traceIdHex());
    }

    @Test
    void uuidV7StringIsCanonicalUuidWithVersion7Nibble() {
        String s = Isotope.uuidV7String();
        assertEquals(36, s.length(), "canonical UUID is 36 chars");
        // version nibble lives at char index 14 (after 3 hex groups + 3 dashes: 8-4-4)
        assertEquals('7', s.charAt(14), "version nibble in canonical form");
        // variant char at index 19 must be 8, 9, a, or b (top 2 bits = 10)
        char v = s.charAt(19);
        assertTrue("89ab".indexOf(v) >= 0, "variant char should be 8/9/a/b, was " + v);
    }

    @Test
    void jsonRoundtripPreservesAllFields() {
        Isotope iso = Isotope.newTrace("order-intake-service", "orders")
            .appendHop(new Isotope.Hop("order-intake-service", "topic-1", 1_000L))
            .appendHop(new Isotope.Hop("order-enrichment-service", "topic-2", 2_000L));

        byte[] bytes = iso.toJsonBytes();
        Isotope decoded = Isotope.fromJsonBytes(bytes);

        assertArrayEquals(iso.traceId(), decoded.traceId());
        assertEquals(iso.originTsMs(), decoded.originTsMs());
        assertEquals(iso.originService(), decoded.originService());
        assertEquals(iso.pipeline(), decoded.pipeline());
        assertEquals(iso.truncated(), decoded.truncated());
        assertEquals(iso.hops(), decoded.hops());
    }

    @Test
    void hopsBeyondMaxAreEvictedAndFlagSet() {
        Isotope iso = Isotope.newTrace("order-intake-service");
        for (int i = 0; i < Isotope.MAX_HOPS + 5; i++) {
            iso.appendHop(new Isotope.Hop("svc-" + i, "topic-" + i, i));
        }
        assertEquals(Isotope.MAX_HOPS, iso.hops().size());
        assertTrue(iso.truncated());

        // Oldest 5 should be gone; newest should be the last appended.
        Isotope.Hop newest = iso.hops().get(iso.hops().size() - 1);
        assertEquals("topic-" + (Isotope.MAX_HOPS + 4), newest.topic());
    }

    @Test
    void fromHeadersReturnsNullWhenAbsent() {
        RecordHeaders headers = new RecordHeaders();
        assertNull(Isotope.fromHeaders(headers));
    }

    @Test
    void fromHeadersDecodesWhenPresent() {
        Isotope iso = Isotope.newTrace("order-intake-service")
            .appendHop(new Isotope.Hop("order-intake-service", "topic-1", 1_000L));
        RecordHeaders headers = new RecordHeaders();
        headers.add(Isotope.HEADER_KEY, iso.toJsonBytes());

        Isotope decoded = Isotope.fromHeaders(headers);
        assertNotNull(decoded);
        assertEquals("order-intake-service", decoded.originService());
        assertEquals(1, decoded.hops().size());
    }

    @Test
    void headerSizeStaysBoundedForTypicalPipeline() {
        // Sanity check: a 3-hop trace should fit in well under a kilobyte
        // even after the CBOR → JSON switch.
        Isotope iso = Isotope.newTrace("ingest-service")
            .appendHop(new Isotope.Hop("ingest-service", "raw-events", 1_700_000_000_000L))
            .appendHop(new Isotope.Hop("enrich-service", "enriched-events", 1_700_000_000_001L))
            .appendHop(new Isotope.Hop("aggregate-service", "agg-events", 1_700_000_000_002L));

        int size = iso.toJsonBytes().length;
        assertTrue(size > 0 && size < 1024,
            "expected 3-hop isotope to fit in < 1024 bytes, was " + size);
    }
}
