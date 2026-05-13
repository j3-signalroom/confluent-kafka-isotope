package com.life360.kafka.isotope;

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
        Isotope iso = Isotope.newTrace("svc-A");
        assertEquals("svc-A", iso.originService());
        assertEquals(16, iso.traceId().length);
        assertEquals(0, iso.hops().size());
        assertFalse(iso.truncated());
        assertTrue(iso.originTsMs() > 0);
    }

    @Test
    void jsonRoundtripPreservesAllFields() {
        Isotope iso = Isotope.newTrace("svc-A")
            .appendHop(new Isotope.Hop("svc-A", "topic-1", 1_000L))
            .appendHop(new Isotope.Hop("svc-B", "topic-2", 2_000L));

        byte[] bytes = iso.toJsonBytes();
        Isotope decoded = Isotope.fromJsonBytes(bytes);

        assertArrayEquals(iso.traceId(), decoded.traceId());
        assertEquals(iso.originTsMs(), decoded.originTsMs());
        assertEquals(iso.originService(), decoded.originService());
        assertEquals(iso.truncated(), decoded.truncated());
        assertEquals(iso.hops(), decoded.hops());
    }

    @Test
    void hopsBeyondMaxAreEvictedAndFlagSet() {
        Isotope iso = Isotope.newTrace("svc-A");
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
        Isotope iso = Isotope.newTrace("svc-A")
            .appendHop(new Isotope.Hop("svc-A", "topic-1", 1_000L));
        RecordHeaders headers = new RecordHeaders();
        headers.add(Isotope.HEADER_KEY, iso.toJsonBytes());

        Isotope decoded = Isotope.fromHeaders(headers);
        assertNotNull(decoded);
        assertEquals("svc-A", decoded.originService());
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
