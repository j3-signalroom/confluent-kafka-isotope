package ai.signalroom.kafka.isotope.flink;

import ai.signalroom.kafka.isotope.Isotope;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IsotopeAppendHopTest {

    private final IsotopeAppendHop fn = new IsotopeAppendHop();

    @Test
    void mintsATraceWhenTheRecordArrivesUntraced() {
        Map<String, String> out = fn.eval(null, "flink-enrich", "orders.enriched", "orders", 1_700_000_000_000L);

        assertNotNull(out.get(Isotope.HEADER_TRACE_ID));
        assertEquals("flink-enrich",    out.get(Isotope.HEADER_ORIGIN_SERVICE));
        assertEquals("orders",          out.get(Isotope.HEADER_PIPELINE));
        assertEquals("flink-enrich",    out.get(Isotope.HEADER_THIS_SERVICE));
        assertEquals("orders.enriched", out.get(Isotope.HEADER_THIS_TOPIC));
        assertEquals("1",               out.get(Isotope.HEADER_HOP_COUNT));
    }

    @Test
    void appendsToAnInboundTraceAndKeepsOriginIdentity() {
        // Simulate what a service's interceptor produced upstream.
        Isotope upstream = Isotope.newTrace("enrich", "orders", 1_700_000_000_000L)
                                  .appendHop(new Isotope.Hop("enrich", "orders.placed", 1_700_000_000_000L));

        Map<String, String> in = new HashMap<>();
        in.put(Isotope.HEADER_KEY, new String(upstream.toJsonBytes()));
        in.put("unrelated-header", "must-survive");

        Map<String, String> out = fn.eval(in, "flink-enrich", "orders.enriched", "orders", 1_700_000_005_000L);

        assertEquals(upstream.traceIdHex(), out.get(Isotope.HEADER_TRACE_ID), "trace identity carries through");
        assertEquals("enrich",          out.get(Isotope.HEADER_ORIGIN_SERVICE), "origin is not overwritten");
        assertEquals("1700000000000",   out.get(Isotope.HEADER_ORIGIN_TS));
        assertEquals("flink-enrich",    out.get(Isotope.HEADER_THIS_SERVICE));
        assertEquals("orders.enriched", out.get(Isotope.HEADER_THIS_TOPIC));
        assertEquals("2",               out.get(Isotope.HEADER_HOP_COUNT), "hop appended, not replaced");
        assertEquals("must-survive",    out.get("unrelated-header"), "non-isotope headers preserved");

        // The JSON chain and the scalar hop-count must agree — the reports read
        // the scalars, but a drifting chain would make the JSON useless for triage.
        Isotope rehydrated = Isotope.fromJsonBytes(out.get(Isotope.HEADER_KEY).getBytes());
        assertEquals(2, rehydrated.hops().size());
        assertEquals("orders.enriched", rehydrated.hops().get(1).topic());
    }
}
