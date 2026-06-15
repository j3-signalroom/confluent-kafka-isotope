/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.net.ServerSocket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the Micrometer/Prometheus exporter — no broker, no HTTP
 * server. {@link IsotopeMetrics#ensureRegistry()} binds the registry without
 * opening a socket, and {@link IsotopeMetrics#scrape()} returns the exposition
 * text the {@code /metrics} endpoint would serve, so the assertions run against
 * exactly what Prometheus would scrape.
 */
class IsotopeMetricsTest {

    @AfterEach
    void reset() {
        IsotopeMetrics.resetForTest();
    }

    // -- gating ---------------------------------------------------------

    @Test
    void disabledByDefaultIsANoOp() {
        assertFalse(IsotopeMetrics.isEnabled());
        // Unguarded call before the exporter is bound must not throw or emit.
        IsotopeMetrics.recordHop("orders", "order-intake-service",
            "order-enrichment-service", "orders.enriched", 1500L, 2);
        assertEquals("", IsotopeMetrics.scrape());
    }

    @Test
    void ensureRegistryEnablesEmission() {
        IsotopeMetrics.ensureRegistry();
        assertTrue(IsotopeMetrics.isEnabled());
    }

    @Test
    void startOnTakenPortDegradesGracefully() throws Exception {
        // Occupy an ephemeral port, then ask the exporter to bind the same one.
        try (ServerSocket hog = new ServerSocket(0)) {
            int taken = hog.getLocalPort();
            // Optional sidecar: a port clash must NOT throw and must leave the
            // exporter unbound, so the caller's pipeline keeps running.
            assertDoesNotThrow(() -> IsotopeMetrics.start(taken));
            assertFalse(IsotopeMetrics.isEnabled());
            assertEquals("", IsotopeMetrics.scrape());
        }
    }

    // -- exposition -----------------------------------------------------

    @Test
    void recordHopEmitsTimerAndCounterExposition() {
        IsotopeMetrics.ensureRegistry();

        // Two identical hops → count 2, sum 3.0s (2 × 1.5s) on one series each.
        for (int i = 0; i < 2; i++) {
            IsotopeMetrics.recordHop("orders", "order-intake-service",
                "order-enrichment-service", "orders.enriched", 1500L, 2);
        }

        String text = IsotopeMetrics.scrape();

        // Metric names use Micrometer's Prometheus naming: dots→underscores,
        // the Timer carries a `_seconds` base unit, the Counter a `_total` suffix.
        assertTrue(text.contains("isotope_hop_latency_seconds_count"),
            () -> "missing latency timer count:\n" + text);
        assertTrue(text.contains("isotope_hop_records_total"),
            () -> "missing hop-records counter:\n" + text);

        // Labels (Prometheus order isn't guaranteed, so assert per-label).
        assertTrue(text.contains("pipeline=\"orders\""));
        assertTrue(text.contains("origin_service=\"order-intake-service\""));
        assertTrue(text.contains("this_service=\"order-enrichment-service\""));
        assertTrue(text.contains("this_topic=\"orders.enriched\""));
        assertTrue(text.contains("hop_count=\"2\""));

        // Values: latency timer aggregated both records; counter incremented twice.
        assertEquals(2.0, value(text, "isotope_hop_latency_seconds_count"), 1e-9);
        assertEquals(3.0, value(text, "isotope_hop_latency_seconds_sum"),   1e-9);
        assertEquals(2.0, value(text, "isotope_hop_records_total"),         1e-9);
    }

    @Test
    void negativeLatencyIsClampedToZero() {
        IsotopeMetrics.ensureRegistry();

        // Cross-service clock skew can make origin→hop go negative; it must be
        // clamped (recorded as 0), not dropped or thrown.
        IsotopeMetrics.recordHop("orders", "order-intake-service",
            "order-enrichment-service", "orders.enriched", -50L, 2);

        String text = IsotopeMetrics.scrape();
        assertEquals(1.0, value(text, "isotope_hop_latency_seconds_count"), 1e-9);
        assertEquals(0.0, value(text, "isotope_hop_latency_seconds_sum"),   1e-9);
    }

    @Test
    void distinctHopCountsAreSeparateSeries() {
        IsotopeMetrics.ensureRegistry();

        IsotopeMetrics.recordHop("orders", "order-intake-service",
            "order-enrichment-service", "orders.enriched", 100L, 2);
        IsotopeMetrics.recordHop("orders", "order-intake-service",
            "order-fulfillment-service", "orders.fulfilled", 100L, 3);

        // hop_distribution keys on hop_count, so the counter splits into two
        // series — one per bucket — exactly like the Flink report's GROUP BY.
        String text = IsotopeMetrics.scrape();
        long records = text.lines()
            .filter(l -> l.startsWith("isotope_hop_records_total{"))
            .count();
        assertEquals(2, records, () -> "expected one series per hop_count:\n" + text);
    }

    // -- consume side ---------------------------------------------------

    @Test
    void recordConsumeEmitsTimerAndCounterExposition() {
        IsotopeMetrics.ensureRegistry();

        // Two identical consumes → count 2, sum 5.0s (2 × 2.5s) on one series each.
        for (int i = 0; i < 2; i++) {
            IsotopeMetrics.recordConsume("orders", "order-intake-service",
                "order-enrichment-service", "orders.enriched", 2500L);
        }

        String text = IsotopeMetrics.scrape();

        assertTrue(text.contains("isotope_consume_latency_seconds_count"),
            () -> "missing consume latency timer count:\n" + text);
        assertTrue(text.contains("isotope_consume_records_total"),
            () -> "missing consume-records counter:\n" + text);

        assertTrue(text.contains("pipeline=\"orders\""));
        assertTrue(text.contains("origin_service=\"order-intake-service\""));
        assertTrue(text.contains("consumer_service=\"order-enrichment-service\""));
        assertTrue(text.contains("this_topic=\"orders.enriched\""));

        assertEquals(2.0, value(text, "isotope_consume_latency_seconds_count"), 1e-9);
        assertEquals(5.0, value(text, "isotope_consume_latency_seconds_sum"),   1e-9);
        assertEquals(2.0, value(text, "isotope_consume_records_total"),         1e-9);
    }

    @Test
    void recordConsumeWithoutLatencyStillCountsTheEdge() {
        IsotopeMetrics.ensureRegistry();

        // A marker whose inbound record carried no origin timestamp (latency < 0):
        // the consume edge is still counted, but no latency timer series appears.
        IsotopeMetrics.recordConsume("orders", "order-intake-service",
            "order-enrichment-service", "orders.enriched", -1L);

        String text = IsotopeMetrics.scrape();
        assertEquals(1.0, value(text, "isotope_consume_records_total"), 1e-9);
        assertFalse(text.contains("isotope_consume_latency_seconds_count"),
            () -> "latency timer should be skipped when origin ts is absent:\n" + text);
    }

    @Test
    void recordConsumeAgeEmitsTimerExposition() {
        IsotopeMetrics.ensureRegistry();

        // Two identical adoptions → count 2, sum 7.0s (2 × 3.5s) on one series.
        for (int i = 0; i < 2; i++) {
            IsotopeMetrics.recordConsumeAge("orders", "order-intake-service",
                "order-enrichment-service", "orders.enriched", 3500L);
        }

        String text = IsotopeMetrics.scrape();

        assertTrue(text.contains("isotope_consume_age_seconds_count"),
            () -> "missing consume-age timer count:\n" + text);

        assertTrue(text.contains("pipeline=\"orders\""));
        assertTrue(text.contains("origin_service=\"order-intake-service\""));
        assertTrue(text.contains("consumer_service=\"order-enrichment-service\""));
        assertTrue(text.contains("this_topic=\"orders.enriched\""));

        assertEquals(2.0, value(text, "isotope_consume_age_seconds_count"), 1e-9);
        assertEquals(7.0, value(text, "isotope_consume_age_seconds_sum"),   1e-9);
    }

    @Test
    void recordConsumeAgeClampsNegativeToZero() {
        IsotopeMetrics.ensureRegistry();

        // Cross-service clock skew can make now − originTs go negative; like the
        // hop latency timer it must clamp to 0, not drop or throw.
        IsotopeMetrics.recordConsumeAge("orders", "order-intake-service",
            "order-enrichment-service", "orders.enriched", -50L);

        String text = IsotopeMetrics.scrape();
        assertEquals(1.0, value(text, "isotope_consume_age_seconds_count"), 1e-9);
        assertEquals(0.0, value(text, "isotope_consume_age_seconds_sum"),   1e-9);
    }

    /**
     * Reads the single value of a labelled metric line out of the exposition.
     * Each test uses one series per metric, so the first matching line is it;
     * the value is the whitespace-delimited token after the label set.
     */
    private static double value(String exposition, String metric) {
        return exposition.lines()
            .filter(line -> line.startsWith(metric + "{"))
            .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
            .findFirst()
            .orElseThrow(() -> new AssertionError(
                "metric not found: " + metric + "\n" + exposition));
    }
}
