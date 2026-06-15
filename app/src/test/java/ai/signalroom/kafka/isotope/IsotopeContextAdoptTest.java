/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.util.ArrayList;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@code isotope.consume.age} meter wired through
 * {@link IsotopeContext#adoptFromRecord(ConsumerRecord, String)}. Builds a
 * record carrying an isotope whose origin timestamp is a known distance in the
 * past, so the emitted age is predictable without a broker.
 */
class IsotopeContextAdoptTest {

    private static final String IN_TOPIC = "topic-AB";

    @AfterEach
    void reset() {
        IsotopeMetrics.resetForTest();
        IsotopeContext.clear();
    }

    /** A record carrying an isotope blob with the given origin timestamp. */
    private static ConsumerRecord<byte[], byte[]> recordWithIsotope(long originTsMs) {
        Isotope iso = new Isotope(
            Isotope.uuidV7Bytes(originTsMs), originTsMs,
            "order-intake-service", "orders", new ArrayList<>(), false);
        RecordHeaders h = new RecordHeaders();
        h.add(Isotope.HEADER_KEY, iso.toJsonBytes());
        return new ConsumerRecord<>(
            IN_TOPIC, 0, 42L,
            0L, TimestampType.CREATE_TIME,
            0, 0, new byte[0], new byte[0],
            h, Optional.empty());
    }

    @Test
    void twoArgAdoptEmitsConsumeAge() {
        IsotopeMetrics.ensureRegistry();

        long originTs = System.currentTimeMillis() - 5_000L; // ~5s old
        Isotope adopted = IsotopeContext.adoptFromRecord(
            recordWithIsotope(originTs), "order-enrichment-service");

        // Adoption behaves like the single-arg overload: isotope is returned
        // and installed as the thread-local.
        assertNotNull(adopted, "record carried an isotope, so it should adopt");
        assertSame(adopted, IsotopeContext.current(), "adopted iso is the thread-local");

        String text = IsotopeMetrics.scrape();
        assertEquals(1.0, value(text, "isotope_consume_age_seconds_count"), 1e-9);
        assertTrue(text.contains("pipeline=\"orders\""));
        assertTrue(text.contains("origin_service=\"order-intake-service\""));
        assertTrue(text.contains("consumer_service=\"order-enrichment-service\""));
        assertTrue(text.contains("this_topic=\"" + IN_TOPIC + "\""));

        // Age ≈ 5s; assert a generous band so wall-clock jitter in CI can't flake it.
        double sum = value(text, "isotope_consume_age_seconds_sum");
        assertTrue(sum >= 4.5 && sum < 60.0, () -> "age out of expected band: " + sum);
    }

    @Test
    void singleArgAdoptDoesNotEmit() {
        IsotopeMetrics.ensureRegistry();

        IsotopeContext.adoptFromRecord(recordWithIsotope(System.currentTimeMillis() - 1_000L));

        assertFalse(IsotopeMetrics.scrape().contains("isotope_consume_age"),
            "single-arg adopt must not emit the consume-age meter");
    }

    @Test
    void nullConsumerServiceDoesNotEmit() {
        IsotopeMetrics.ensureRegistry();

        IsotopeContext.adoptFromRecord(
            recordWithIsotope(System.currentTimeMillis() - 1_000L), null);

        assertFalse(IsotopeMetrics.scrape().contains("isotope_consume_age"),
            "null consumer service must not emit the consume-age meter");
    }

    private static double value(String exposition, String metric) {
        return exposition.lines()
            .filter(line -> line.startsWith(metric + "{"))
            .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("metric not found: " + metric + "\n" + exposition));
    }
}
