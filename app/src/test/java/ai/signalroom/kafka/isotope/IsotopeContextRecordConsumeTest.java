/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IsotopeContext#recordConsume}. Uses Kafka's
 * {@link MockProducer} so the tests don't need a real broker.
 */
class IsotopeContextRecordConsumeTest {

    private static final String IN_TOPIC = "topic-AB";
    private static final String MARKER_TOPIC = IsotopeContext.CONSUME_EVENTS_TOPIC;

    @AfterEach
    void reset() {
        IsotopeMetrics.resetForTest();
        IsotopeContext.clear();
    }

    private static ConsumerRecord<byte[], byte[]> taggedRecord(
            String traceIdHex,
            String originService,
            String thisService,
            String thisTopic,
            int hopCount) {
        RecordHeaders h = new RecordHeaders();
        h.add(Isotope.HEADER_TRACE_ID,       traceIdHex.getBytes(StandardCharsets.UTF_8));
        h.add(Isotope.HEADER_ORIGIN_TS,      Long.toString(1_700_000_000_000L).getBytes(StandardCharsets.UTF_8));
        h.add(Isotope.HEADER_ORIGIN_SERVICE, originService.getBytes(StandardCharsets.UTF_8));
        h.add(Isotope.HEADER_PIPELINE,       "orders".getBytes(StandardCharsets.UTF_8));
        h.add(Isotope.HEADER_THIS_SERVICE,   thisService.getBytes(StandardCharsets.UTF_8));
        h.add(Isotope.HEADER_THIS_TOPIC,     thisTopic.getBytes(StandardCharsets.UTF_8));
        h.add(Isotope.HEADER_HOP_COUNT,      Integer.toString(hopCount).getBytes(StandardCharsets.UTF_8));
        return new ConsumerRecord<>(
            thisTopic, 0, 42L,
            0L, org.apache.kafka.common.record.TimestampType.CREATE_TIME,
            0, 0, new byte[0], new byte[0],
            h, java.util.Optional.empty());
    }

    private static String headerString(ProducerRecord<byte[], byte[]> rec, String key) {
        Header h = rec.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Test
    void emitsMarkerWithForwardedHeadersAndConsumerServiceAdded() {
        MockProducer<byte[], byte[]> mock = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());

        ConsumerRecord<byte[], byte[]> inbound =
            taggedRecord("0192abcd-deadbeef", "order-intake-service", "order-intake-service", IN_TOPIC, 1);

        IsotopeContext.recordConsume(inbound, "order-enrichment-service", mock);

        List<ProducerRecord<byte[], byte[]>> sent = mock.history();
        assertEquals(1, sent.size(), "exactly one marker should be emitted");
        ProducerRecord<byte[], byte[]> marker = sent.get(0);

        assertEquals(MARKER_TOPIC, marker.topic());
        assertNull(marker.key(),   "marker key is null");
        assertNull(marker.value(), "marker value is null");

        // Forwarded headers
        assertEquals("0192abcd-deadbeef", headerString(marker, Isotope.HEADER_TRACE_ID));
        assertEquals("1700000000000",     headerString(marker, Isotope.HEADER_ORIGIN_TS));
        assertEquals("order-intake-service",             headerString(marker, Isotope.HEADER_ORIGIN_SERVICE));
        assertEquals("orders",            headerString(marker, Isotope.HEADER_PIPELINE));
        assertEquals("order-intake-service",             headerString(marker, Isotope.HEADER_THIS_SERVICE));
        assertEquals(IN_TOPIC,            headerString(marker, Isotope.HEADER_THIS_TOPIC));
        assertEquals("1",                 headerString(marker, Isotope.HEADER_HOP_COUNT));

        // Newly stamped consumer service
        assertEquals("order-enrichment-service", headerString(marker, Isotope.HEADER_CONSUMER_SERVICE));
    }

    @Test
    void noOpWhenInboundRecordHasNoTraceId() {
        MockProducer<byte[], byte[]> mock = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());

        ConsumerRecord<byte[], byte[]> untagged = new ConsumerRecord<>(
            IN_TOPIC, 0, 0L,
            0L, org.apache.kafka.common.record.TimestampType.CREATE_TIME,
            0, 0, new byte[0], new byte[0],
            new RecordHeaders(), java.util.Optional.empty());

        IsotopeContext.recordConsume(untagged, "order-enrichment-service", mock);

        assertTrue(mock.history().isEmpty(),
            "no marker should be emitted for untagged records");
    }

    @Test
    void nullConsumerServiceDefaultsToUnknown() {
        MockProducer<byte[], byte[]> mock = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());

        ConsumerRecord<byte[], byte[]> inbound =
            taggedRecord("0192abcd-deadbeef", "order-intake-service", "order-intake-service", IN_TOPIC, 1);

        IsotopeContext.recordConsume(inbound, null, mock);

        ProducerRecord<byte[], byte[]> marker = mock.history().get(0);
        assertNotNull(marker.headers().lastHeader(Isotope.HEADER_CONSUMER_SERVICE));
        assertEquals("unknown", headerString(marker, Isotope.HEADER_CONSUMER_SERVICE));
    }

    @Test
    void overloadWithCustomTopicEmitsToThatTopic() {
        MockProducer<byte[], byte[]> mock = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());
        String custom = "alt_consume_events";

        ConsumerRecord<byte[], byte[]> inbound =
            taggedRecord("0192abcd-deadbeef", "order-intake-service", "order-intake-service", IN_TOPIC, 1);

        IsotopeContext.recordConsume(inbound, "order-enrichment-service", mock, custom);

        assertEquals(custom, mock.history().get(0).topic());
    }

    // -- consume-age meter (terminal-consumer path) ---------------------

    @Test
    void terminalConsumeEmitsConsumeAge() {
        IsotopeMetrics.ensureRegistry();
        MockProducer<byte[], byte[]> mock = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());

        ConsumerRecord<byte[], byte[]> inbound =
            taggedRecord("0192abcd-deadbeef", "order-intake-service", "order-intake-service", IN_TOPIC, 1);

        // Terminal consumer: never adopted, so current() is null and the age
        // meter is emitted here (the adoption path would otherwise miss it).
        assertNull(IsotopeContext.current(), "precondition: no adoption on this thread");
        IsotopeContext.recordConsume(inbound, "order-enrichment-service", mock);

        String text = IsotopeMetrics.scrape();
        assertEquals(1.0, value(text, "isotope_consume_age_seconds_count"), 1e-9);
        assertTrue(text.contains("consumer_service=\"order-enrichment-service\""));
        assertTrue(text.contains("origin_service=\"order-intake-service\""));
        assertTrue(text.contains("pipeline=\"orders\""));
        assertTrue(text.contains("this_topic=\"" + IN_TOPIC + "\""));
    }

    @Test
    void adoptedRecordConsumeDoesNotDoubleEmitAge() {
        IsotopeMetrics.ensureRegistry();
        MockProducer<byte[], byte[]> mock = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());

        ConsumerRecord<byte[], byte[]> inbound =
            taggedRecord("0192abcd-deadbeef", "order-intake-service", "order-intake-service", IN_TOPIC, 1);

        // Simulate a stage that already adopted (e.g. `hop`): current() is set,
        // so age was emitted on the adoption path and recordConsume must skip it
        // — but the edge counter still fires.
        IsotopeContext.set(Isotope.newTrace("order-enrichment-service"));
        IsotopeContext.recordConsume(inbound, "order-enrichment-service", mock);

        String text = IsotopeMetrics.scrape();
        assertFalse(text.contains("isotope_consume_age"),
            () -> "adopted record must not double-emit age via recordConsume:\n" + text);
        assertEquals(1.0, value(text, "isotope_consume_records_total"), 1e-9);
    }

    private static double value(String exposition, String metric) {
        return exposition.lines()
            .filter(line -> line.startsWith(metric + "{"))
            .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("metric not found: " + metric + "\n" + exposition));
    }
}
