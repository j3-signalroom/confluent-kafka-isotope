/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;

/**
 * Thread-local handoff for the in-flight isotope between a consumer and a
 * downstream producer in the same processing thread. In a typical
 * consume-then-produce service, the application calls
 * {@link #adoptFromRecord(ConsumerRecord)} before processing each record so
 * that {@link IsotopeProducerInterceptor} can find the inbound trace context
 * and append the next hop instead of starting a new trace.
 *
 * <p>For bipartite topology visibility, consumers also call
 * {@link #recordConsume(ConsumerRecord, String, KafkaProducer)} to emit a
 * best-effort consume-edge marker to {@value #CONSUME_EVENTS_TOPIC}. The
 * marker carries the inbound record's scalar isotope headers plus a new
 * {@link Isotope#HEADER_CONSUMER_SERVICE} naming the consumer, so Flink can
 * union produce edges (from the {@code isotope} view) with consume edges
 * (from a {@code consume_events} view over {@value #CONSUME_EVENTS_TOPIC})
 * into a single bipartite (service ↔ topic ↔ service) report.
 */
public final class IsotopeContext {

    /** Default topic that {@link #recordConsume} writes markers to. */
    public static final String CONSUME_EVENTS_TOPIC = "iso_consume_events";

    private static final ThreadLocal<Isotope> CURRENT = new ThreadLocal<>();

    private IsotopeContext() {}

    public static Isotope current()        { return CURRENT.get(); }
    public static void set(Isotope iso)    { CURRENT.set(iso); }
    public static void clear()             { CURRENT.remove(); }

    /**
     * Extracts the isotope from the given record's headers (if any) and
     * installs it as the current thread-local context. Returns the isotope
     * adopted, or {@code null} if the record carried no isotope.
     */
    public static Isotope adoptFromRecord(ConsumerRecord<?, ?> record) {
        Isotope iso = Isotope.fromHeaders(record.headers());
        if (iso != null) {
            set(iso);
        } else {
            clear();
        }
        return iso;
    }

    /**
     * Emits a best-effort consume-edge marker to {@value #CONSUME_EVENTS_TOPIC}
     * using the default topic name. Equivalent to
     * {@link #recordConsume(ConsumerRecord, String, Producer, String)}
     * with {@code topic = CONSUME_EVENTS_TOPIC}.
     */
    public static void recordConsume(
            ConsumerRecord<?, ?> record,
            String consumerService,
            Producer<byte[], byte[]> emitter) {
        recordConsume(record, consumerService, emitter, CONSUME_EVENTS_TOPIC);
    }

    /**
     * Emits a best-effort consume-edge marker describing the consumption of
     * {@code record} by {@code consumerService}. The marker is an empty-value
     * record on {@code topic} whose headers forward every scalar
     * {@code x-isotope-*} header from the inbound record plus a new
     * {@link Isotope#HEADER_CONSUMER_SERVICE} naming the consumer.
     *
     * <p><b>No-op</b> when the inbound record carries no
     * {@link Isotope#HEADER_TRACE_ID} — untagged records don't belong in the
     * bipartite graph.
     *
     * <p>Fire-and-forget: the send is async with no callback. A failed marker
     * leaves a hole in the bipartite report but does not affect the host
     * application's consume/produce pipeline. The {@link Producer}
     * lifecycle (creation, configuration, close) is the caller's responsibility.
     */
    public static void recordConsume(
            ConsumerRecord<?, ?> record,
            String consumerService,
            Producer<byte[], byte[]> emitter,
            String topic) {
        if (record == null || record.headers() == null) return;
        Header traceHeader = record.headers().lastHeader(Isotope.HEADER_TRACE_ID);
        if (traceHeader == null) return;

        RecordHeaders markerHeaders = new RecordHeaders();
        forwardHeader(record, markerHeaders, Isotope.HEADER_TRACE_ID);
        forwardHeader(record, markerHeaders, Isotope.HEADER_ORIGIN_TS);
        forwardHeader(record, markerHeaders, Isotope.HEADER_ORIGIN_SERVICE);
        forwardHeader(record, markerHeaders, Isotope.HEADER_THIS_SERVICE);
        forwardHeader(record, markerHeaders, Isotope.HEADER_THIS_TOPIC);
        forwardHeader(record, markerHeaders, Isotope.HEADER_HOP_COUNT);
        markerHeaders.add(
            Isotope.HEADER_CONSUMER_SERVICE,
            (consumerService == null ? "unknown" : consumerService)
                .getBytes(StandardCharsets.UTF_8));

        emitter.send(new ProducerRecord<>(
            topic, null, null, null, null, markerHeaders));
    }

    private static void forwardHeader(
            ConsumerRecord<?, ?> source, RecordHeaders dest, String key) {
        Header h = source.headers().lastHeader(key);
        if (h != null) dest.add(key, h.value());
    }
}
