/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Thread-local handoff for the in-flight isotope between a consumer and a
 * downstream producer in the same processing thread. In a typical
 * consume-then-produce service, the application calls
 * {@link #adoptFromRecord(ConsumerRecord)} before processing each record so
 * that {@link IsotopeProducerInterceptor} can find the inbound trace context
 * and append the next hop instead of starting a new trace.
 *
 * <p>For bipartite topology visibility, consumers also call
 * {@link #recordConsume(ConsumerRecord, String, Producer)} to emit a
 * best-effort consume-edge marker to {@value #CONSUME_EVENTS_TOPIC}.
 */
public final class IsotopeContext {

    /** Default topic that {@link #recordConsume} writes markers to. */
    public static final String CONSUME_EVENTS_TOPIC = "platform.observability.consume_events";

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
    public static Isotope adoptFromRecord(ConsumerRecord<?, ?> consumerRecord) {
        Isotope iso = Isotope.fromHeaders(consumerRecord.headers());
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
            ConsumerRecord<?, ?> consumerRecord,
            String consumerService,
            Producer<byte[], byte[]> emitter) {
        recordConsume(consumerRecord, consumerService, emitter, CONSUME_EVENTS_TOPIC);
    }

    /**
     * Emits a best-effort consume-edge marker describing the consumption of
     * {@code record} by {@code consumerService}. The marker is an empty-value
     * record on {@code topic} whose headers forward every scalar
     * {@code x-isotope-*} header from the inbound record plus a new
     * {@link Isotope#HEADER_CONSUMER_SERVICE} naming the consumer.
     *
     * <p><b>No-op</b> when the inbound record carries no
     * {@link Isotope#HEADER_TRACE_ID} - untagged records don't belong in the
     * bipartite graph.
     *
     * <p>Fire-and-forget: the send is async with no callback. The
     * {@link Producer} lifecycle is the caller's responsibility.
     *
     * <p>When the Micrometer exporter is enabled ({@link IsotopeMetrics#start}),
     * this also emits the stateless consume-edge metrics for the marker via
     * {@link IsotopeMetrics#recordConsume} — the consume-side analogue of the
     * produce-side {@link IsotopeProducerInterceptor} emission.
     */
    public static void recordConsume(
            ConsumerRecord<?, ?> consumerRecord,
            String consumerService,
            Producer<byte[], byte[]> emitter,
            String topic) {
        if (consumerRecord == null || consumerRecord.headers() == null) {
            return;
        }
        Header traceHeader = consumerRecord.headers().lastHeader(Isotope.HEADER_TRACE_ID);
        if (traceHeader == null) {
            return;
        }

        RecordHeaders markerHeaders = new RecordHeaders();
        forwardHeader(consumerRecord, markerHeaders, Isotope.HEADER_TRACE_ID);
        forwardHeader(consumerRecord, markerHeaders, Isotope.HEADER_ORIGIN_TS);
        forwardHeader(consumerRecord, markerHeaders, Isotope.HEADER_ORIGIN_SERVICE);
        forwardHeader(consumerRecord, markerHeaders, Isotope.HEADER_PIPELINE);
        forwardHeader(consumerRecord, markerHeaders, Isotope.HEADER_THIS_SERVICE);
        forwardHeader(consumerRecord, markerHeaders, Isotope.HEADER_THIS_TOPIC);
        forwardHeader(consumerRecord, markerHeaders, Isotope.HEADER_HOP_COUNT);
        markerHeaders.add(
            Isotope.HEADER_CONSUMER_SERVICE,
            (consumerService == null ? "unknown" : consumerService)
                .getBytes(StandardCharsets.UTF_8));

        emitter.send(new ProducerRecord<>(
            topic, null, null, null, null, markerHeaders));

        // Mirror the produce-side IsotopeProducerInterceptor: also emit the
        // stateless consume-edge metrics (topic→consumer count + time-to-consume
        // latency) to Micrometer for Prometheus/Grafana. No-op unless the app
        // started the exporter (IsotopeMetrics.start).
        if (IsotopeMetrics.isEnabled()) {
            long originTs = parseLongOrNeg(
                headerString(consumerRecord, Isotope.HEADER_ORIGIN_TS, null));
            long latencyMs = originTs < 0
                ? -1L
                : Math.max(0L, System.currentTimeMillis() - originTs);
            IsotopeMetrics.recordConsume(
                headerString(consumerRecord, Isotope.HEADER_PIPELINE,       "unknown"),
                headerString(consumerRecord, Isotope.HEADER_ORIGIN_SERVICE, "unknown"),
                consumerService == null ? "unknown" : consumerService,
                headerString(consumerRecord, Isotope.HEADER_THIS_TOPIC,     "unknown"),
                latencyMs);
        }
    }

    private static void forwardHeader(
            ConsumerRecord<?, ?> source, RecordHeaders dest, String key) {
        Header h = source.headers().lastHeader(key);
        if (h != null) {
            dest.add(key, h.value());
        }
    }

    /** Reads a scalar isotope header as a UTF-8 string, or {@code dflt} if absent. */
    private static String headerString(
            ConsumerRecord<?, ?> source, String key, String dflt) {
        Header h = source.headers().lastHeader(key);
        return h == null ? dflt : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** Parses {@code s} as a long, returning {@code -1} when null or malformed. */
    private static long parseLongOrNeg(String s) {
        if (s == null) return -1L;
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return -1L;
        }
    }
}