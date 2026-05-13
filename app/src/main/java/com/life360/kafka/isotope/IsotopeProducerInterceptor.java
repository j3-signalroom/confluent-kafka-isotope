package com.life360.kafka.isotope;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Producer-side half of the isotope tracer.
 *
 * On every {@code send()}, this interceptor finds (or creates) the in-flight
 * {@link Isotope}, appends a hop describing this produce edge, and writes:
 *
 *   - the JSON-encoded full isotope into {@value Isotope#HEADER_KEY}
 *     (used for downstream propagation — the next hop reads this header to
 *     reconstruct the trace and extend the hop list);
 *   - six scalar UTF-8 string headers describing the just-appended hop, so
 *     that Flink SQL reports can read them via
 *     {@code CAST(headers['x-isotope-…'] AS STRING)} without needing a UDF
 *     to parse the JSON array. See {@link Isotope#HEADER_TRACE_ID} and
 *     siblings.
 *
 * The isotope is sourced as follows, in order:
 *   1. {@link IsotopeContext#current()} — set by a consumer-then-produce app
 *      via {@link IsotopeContext#adoptFromRecord} before processing.
 *   2. An existing {@value Isotope#HEADER_KEY} header on the outgoing record
 *      — useful when an app builds the record itself with a pre-populated
 *      header but does not use the thread-local.
 *   3. A new trace, stamped with the service name from
 *      {@value #SERVICE_NAME_CONFIG}.
 *
 * Configuration:
 *   isotope.service.name = <string>   (required-in-practice; defaults to "unknown")
 */
public class IsotopeProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    public static final String SERVICE_NAME_CONFIG = "isotope.service.name";

    private static final Logger LOG = LoggerFactory.getLogger(IsotopeProducerInterceptor.class);

    private String serviceName = "unknown";

    @Override
    public void configure(Map<String, ?> configs) {
        Object v = configs.get(SERVICE_NAME_CONFIG);
        if (v instanceof String s && !s.isBlank()) {
            serviceName = s;
        } else {
            LOG.warn("{} not configured; isotope hops will be tagged service=\"unknown\"",
                SERVICE_NAME_CONFIG);
        }
    }

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        Isotope iso = IsotopeContext.current();
        if (iso == null) {
            iso = Isotope.fromHeaders(record.headers());
        }
        if (iso == null) {
            iso = Isotope.newTrace(serviceName);
        }

        long hopTsMs = System.currentTimeMillis();
        iso.appendHop(new Isotope.Hop(serviceName, record.topic(), hopTsMs));

        Headers h = record.headers();
        h.remove(Isotope.HEADER_KEY);
        h.add(Isotope.HEADER_KEY, iso.toJsonBytes());

        // Scalar reporting headers — overwritten on every hop so each record
        // carries the most-recent-hop scalars.
        putString(h, Isotope.HEADER_TRACE_ID,       iso.traceIdHex());
        putString(h, Isotope.HEADER_ORIGIN_TS,      Long.toString(iso.originTsMs()));
        putString(h, Isotope.HEADER_ORIGIN_SERVICE, iso.originService());
        putString(h, Isotope.HEADER_THIS_SERVICE,   serviceName);
        putString(h, Isotope.HEADER_THIS_TOPIC,     record.topic());
        putString(h, Isotope.HEADER_HOP_COUNT,      Integer.toString(iso.hops().size()));

        return record;
    }

    private static void putString(Headers h, String key, String value) {
        h.remove(key);
        h.add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // No-op: the hop is appended in onSend; broker-assigned partition/offset
        // are not threaded back into the header (which has already been sent).
    }

    @Override
    public void close() {}
}
