/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer-side half of the isotope tracer.
 *
 * <p>On every {@code send()}, this interceptor finds (or creates) the in-flight
 * {@link Isotope}, appends a hop describing this produce edge, and writes the
 * JSON-encoded isotope plus six scalar headers describing the just-appended
 * hop. Sourcing order: thread-local context, inbound header, or a fresh trace
 * stamped with {@value #SERVICE_NAME_CONFIG}.
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
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
        Isotope iso = IsotopeContext.current();
        if (iso == null) {
            iso = Isotope.fromHeaders(producerRecord.headers());
        }
        if (iso == null) {
            iso = Isotope.newTrace(serviceName);
        }

        long hopTsMs = System.currentTimeMillis();
        iso.appendHop(new Isotope.Hop(serviceName, producerRecord.topic(), hopTsMs));

        Headers h = producerRecord.headers();
        h.remove(Isotope.HEADER_KEY);
        h.add(Isotope.HEADER_KEY, iso.toJsonBytes());

        // Scalar reporting headers - overwritten on every hop so each record
        // carries the most-recent-hop scalars.
        putString(h, Isotope.HEADER_TRACE_ID,       iso.traceIdHex());
        putString(h, Isotope.HEADER_ORIGIN_TS,      Long.toString(iso.originTsMs()));
        putString(h, Isotope.HEADER_ORIGIN_SERVICE, iso.originService());
        putString(h, Isotope.HEADER_THIS_SERVICE,   serviceName);
        putString(h, Isotope.HEADER_THIS_TOPIC,     producerRecord.topic());
        putString(h, Isotope.HEADER_HOP_COUNT,      Integer.toString(iso.hops().size()));

        return producerRecord;
    }

    private static void putString(Headers h, String key, String value) {
        h.remove(key);
        h.add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // No-op: hop is appended in onSend; broker-assigned partition/offset
        // are not threaded back into the header.
    }

    @Override
    public void close() {
        // This method is intentionally empty because this interceptor holds
        // no resources requiring cleanup.  Furthermore, it is here only to
        // satisfy the interface. 
    }
}