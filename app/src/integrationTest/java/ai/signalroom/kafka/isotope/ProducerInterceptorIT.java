/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import ai.signalroom.kafka.isotope.proto.DemoEvent;

class ProducerInterceptorIT {

    @Test
    void producerStampsIsotopeHeaderOnSend() throws Exception {
        try (Admin admin = IsotopeTestHarness.admin()) {
            String topic = IsotopeTestHarness.createUniqueTopic(admin, "iso-prod");
            try {
                IsotopeContext.clear();
                long beforeSend = System.currentTimeMillis();

                try (KafkaProducer<byte[], DemoEvent> producer =
                         IsotopeTestHarness.producer("origin-service")) {
                    DemoEvent event = IsotopeTestHarness.newDemoEvent("origin-service", "hello");
                    producer.send(new ProducerRecord<>(topic, "key".getBytes(), event)).get();
                }

                try (KafkaConsumer<byte[], DemoEvent> consumer =
                         IsotopeTestHarness.bareConsumer("grp-" + topic)) {
                    consumer.subscribe(List.of(topic));
                    ConsumerRecords<byte[], DemoEvent> records =
                        consumer.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, records.count(),
                        "expected exactly one record from " + topic);

                    ConsumerRecord<byte[], DemoEvent> rec = records.iterator().next();
                    DemoEvent decoded = rec.value();
                    assertNotNull(decoded, "Protobuf-decoded value must not be null");
                    assertEquals("origin-service", decoded.getSource());
                    assertEquals("hello",          decoded.getPayload());
                    Headers hs = rec.headers();
                    Header h = hs.lastHeader(Isotope.HEADER_KEY);
                    assertNotNull(h, "record must carry the " + Isotope.HEADER_KEY + " header");

                    Isotope iso = Isotope.fromJsonBytes(h.value());
                    assertEquals("origin-service", iso.originService());
                    assertTrue(iso.originTsMs() >= beforeSend,
                        "originTsMs " + iso.originTsMs() + " must be >= test start " + beforeSend);
                    assertEquals(16, iso.traceId().length);
                    assertEquals(1, iso.hops().size(),
                        "first-hop trace should have exactly one hop");
                    assertFalse(iso.truncated());

                    Isotope.Hop hop = iso.hops().get(0);
                    assertEquals("origin-service", hop.service());
                    assertEquals(topic, hop.topic());

                    // Scalar reporting headers — these are what the Flink SQL
                    // reports read, so each one must match the JSON payload.
                    assertEquals(iso.traceIdHex(),                IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_TRACE_ID));
                    assertEquals(Long.toString(iso.originTsMs()), IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_ORIGIN_TS));
                    assertEquals("origin-service",                IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_ORIGIN_SERVICE));
                    assertEquals("origin-service",                IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_THIS_SERVICE));
                    assertEquals(topic,                           IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_THIS_TOPIC));
                    assertEquals("1",                             IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_HOP_COUNT));
                }
            } finally {
                IsotopeTestHarness.deleteTopic(admin, topic);
                IsotopeContext.clear();
            }
        }
    }
}
