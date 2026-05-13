package com.life360.kafka.isotope;

import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

/**
 * Exercises the full producer→consumer→producer→consumer chain.
 *
 *   svc-A → topic-AB → svc-B → topic-BC → svc-C
 *
 * Assertions:
 *   - The trace ID is constant across all three stages.
 *   - The originService stays "svc-A" (set on the first hop) even at svc-C.
 *   - At svc-C the isotope carries exactly two hops:
 *       hop[0] = {service: svc-A, topic: topic-AB}
 *       hop[1] = {service: svc-B, topic: topic-BC}
 *   - Hop timestamps are monotonically non-decreasing.
 */
class ThreeStageHopPropagationIT {

    @Test
    void threeStagePipelineProducesTwoOrderedHops() throws Exception {
        try (Admin admin = IsotopeTestHarness.admin()) {
            String topicAB = IsotopeTestHarness.createUniqueTopic(admin, "iso-3stage-ab");
            String topicBC = IsotopeTestHarness.createUniqueTopic(admin, "iso-3stage-bc");

            try {
                IsotopeContext.clear();

                // Stage 1: svc-A produces to topic-AB.
                try (KafkaProducer<byte[], byte[]> prodA = IsotopeTestHarness.producer("svc-A")) {
                    prodA.send(new ProducerRecord<>(topicAB, "k".getBytes(), "v".getBytes())).get();
                }

                // Stage 2: svc-B consumes topic-AB, adopts the isotope, produces to topic-BC.
                ConsumerRecord<byte[], byte[]> recAtB;
                try (KafkaConsumer<byte[], byte[]> consB =
                         IsotopeTestHarness.consumer("grp-B-" + topicAB)) {
                    consB.subscribe(List.of(topicAB));
                    ConsumerRecords<byte[], byte[]> batch = consB.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, batch.count(),
                        "svc-B should see exactly the one record svc-A produced");
                    recAtB = batch.iterator().next();
                    IsotopeContext.adoptFromRecord(recAtB);
                }

                Isotope isoAtB = IsotopeContext.current();
                assertNotNull(isoAtB, "svc-B should have adopted the isotope from topic-AB");
                assertEquals(1, isoAtB.hops().size(), "after stage 1, only one hop should exist");
                byte[] traceId = isoAtB.traceId();

                try (KafkaProducer<byte[], byte[]> prodB = IsotopeTestHarness.producer("svc-B")) {
                    prodB.send(new ProducerRecord<>(topicBC, "k".getBytes(), "v".getBytes())).get();
                }
                IsotopeContext.clear();

                // Stage 3: svc-C consumes topic-BC and inspects the isotope.
                Isotope isoAtC;
                try (KafkaConsumer<byte[], byte[]> consC =
                         IsotopeTestHarness.consumer("grp-C-" + topicBC)) {
                    consC.subscribe(List.of(topicBC));
                    ConsumerRecords<byte[], byte[]> batch = consC.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, batch.count(),
                        "svc-C should see exactly the one record svc-B produced");

                    ConsumerRecord<byte[], byte[]> recAtC = batch.iterator().next();
                    Headers hs = recAtC.headers();
                    Header h = hs.lastHeader(Isotope.HEADER_KEY);
                    assertNotNull(h);
                    isoAtC = Isotope.fromJsonBytes(h.value());

                    // Scalar reporting headers carry the *current* hop's view:
                    // origin remains svc-A, but this-service / this-topic reflect
                    // the producer that wrote this very record (svc-B → topic-BC).
                    assertEquals(isoAtC.traceIdHex(),
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_TRACE_ID));
                    assertEquals("svc-A",
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_ORIGIN_SERVICE));
                    assertEquals("svc-B",
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_THIS_SERVICE));
                    assertEquals(topicBC,
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_THIS_TOPIC));
                    assertEquals("2",
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_HOP_COUNT));
                }

                // Trace identity preserved.
                assertArrayEquals(traceId, isoAtC.traceId(),
                    "trace id must be stable across hops");
                assertEquals("svc-A", isoAtC.originService(),
                    "originService is set once at trace creation and never reassigned");

                // Hops are ordered: A→AB then B→BC.
                assertEquals(2, isoAtC.hops().size());
                Isotope.Hop hop0 = isoAtC.hops().get(0);
                Isotope.Hop hop1 = isoAtC.hops().get(1);

                assertEquals("svc-A", hop0.service());
                assertEquals(topicAB, hop0.topic());

                assertEquals("svc-B", hop1.service());
                assertEquals(topicBC, hop1.topic());

                // Latency-sanity: hop timestamps non-decreasing.
                assert hop0.tsMs() <= hop1.tsMs()
                    : "hop timestamps should be monotonically non-decreasing: " + hop0.tsMs()
                        + " vs " + hop1.tsMs();
            } finally {
                IsotopeTestHarness.deleteTopic(admin, topicAB);
                IsotopeTestHarness.deleteTopic(admin, topicBC);
                IsotopeContext.clear();
            }
        }
    }
}
