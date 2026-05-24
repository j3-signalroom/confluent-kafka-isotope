/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.util.List;

import ai.signalroom.kafka.isotope.proto.DemoEvent;
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
 *   order-intake-service → topic-AB → order-enrichment-service → topic-BC → order-fulfillment-service
 *
 * Assertions:
 *   - The trace ID is constant across all three stages.
 *   - The originService stays "order-intake-service" (set on the first hop) even at order-fulfillment-service.
 *   - At order-fulfillment-service the isotope carries exactly two hops:
 *       hop[0] = {service: order-intake-service, topic: topic-AB}
 *       hop[1] = {service: order-enrichment-service, topic: topic-BC}
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

                // Stage 1: order-intake-service produces to topic-AB.
                try (KafkaProducer<byte[], DemoEvent> prodA = IsotopeTestHarness.producer("order-intake-service")) {
                    DemoEvent eventA = IsotopeTestHarness.newDemoEvent("order-intake-service", "stage-1");
                    prodA.send(new ProducerRecord<>(topicAB, "k".getBytes(), eventA)).get();
                }

                // Stage 2: order-enrichment-service consumes topic-AB, adopts the isotope, produces to topic-BC.
                ConsumerRecord<byte[], DemoEvent> recAtB;
                try (KafkaConsumer<byte[], DemoEvent> consB =
                         IsotopeTestHarness.consumer("grp-B-" + topicAB)) {
                    consB.subscribe(List.of(topicAB));
                    ConsumerRecords<byte[], DemoEvent> batch = consB.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, batch.count(),
                        "order-enrichment-service should see exactly the one record order-intake-service produced");
                    recAtB = batch.iterator().next();
                    IsotopeContext.adoptFromRecord(recAtB);
                }

                Isotope isoAtB = IsotopeContext.current();
                assertNotNull(isoAtB, "order-enrichment-service should have adopted the isotope from topic-AB");
                assertEquals(1, isoAtB.hops().size(), "after stage 1, only one hop should exist");
                byte[] traceId = isoAtB.traceId();

                try (KafkaProducer<byte[], DemoEvent> prodB = IsotopeTestHarness.producer("order-enrichment-service")) {
                    DemoEvent eventB = IsotopeTestHarness.newDemoEvent("order-enrichment-service", "stage-2");
                    prodB.send(new ProducerRecord<>(topicBC, "k".getBytes(), eventB)).get();
                }
                IsotopeContext.clear();

                // Stage 3: order-fulfillment-service consumes topic-BC and inspects the isotope.
                Isotope isoAtC;
                try (KafkaConsumer<byte[], DemoEvent> consC =
                         IsotopeTestHarness.consumer("grp-C-" + topicBC)) {
                    consC.subscribe(List.of(topicBC));
                    ConsumerRecords<byte[], DemoEvent> batch = consC.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, batch.count(),
                        "order-fulfillment-service should see exactly the one record order-enrichment-service produced");

                    ConsumerRecord<byte[], DemoEvent> recAtC = batch.iterator().next();
                    Headers hs = recAtC.headers();
                    Header h = hs.lastHeader(Isotope.HEADER_KEY);
                    assertNotNull(h);
                    isoAtC = Isotope.fromJsonBytes(h.value());

                    // Scalar reporting headers carry the *current* hop's view:
                    // origin remains order-intake-service, but this-service / this-topic reflect
                    // the producer that wrote this very record (order-enrichment-service → topic-BC).
                    assertEquals(isoAtC.traceIdHex(),
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_TRACE_ID));
                    assertEquals("order-intake-service",
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_ORIGIN_SERVICE));
                    assertEquals("order-enrichment-service",
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_THIS_SERVICE));
                    assertEquals(topicBC,
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_THIS_TOPIC));
                    assertEquals("2",
                        IsotopeTestHarness.stringHeader(hs, Isotope.HEADER_HOP_COUNT));
                }

                // Trace identity preserved.
                assertArrayEquals(traceId, isoAtC.traceId(),
                    "trace id must be stable across hops");
                assertEquals("order-intake-service", isoAtC.originService(),
                    "originService is set once at trace creation and never reassigned");

                // Hops are ordered: A→AB then B→BC.
                assertEquals(2, isoAtC.hops().size());
                Isotope.Hop hop0 = isoAtC.hops().get(0);
                Isotope.Hop hop1 = isoAtC.hops().get(1);

                assertEquals("order-intake-service", hop0.service());
                assertEquals(topicAB, hop0.topic());

                assertEquals("order-enrichment-service", hop1.service());
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
