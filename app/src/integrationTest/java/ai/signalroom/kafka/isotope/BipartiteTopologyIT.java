/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import ai.signalroom.kafka.isotope.proto.DemoEvent;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

/**
 * Exercises the bipartite-topology marker pipeline end-to-end.
 *
 *   svc-A → topic-AB → svc-B → topic-BC → svc-C → topic-CD → svc-D
 *
 * For every consume edge (B↔AB, C↔BC, D↔CD), the consumer calls
 * {@link IsotopeContext#recordConsume} which emits a marker to a
 * per-test {@code iso-consume-events-*} topic. Assertions:
 *   - Exactly three markers land — one per consume edge.
 *   - Trace ID is identical across all three markers (and matches the
 *     trace produced by svc-A on stage 1).
 *   - Each marker carries the forwarded x-isotope-* scalars describing
 *     the UPSTREAM producer (this_service / this_topic) and a new
 *     x-isotope-consumer-service naming the DOWNSTREAM consumer.
 *   - The three (consumer_service, consumed_topic) pairs are exactly
 *     {(svc-B, topic-AB), (svc-C, topic-BC), (svc-D, topic-CD)} — i.e.
 *     the inbound edges of the bipartite graph.
 */
class BipartiteTopologyIT {

    @Test
    void fourStagePipelineEmitsOneMarkerPerConsumeEdge() throws Exception {
        try (Admin admin = IsotopeTestHarness.admin()) {
            String topicAB = IsotopeTestHarness.createUniqueTopic(admin, "iso-bipartite-ab");
            String topicBC = IsotopeTestHarness.createUniqueTopic(admin, "iso-bipartite-bc");
            String topicCD = IsotopeTestHarness.createUniqueTopic(admin, "iso-bipartite-cd");
            String markersTopic =
                IsotopeTestHarness.createUniqueTopic(admin, "iso-bipartite-consume-events");

            try (KafkaProducer<byte[], byte[]> markers = bytesProducer()) {
                IsotopeContext.clear();

                // Stage 1: svc-A produces the trace seed onto topic-AB.
                byte[] traceId;
                try (KafkaProducer<byte[], DemoEvent> prodA = IsotopeTestHarness.producer("svc-A")) {
                    DemoEvent eventA = IsotopeTestHarness.newDemoEvent("svc-A", "stage-1");
                    prodA.send(new ProducerRecord<>(topicAB, "k".getBytes(), eventA)).get();
                }

                // Stage 2: svc-B consumes, marks consume edge, produces to topic-BC.
                try (KafkaConsumer<byte[], DemoEvent> consB =
                         IsotopeTestHarness.consumer("grp-B-" + topicAB)) {
                    consB.subscribe(List.of(topicAB));
                    ConsumerRecords<byte[], DemoEvent> batch = consB.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, batch.count(), "svc-B should see svc-A's record");
                    ConsumerRecord<byte[], DemoEvent> recAtB = batch.iterator().next();
                    IsotopeContext.adoptFromRecord(recAtB);
                    IsotopeContext.recordConsume(recAtB, "svc-B", markers, markersTopic);
                    traceId = IsotopeContext.current().traceId();

                    try (KafkaProducer<byte[], DemoEvent> prodB =
                             IsotopeTestHarness.producer("svc-B")) {
                        prodB.send(new ProducerRecord<>(topicBC, "k".getBytes(),
                            IsotopeTestHarness.newDemoEvent("svc-B", "stage-2"))).get();
                    }
                    IsotopeContext.clear();
                }

                // Stage 3: svc-C consumes, marks consume edge, produces to topic-CD.
                try (KafkaConsumer<byte[], DemoEvent> consC =
                         IsotopeTestHarness.consumer("grp-C-" + topicBC)) {
                    consC.subscribe(List.of(topicBC));
                    ConsumerRecords<byte[], DemoEvent> batch = consC.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, batch.count(), "svc-C should see svc-B's record");
                    ConsumerRecord<byte[], DemoEvent> recAtC = batch.iterator().next();
                    IsotopeContext.adoptFromRecord(recAtC);
                    IsotopeContext.recordConsume(recAtC, "svc-C", markers, markersTopic);

                    try (KafkaProducer<byte[], DemoEvent> prodC =
                             IsotopeTestHarness.producer("svc-C")) {
                        prodC.send(new ProducerRecord<>(topicCD, "k".getBytes(),
                            IsotopeTestHarness.newDemoEvent("svc-C", "stage-3"))).get();
                    }
                    IsotopeContext.clear();
                }

                // Stage 4: svc-D terminal-consumes, marks consume edge, does not produce.
                try (KafkaConsumer<byte[], DemoEvent> consD =
                         IsotopeTestHarness.consumer("grp-D-" + topicCD)) {
                    consD.subscribe(List.of(topicCD));
                    ConsumerRecords<byte[], DemoEvent> batch = consD.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, batch.count(), "svc-D should see svc-C's record");
                    ConsumerRecord<byte[], DemoEvent> recAtD = batch.iterator().next();
                    IsotopeContext.recordConsume(recAtD, "svc-D", markers, markersTopic);
                }

                markers.flush();

                // Read the three markers back and assert their shape.
                List<ConsumerRecord<byte[], byte[]>> seen = new ArrayList<>();
                try (KafkaConsumer<byte[], byte[]> markerReader =
                         bytesConsumer("grp-markers-" + markersTopic)) {
                    markerReader.subscribe(List.of(markersTopic));
                    long deadline = System.currentTimeMillis() + 30_000L;
                    while (seen.size() < 3 && System.currentTimeMillis() < deadline) {
                        ConsumerRecords<byte[], byte[]> batch =
                            markerReader.poll(java.time.Duration.ofSeconds(2));
                        batch.forEach(seen::add);
                    }
                }
                assertEquals(3, seen.size(),
                    "exactly three consume-edge markers should have been emitted");

                String traceIdHex = new Isotope(traceId, 0L, "svc-A",
                    java.util.List.of(), false).traceIdHex();
                Set<String> expectedEdges = new HashSet<>();
                expectedEdges.add("svc-B|" + topicAB);
                expectedEdges.add("svc-C|" + topicBC);
                expectedEdges.add("svc-D|" + topicCD);

                Set<String> actualEdges = new HashSet<>();
                Map<String, String> consumerToProducer = new HashMap<>();
                for (ConsumerRecord<byte[], byte[]> marker : seen) {
                    assertNull(marker.key(),   "marker key should be null");
                    assertNull(marker.value(), "marker value should be null");

                    assertEquals(traceIdHex,
                        IsotopeTestHarness.stringHeader(marker.headers(), Isotope.HEADER_TRACE_ID),
                        "every marker carries the same trace id");
                    assertEquals("svc-A",
                        IsotopeTestHarness.stringHeader(marker.headers(), Isotope.HEADER_ORIGIN_SERVICE));

                    String consumer = IsotopeTestHarness.stringHeader(
                        marker.headers(), Isotope.HEADER_CONSUMER_SERVICE);
                    String consumedTopic = IsotopeTestHarness.stringHeader(
                        marker.headers(), Isotope.HEADER_THIS_TOPIC);
                    String producer = IsotopeTestHarness.stringHeader(
                        marker.headers(), Isotope.HEADER_THIS_SERVICE);

                    assertNotNull(consumer);
                    assertNotNull(consumedTopic);
                    assertNotNull(producer);

                    actualEdges.add(consumer + "|" + consumedTopic);
                    consumerToProducer.put(consumer, producer);
                }
                assertEquals(expectedEdges, actualEdges,
                    "consume edges must be exactly {(B,AB), (C,BC), (D,CD)}");
                assertEquals("svc-A", consumerToProducer.get("svc-B"));
                assertEquals("svc-B", consumerToProducer.get("svc-C"));
                assertEquals("svc-C", consumerToProducer.get("svc-D"));

                // Trace-id byte equality on the first marker for good measure.
                ConsumerRecord<byte[], byte[]> any = seen.get(0);
                assertArrayEquals(traceId,
                    java.util.HexFormat.of().parseHex(
                        IsotopeTestHarness.stringHeader(any.headers(), Isotope.HEADER_TRACE_ID)));
            } finally {
                IsotopeTestHarness.deleteTopic(admin, topicAB);
                IsotopeTestHarness.deleteTopic(admin, topicBC);
                IsotopeTestHarness.deleteTopic(admin, topicCD);
                IsotopeTestHarness.deleteTopic(admin, markersTopic);
                IsotopeContext.clear();
            }
        }
    }

    /** byte[]/byte[] producer with no interceptor — for emitting consume-edge markers. */
    private static KafkaProducer<byte[], byte[]> bytesProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IsotopeTestHarness.BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    /** byte[]/byte[] consumer — for reading consume-edge markers. */
    private static KafkaConsumer<byte[], byte[]> bytesConsumer(String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IsotopeTestHarness.BOOTSTRAP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(p);
    }
}
