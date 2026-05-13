package com.life360.kafka.isotope;

import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

class ConsumerInterceptorIT {

    @Test
    void adoptFromRecordExtractsIsotopeIntoContext() throws Exception {
        try (Admin admin = IsotopeTestHarness.admin()) {
            String topic = IsotopeTestHarness.createUniqueTopic(admin, "iso-cons");
            try {
                IsotopeContext.clear();

                try (KafkaProducer<byte[], byte[]> producer =
                         IsotopeTestHarness.producer("svc-A")) {
                    producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes()))
                            .get();
                }

                try (KafkaConsumer<byte[], byte[]> consumer =
                         IsotopeTestHarness.consumer("grp-" + topic)) {
                    consumer.subscribe(List.of(topic));
                    ConsumerRecords<byte[], byte[]> records =
                        consumer.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, records.count());

                    // Pre-condition: the consumer interceptor is a no-op for context,
                    // so before adoptFromRecord the thread-local is unset.
                    assertNull(IsotopeContext.current());

                    ConsumerRecord<byte[], byte[]> rec = records.iterator().next();
                    Isotope adopted = IsotopeContext.adoptFromRecord(rec);

                    assertNotNull(adopted);
                    assertEquals("svc-A", adopted.originService());
                    assertEquals(1, adopted.hops().size());
                    assertEquals(topic, adopted.hops().get(0).topic());

                    // The context now holds the same instance.
                    assertNotNull(IsotopeContext.current());
                    assertEquals(adopted.originService(),
                        IsotopeContext.current().originService());
                }
            } finally {
                IsotopeTestHarness.deleteTopic(admin, topic);
                IsotopeContext.clear();
            }
        }
    }

    @Test
    void adoptFromRecordReturnsNullWhenHeaderAbsent() throws Exception {
        try (Admin admin = IsotopeTestHarness.admin()) {
            String topic = IsotopeTestHarness.createUniqueTopic(admin, "iso-untagged");
            try {
                IsotopeContext.set(Isotope.newTrace("stale"));

                try (KafkaProducer<byte[], byte[]> producer =
                         IsotopeTestHarness.bareProducer()) {
                    producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes()))
                            .get();
                }

                try (KafkaConsumer<byte[], byte[]> consumer =
                         IsotopeTestHarness.bareConsumer("grp-" + topic)) {
                    consumer.subscribe(List.of(topic));
                    ConsumerRecords<byte[], byte[]> records =
                        consumer.poll(IsotopeTestHarness.POLL_TIMEOUT);
                    assertEquals(1, records.count());

                    Isotope adopted = IsotopeContext.adoptFromRecord(records.iterator().next());
                    assertNull(adopted);
                    // adoptFromRecord clears stale context when the record has none.
                    assertNull(IsotopeContext.current());
                }
            } finally {
                IsotopeTestHarness.deleteTopic(admin, topic);
                IsotopeContext.clear();
            }
        }
    }
}
