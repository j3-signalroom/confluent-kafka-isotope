package com.life360.kafka.isotope;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Consumer-side half of the isotope tracer.
 *
 * Why this is mostly a no-op: {@code onConsume} sees the whole batch returned
 * from {@code poll()}, but the application processes records one at a time.
 * A thread-local that snapshots "the current isotope" from the batch is
 * inherently ambiguous when {@code batch.size() > 1}, so this interceptor
 * deliberately does not mutate {@link IsotopeContext}.
 *
 * Instead, applications call
 * {@link IsotopeContext#adoptFromRecord(ConsumerRecord)} explicitly per
 * record before doing any downstream {@code producer.send()}. This
 * interceptor exists to:
 *   - emit a debug log per polled batch (visibility into trace coverage),
 *   - count records missing the isotope header (drift/coverage signal),
 *   - hold the seam for future enrichment (e.g. SR-resolved schema versions).
 */
public class IsotopeConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(IsotopeConsumerInterceptor.class);

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        if (!LOG.isDebugEnabled() || records.isEmpty()) return records;

        int tagged = 0;
        int missing = 0;
        for (ConsumerRecord<K, V> r : records) {
            if (r.headers().lastHeader(Isotope.HEADER_KEY) != null) tagged++;
            else missing++;
        }
        LOG.debug("polled batch: total={} tagged={} missing={}",
            records.count(), tagged, missing);
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void close() {}
}
