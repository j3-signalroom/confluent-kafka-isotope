package com.life360.kafka.isotope;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.life360.kafka.isotope.proto.DemoEvent;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Shared helpers for the isotope integration tests. Each test gets its own
 * uniquely-named topics so concurrent test runs don't collide.
 *
 * Value format: SR-framed Protobuf ({@link DemoEvent}). The Schema Registry
 * URL defaults to {@code http://localhost:8081} and can be overridden via
 * {@code -PschemaRegistryUrl=...} on the Gradle command line (wired into
 * the {@code integrationTest} task as a system property).
 */
final class IsotopeTestHarness {

    static final String BOOTSTRAP =
        System.getProperty("kafka.bootstrap", "localhost:30092");
    static final String SCHEMA_REGISTRY_URL =
        System.getProperty("schema.registry.url", "http://localhost:8081");
    static final Duration POLL_TIMEOUT = Duration.ofSeconds(15);

    private IsotopeTestHarness() {}

    /** Returns the UTF-8 decoded value of the given header, or {@code null}. */
    static String stringHeader(Headers hs, String key) {
        Header h = hs.lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** Convenience: build a DemoEvent with a unique event_id. */
    static DemoEvent newDemoEvent(String source, String payload) {
        return DemoEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setSource(source)
            .setCreatedAtMs(System.currentTimeMillis())
            .setPayload(payload)
            .build();
    }

    static Admin admin() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        return Admin.create(p);
    }

    static String createUniqueTopic(Admin admin, String prefix) throws Exception {
        String topic = prefix + "-" + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        return topic;
    }

    static void deleteTopic(Admin admin, String topic) {
        try {
            admin.deleteTopics(List.of(topic)).all().get();
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }

    /** Producer that loads the isotope interceptor and serializes values as SR-framed Protobuf. */
    static KafkaProducer<byte[], DemoEvent> producer(String serviceName) {
        Properties p = baseProducerProps();
        p.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            IsotopeProducerInterceptor.class.getName());
        p.put(IsotopeProducerInterceptor.SERVICE_NAME_CONFIG, serviceName);
        return new KafkaProducer<>(p);
    }

    /** Producer with no interceptor — used to verify what untagged traffic looks like. */
    static KafkaProducer<byte[], DemoEvent> bareProducer() {
        return new KafkaProducer<>(baseProducerProps());
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        // Auto-register the DemoEvent schema on first send. Fine for the
        // demo; production setups typically pre-register schemas via CI.
        p.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        return p;
    }

    /** Consumer with no interceptor — keeps assertions independent of consumer-side code. */
    static KafkaConsumer<byte[], DemoEvent> bareConsumer(String groupId) {
        return new KafkaConsumer<>(baseConsumerProps(groupId));
    }

    /** Consumer that loads the isotope interceptor. */
    static KafkaConsumer<byte[], DemoEvent> consumer(String groupId) {
        Properties p = baseConsumerProps(groupId);
        p.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            IsotopeConsumerInterceptor.class.getName());
        return new KafkaConsumer<>(p);
    }

    private static Properties baseConsumerProps(String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        // Tell the deserializer the concrete generated class so it returns
        // typed DemoEvent records instead of DynamicMessage.
        p.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
            DemoEvent.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return p;
    }
}
