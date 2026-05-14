package com.life360.kafka.isotope;

import com.life360.kafka.isotope.proto.DemoEvent;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Demo CLI. Two modes:
 *
 *   send <topic> <service> <payload>
 *      Produces a single SR-framed Protobuf DemoEvent to <topic>, tagged
 *      by IsotopeProducerInterceptor with the trace headers.
 *
 *   sink <topic>
 *      Subscribes to <topic> and pretty-prints the isotope trail for
 *      every arriving record, until interrupted.
 *
 * Reads kafka.bootstrap / schema.registry.url system properties; defaults
 * are wired for the local Minikube setup once `make kafka-pf-up` is up.
 */
public final class App {

    private static final String BOOTSTRAP =
        System.getProperty("kafka.bootstrap", "localhost:30092");
    private static final String SCHEMA_REGISTRY_URL =
        System.getProperty("schema.registry.url", "http://localhost:8081");

    private App() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) { usage(); System.exit(2); }
        switch (args[0]) {
            case "send" -> send(args);
            case "hop"  -> hop(args);
            case "sink" -> sink(args);
            default     -> { usage(); System.exit(2); }
        }
    }

    private static void usage() {
        System.err.println("""
            Usage:
              app send <topic>     <service> <payload>
              app hop  <in-topic>  <out-topic> <service>
              app sink <topic>

            Examples (after `make kafka-pf-up`):
              ./gradlew :app:run --args="send iso-start svc-A 'hello'"
              ./gradlew :app:run --args="hop  iso-start iso-mid   svc-B"
              ./gradlew :app:run --args="hop  iso-mid   iso-final svc-C"
              ./gradlew :app:run --args="sink iso-final"

            Endpoints (override via -Dkafka.bootstrap=... -Dschema.registry.url=...):
              kafka.bootstrap     = localhost:30092
              schema.registry.url = http://localhost:8081
            """);
    }

    // -- helpers --------------------------------------------------------

    /**
     * Creates the topic if it doesn't exist (1 partition, replication
     * factor 1 — matches the single-broker dev cluster). Idempotent: if
     * the topic already exists, returns silently.
     */
    private static void ensureTopic(String topic) throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        try (Admin admin = Admin.create(p)) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
                System.out.printf("✔ created topic %s%n", topic);
            } catch (java.util.concurrent.ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                    return; // already there — fine
                }
                throw e;
            }
        }
    }

    // -- send -----------------------------------------------------------

    private static void send(String[] args) throws Exception {
        if (args.length < 4) { usage(); System.exit(2); }
        String topic   = args[1];
        String service = args[2];
        String payload = args[3];

        ensureTopic(topic);

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        p.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,      true);
        p.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            IsotopeProducerInterceptor.class.getName());
        p.put(IsotopeProducerInterceptor.SERVICE_NAME_CONFIG, service);

        DemoEvent event = DemoEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setSource(service)
            .setCreatedAtMs(System.currentTimeMillis())
            .setPayload(payload)
            .build();

        try (KafkaProducer<byte[], DemoEvent> producer = new KafkaProducer<>(p)) {
            RecordMetadata md = producer
                .send(new ProducerRecord<>(topic, service.getBytes(), event))
                .get();
            System.out.printf("✔ sent  event_id=%s%n  service=%s  →  %s-%d@%d%n",
                event.getEventId(), service, md.topic(), md.partition(), md.offset());
        }
    }

    // -- hop ------------------------------------------------------------

    /**
     * Consume-then-produce stage. Subscribes to {@code in}, and for every
     * arriving record:
     *   1. Calls {@link IsotopeContext#adoptFromRecord} so the trace
     *      context is in the thread-local before producing.
     *   2. Forwards the same {@link DemoEvent} value to {@code out} with
     *      {@code service} as the producer's isotope service name.
     *
     * The producer interceptor reads {@code IsotopeContext.current()} and
     * appends a new hop {service: <service>, topic: <out>, …} to the
     * isotope's hop list, so traces grow as records flow through chained
     * {@code hop} stages. Runs until Ctrl-C.
     */
    private static void hop(String[] args) throws Exception {
        if (args.length < 4) { usage(); System.exit(2); }
        String inTopic  = args[1];
        String outTopic = args[2];
        String service  = args[3];

        ensureTopic(inTopic);
        ensureTopic(outTopic);

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   ByteArrayDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        cp.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        cp.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
            DemoEvent.class.getName());
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, "isotope-hop-" + service + "-" + UUID.randomUUID());
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        pp.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        pp.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,      true);
        pp.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            IsotopeProducerInterceptor.class.getName());
        pp.put(IsotopeProducerInterceptor.SERVICE_NAME_CONFIG, service);

        System.out.printf("→ hopping %s → %s as %s (Ctrl-C to stop)%n", inTopic, outTopic, service);

        try (KafkaConsumer<byte[], DemoEvent> consumer = new KafkaConsumer<>(cp);
             KafkaProducer<byte[], DemoEvent> producer = new KafkaProducer<>(pp)) {
            consumer.subscribe(List.of(inTopic));
            while (true) {
                ConsumerRecords<byte[], DemoEvent> batch = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], DemoEvent> rec : batch) {
                    try {
                        IsotopeContext.adoptFromRecord(rec);
                        RecordMetadata md = producer
                            .send(new ProducerRecord<>(outTopic, rec.key(), rec.value()))
                            .get();
                        System.out.printf("  %s@%d → %s@%d%n",
                            inTopic, rec.offset(), outTopic, md.offset());
                    } finally {
                        IsotopeContext.clear();
                    }
                }
            }
        }
    }

    // -- sink -----------------------------------------------------------

    private static void sink(String[] args) {
        if (args.length < 2) { usage(); System.exit(2); }
        String topic = args[1];

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        p.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
            DemoEvent.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "isotope-sink-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        System.out.printf("→ sinking %s (Ctrl-C to stop)%n", topic);
        try (KafkaConsumer<byte[], DemoEvent> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<byte[], DemoEvent> batch = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], DemoEvent> rec : batch) {
                    print(rec);
                }
            }
        }
    }

    private static void print(ConsumerRecord<byte[], DemoEvent> rec) {
        Isotope iso = Isotope.fromHeaders(rec.headers());
        DemoEvent v = rec.value();

        System.out.printf("%n[%s @ partition=%d offset=%d]%n",
            rec.topic(), rec.partition(), rec.offset());
        System.out.printf("  event_id : %s%n", v.getEventId());
        System.out.printf("  payload  : %s%n", v.getPayload());

        if (iso == null) {
            System.out.println("  isotope  : (no x-isotope header — untagged record)");
            return;
        }

        long latencyMs = System.currentTimeMillis() - iso.originTsMs();
        System.out.printf("  trace_id : %s%n", iso.traceIdHex());
        System.out.printf("  origin   : %s @ %d (now %dms ago)%n",
            iso.originService(), iso.originTsMs(), latencyMs);
        System.out.printf("  hops     : %d%s%n", iso.hops().size(),
            iso.truncated() ? " (truncated — older hops evicted)" : "");
        for (int i = 0; i < iso.hops().size(); i++) {
            Isotope.Hop h = iso.hops().get(i);
            System.out.printf("    %d. %-12s → %-30s @ %d%n",
                i + 1, h.service(), h.topic(), h.tsMs());
        }
    }
}
