/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import ai.signalroom.kafka.isotope.proto.DemoEvent;

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
 * Demo CLI. Four modes:
 *
 *   send <topic> <service> <payload>
 *      Produces a single SR-framed Protobuf DemoEvent to <topic>, tagged
 *      by IsotopeProducerInterceptor with the trace headers.
 *
 *   hop <in-topic> <out-topic> <service>
 *      Consume-then-produce stage. Adopts the inbound isotope, emits a
 *      consume-edge marker to iso_consume_events, then produces the same
 *      DemoEvent to <out-topic> tagged as <service>.
 *
 *   consume <topic> <service>
 *      Terminal-consumer stage. Subscribes to <topic>, emits a
 *      consume-edge marker to iso_consume_events as <service>, and
 *      pretty-prints the isotope trail. Use this for the final node of
 *      a bipartite-topology demo (svc-D in svc-A → svc-B → svc-C → svc-D).
 *
 *   sink <topic>
 *      Passive peek tool — pretty-prints the isotope trail but does NOT
 *      emit a consume-edge marker. Use for ad-hoc inspection.
 *
 * Reads kafka.bootstrap / schema.registry.url system properties; defaults
 * are wired for the local Minikube setup once `make kafka-pf-up` is up.
 *
 * For Confluent Cloud, also pass:
 *   -Dkafka.security.protocol=SASL_SSL
 *   -Dkafka.sasl.mechanism=PLAIN
 *   -Dkafka.sasl.jaas.config='org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";'
 *   -Dschema.registry.basic.auth.user.info=<sr-key>:<sr-secret>
 * — `scripts/cc-cli-env.sh` builds these from `terraform output`.
 */
public final class App {

    private static final String BOOTSTRAP =
        System.getProperty("kafka.bootstrap", "localhost:30092");
    private static final String SCHEMA_REGISTRY_URL =
        System.getProperty("schema.registry.url", "http://localhost:8081");

    // Optional CCAF / SASL_SSL config. Defaults are blank (the Minikube
    // dev cluster is plaintext-no-auth), so applying these is a no-op
    // unless the user passes the matching -D properties.
    private static final String SECURITY_PROTOCOL =
        System.getProperty("kafka.security.protocol", "PLAINTEXT");
    private static final String SASL_MECHANISM =
        System.getProperty("kafka.sasl.mechanism", "");
    private static final String SASL_JAAS_CONFIG =
        System.getProperty("kafka.sasl.jaas.config", "");

    // Optional Schema Registry basic-auth (CCAF Stream Governance). The
    // Confluent SR serializer/deserializer reads these keys directly off
    // the producer/consumer Properties, so we set them on the same map.
    private static final String SR_BASIC_AUTH_USER_INFO =
        System.getProperty("schema.registry.basic.auth.user.info", "");

    private App() {}

    /**
     * Apply Kafka client security settings (security.protocol +
     * SASL mechanism + JAAS) to a producer/consumer/admin Properties
     * map. No-op when the defaults are unchanged from PLAINTEXT.
     */
    private static void applyKafkaSecurity(Properties p) {
        p.put("security.protocol", SECURITY_PROTOCOL);
        if (!SASL_MECHANISM.isEmpty())   p.put("sasl.mechanism", SASL_MECHANISM);
        if (!SASL_JAAS_CONFIG.isEmpty()) p.put("sasl.jaas.config", SASL_JAAS_CONFIG);
    }

    /**
     * Apply Schema Registry basic-auth credentials to a producer/consumer
     * Properties map (the SR serializer reads them from there). No-op
     * unless -Dschema.registry.basic.auth.user.info=... was passed.
     */
    private static void applySchemaRegistryAuth(Properties p) {
        if (!SR_BASIC_AUTH_USER_INFO.isEmpty()) {
            p.put("basic.auth.credentials.source", "USER_INFO");
            p.put("basic.auth.user.info", SR_BASIC_AUTH_USER_INFO);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) { usage(); System.exit(2); }
        switch (args[0]) {
            case "send"    -> send(args);
            case "hop"     -> hop(args);
            case "consume" -> consume(args);
            case "sink"    -> sink(args);
            default        -> { usage(); System.exit(2); }
        }
    }

    private static void usage() {
        System.err.println("""
            Usage:
              app send    <topic>     <service>   <payload>
              app hop     <in-topic>  <out-topic> <service>
              app consume <topic>     <service>
              app sink    <topic>

            Examples (after `make kafka-pf-up`):
              ./gradlew :app:run --args="send    iso_start svc-A 'hello'"
              ./gradlew :app:run --args="hop     iso_start iso_mid   svc-B"
              ./gradlew :app:run --args="hop     iso_mid   iso_final svc-C"
              ./gradlew :app:run --args="consume iso_final svc-D"
              ./gradlew :app:run --args="sink    iso_final"

            Endpoints (override via -Dkafka.bootstrap=... -Dschema.registry.url=...):
              kafka.bootstrap     = localhost:30092
              schema.registry.url = http://localhost:8081

            Optional Confluent Cloud auth (default: plaintext, no auth):
              -Dkafka.security.protocol=SASL_SSL
              -Dkafka.sasl.mechanism=PLAIN
              -Dkafka.sasl.jaas.config=<JAAS line>
              -Dschema.registry.basic.auth.user.info=<sr-key>:<sr-secret>
            `source scripts/cc-cli-env.sh` exports the right values from
            `terraform output`.
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
        applyKafkaSecurity(p);
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
        applyKafkaSecurity(p);
        applySchemaRegistryAuth(p);

        DemoEvent event = DemoEvent.newBuilder()
            .setEventId(Isotope.uuidV7String())
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

    /**
     * Builds the byte-array Kafka producer used by hop/consume modes to
     * emit consume-edge markers to {@link IsotopeContext#CONSUME_EVENTS_TOPIC}.
     * Deliberately has NO isotope producer interceptor — markers carry the
     * inbound record's scalar headers verbatim plus
     * {@link Isotope#HEADER_CONSUMER_SERVICE}; the interceptor would otherwise
     * append a spurious hop describing the marker emission itself.
     */
    private static KafkaProducer<byte[], byte[]> markerProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Best-effort: don't block the host pipeline on broker acks for markers.
        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        applyKafkaSecurity(p);
        return new KafkaProducer<>(p);
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
        ensureTopic(IsotopeContext.CONSUME_EVENTS_TOPIC);

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
        applyKafkaSecurity(cp);
        applySchemaRegistryAuth(cp);

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        pp.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        pp.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,      true);
        pp.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            IsotopeProducerInterceptor.class.getName());
        pp.put(IsotopeProducerInterceptor.SERVICE_NAME_CONFIG, service);
        applyKafkaSecurity(pp);
        applySchemaRegistryAuth(pp);

        System.out.printf("→ hopping %s → %s as %s (Ctrl-C to stop)%n", inTopic, outTopic, service);

        try (KafkaConsumer<byte[], DemoEvent> consumer = new KafkaConsumer<>(cp);
             KafkaProducer<byte[], DemoEvent> producer = new KafkaProducer<>(pp);
             KafkaProducer<byte[], byte[]>    markers  = markerProducer()) {
            consumer.subscribe(List.of(inTopic));
            while (true) {
                ConsumerRecords<byte[], DemoEvent> batch = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], DemoEvent> rec : batch) {
                    try {
                        IsotopeContext.adoptFromRecord(rec);
                        IsotopeContext.recordConsume(rec, service, markers);
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

    // -- consume --------------------------------------------------------

    /**
     * Terminal-consumer stage. Subscribes to {@code topic}, emits a
     * consume-edge marker to {@link IsotopeContext#CONSUME_EVENTS_TOPIC} as
     * {@code service} for every arriving record, and pretty-prints the
     * isotope trail. Unlike {@link #hop}, does not re-produce the record
     * downstream — this is what makes the consumer "terminal" and the
     * marker the only way it shows up in the bipartite topology graph.
     */
    private static void consume(String[] args) throws Exception {
        if (args.length < 3) { usage(); System.exit(2); }
        String topic   = args[1];
        String service = args[2];

        ensureTopic(topic);
        ensureTopic(IsotopeContext.CONSUME_EVENTS_TOPIC);

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        p.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
            DemoEvent.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "isotope-consume-" + service + "-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        applyKafkaSecurity(p);
        applySchemaRegistryAuth(p);

        System.out.printf("→ consuming %s as %s (Ctrl-C to stop)%n", topic, service);
        try (KafkaConsumer<byte[], DemoEvent> consumer = new KafkaConsumer<>(p);
             KafkaProducer<byte[], byte[]>   markers  = markerProducer()) {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<byte[], DemoEvent> batch = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], DemoEvent> rec : batch) {
                    IsotopeContext.recordConsume(rec, service, markers);
                    print(rec);
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
        applyKafkaSecurity(p);
        applySchemaRegistryAuth(p);

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
