/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class BrokerSmokeIT {

    private static final String BOOTSTRAP =
        System.getProperty("kafka.bootstrap", "localhost:30092");

    @Test
    void canConnectAndCreateTopic() throws Exception {
        String topic = "isotope-smoke-" + UUID.randomUUID();

        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000))) {

            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                 .all().get();

            Set<String> topics = admin.listTopics().names().get();
            assertTrue(topics.contains(topic),
                "broker reachable at " + BOOTSTRAP + " did not list freshly-created topic " + topic);

            admin.deleteTopics(List.of(topic)).all().get();
        }
    }
}
