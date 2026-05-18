resource "confluent_kafka_cluster" "isotope" {
  display_name = local.cluster_display_name
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region
  standard {}

  environment {
    id = confluent_environment.isotope.id
  }
}

# Rotating Kafka API key pair owned by the Flink SQL runner service account.
# The active key is what topic creation + Flink runtime reads/writes use.
module "kafka_api_key_rotation" {
  source = "github.com/j3-signalroom/iac-confluent-api_key_rotation-tf_module"

  owner = {
    id          = confluent_service_account.flink_sql_runner.id
    api_version = confluent_service_account.flink_sql_runner.api_version
    kind        = confluent_service_account.flink_sql_runner.kind
  }

  resource = {
    id          = confluent_kafka_cluster.isotope.id
    api_version = confluent_kafka_cluster.isotope.api_version
    kind        = confluent_kafka_cluster.isotope.kind

    environment = {
      id = confluent_environment.isotope.id
    }
  }

  key_display_name             = "Kafka Service Account API Key - {date} - Managed by Terraform (confluent-kafka-isotope)"
  number_of_api_keys_to_retain = var.number_of_api_keys_to_retain
  day_count                    = var.day_count

  depends_on = [
    confluent_role_binding.flink_sql_runner_as_resource_owner_topic_access
  ]
}

# ---------------------------------------------------------------------------
# Topics — explicit confluent_kafka_topic resources so `terraform destroy`
# tears them down. Three isotope event topics (demo CLI writes DemoEvent
# Protobuf+SR records to these) plus six report sinks (Flink writes
# Protobuf+SR aggregates here).
# ---------------------------------------------------------------------------

resource "confluent_kafka_topic" "isotope_event" {
  for_each = toset(local.isotope_event_topics)

  kafka_cluster {
    id = confluent_kafka_cluster.isotope.id
  }
  topic_name    = each.value
  rest_endpoint = confluent_kafka_cluster.isotope.rest_endpoint

  credentials {
    key    = module.kafka_api_key_rotation.active_api_key.id
    secret = module.kafka_api_key_rotation.active_api_key.secret
  }
}

resource "confluent_kafka_topic" "isotope_report" {
  for_each = toset(local.isotope_report_topics)

  kafka_cluster {
    id = confluent_kafka_cluster.isotope.id
  }
  topic_name    = each.value
  rest_endpoint = confluent_kafka_cluster.isotope.rest_endpoint

  credentials {
    key    = module.kafka_api_key_rotation.active_api_key.id
    secret = module.kafka_api_key_rotation.active_api_key.secret
  }
}
