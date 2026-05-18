data "confluent_organization" "current" {}

# Schema Registry for the environment created in setup-confluent-environment.tf.
data "confluent_schema_registry_cluster" "isotope" {
  environment {
    id = confluent_environment.isotope.id
  }

  depends_on = [
    confluent_kafka_cluster.isotope
  ]
}

# Flink region (provider data source for compute-pool/statement endpoints).
data "confluent_flink_region" "isotope" {
  cloud  = var.cloud
  region = var.region
}

locals {
  # Display names used as catalog/database in confluent_flink_statement.properties.
  env_display_name     = "confluent-kafka-isotope"
  cluster_display_name = "kafka-isotope"

  # Canonical isotope event topics. The demo CLI writes DemoEvent (SR-Protobuf)
  # to these; the source view in flink/sql/cc/00_source_table.fql UNIONs them.
  isotope_event_topics = [
    "iso-start",
    "iso-mid",
    "iso-final",
  ]

  # Sink topics for the six Flink SQL reports. Names mirror CP's sink topic
  # names so the shared shared/{10,20,30,40,60,70}_*.fql INSERTs target the
  # same logical table names.
  isotope_report_topics = [
    "isotope-report-latency-1m",
    "isotope-report-topology-1m",
    "isotope-report-hop-distribution-1m",
    "isotope-report-coverage-1m",
    "isotope-report-stuck-trace-1m",
    "isotope-report-latency-percentiles-1m",
  ]

  # Path constants used across the setup-* files.
  shared_sql_dir = "${path.module}/../flink/sql/shared"
  cc_sql_dir     = "${path.module}/../flink/sql/cc"

  # Shared INSERT files use `SET 'pipeline.name' = '...';` so the CP sql-client
  # can name jobs. CCAF rejects SET in submitted statements (it's a SQL-Client
  # directive, not a Flink SQL statement). Strip it before submission; the
  # job name on CCAF comes from the resource's `statement_name` field instead.
  set_pipeline_name_regex = "/SET 'pipeline\\.name' = '[^']+';\\s*/"
}
