output "environment_id" {
  description = "Confluent Cloud environment ID hosting the isotope resources."
  value       = confluent_environment.isotope.id
}

output "kafka_cluster_id" {
  description = "Kafka cluster ID."
  value       = confluent_kafka_cluster.isotope.id
}

output "kafka_bootstrap_servers" {
  description = "Bootstrap endpoint to use as -Dkafka.bootstrap=... for the demo CLI."
  value       = confluent_kafka_cluster.isotope.bootstrap_endpoint
}

output "kafka_api_key" {
  description = "Active Kafka API key (rotating) for the Flink SQL runner service account. Reuse for the demo CLI producer."
  value       = module.kafka_api_key_rotation.active_api_key.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Active Kafka API secret. Pair with kafka_api_key in the demo CLI's SASL config."
  value       = module.kafka_api_key_rotation.active_api_key.secret
  sensitive   = true
}

output "schema_registry_url" {
  description = "Schema Registry REST endpoint to use as -Dschema.registry.url=... for the demo CLI."
  value       = data.confluent_schema_registry_cluster.isotope.rest_endpoint
}

output "compute_pool_id" {
  description = "Flink compute pool ID running the report INSERTs."
  value       = confluent_flink_compute_pool.isotope.id
}

output "artifact_id" {
  description = "Flink artifact ID for the PTF/UDAF JAR (referenced by the CREATE FUNCTION statements)."
  value       = confluent_flink_artifact.isotope_udf.id
}
