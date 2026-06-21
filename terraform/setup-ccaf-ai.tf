# =============================================================================
# Optional: AI-enriched root-cause analysis for the stuck-trace report (CCAF).
# =============================================================================
# Wires the trace_rca PoC (scripts/flink/sql/ccaf-ai/trace_rca.sql, Option A)
# into the managed Flink report set as `confluent_flink_statement` resources.
#
# DISABLED BY DEFAULT — every resource is gated on `var.enable_trace_rca`
# (count = 0 when false), so a normal `terraform apply` is completely
# unaffected. Turn it on only after setting `rca_model_api_key` (and, for a
# non-OpenAI provider, the matching provider/endpoint/version vars).
#
# It registers a remote text-generation model, then calls it once per
# stuck-trace alert (NOT per record — the source is the low-volume 1-min alert
# topic) to emit an LLM root-cause hypothesis to isotope_report_trace_rca_1m.
# The AI output is a hypothesis on its own topic; it never overwrites the
# deterministic reports.
#
# NOTE: Anthropic is not a direct CCAF provider — for Claude, set
# rca_model_provider = "bedrock" and supply the Bedrock model id + AWS auth
# (Bedrock authenticates via AWS credentials rather than a bare api_key, so the
# WITH block below may need provider-specific keys; see the create-model docs).
# =============================================================================

variable "enable_trace_rca" {
  description = "Enable the optional AI root-cause-analysis Flink statements (model + sink + insert)."
  type        = bool
  default     = false
}

variable "rca_model_provider" {
  description = "CCAF AI Model Inference provider for the RCA model (openai, bedrock, vertexai, azureopenai, googleai, sagemaker, azureml)."
  type        = string
  default     = "openai"
}

variable "rca_model_version" {
  description = "Provider model id/version used for RCA text generation (e.g. gpt-4o, or a Bedrock Claude model id)."
  type        = string
  default     = "gpt-4o"
}

variable "rca_model_endpoint" {
  description = "Provider inference endpoint for the RCA model."
  type        = string
  default     = "https://api.openai.com/v1/chat/completions"
}

variable "rca_model_api_key" {
  description = "API key/secret for the RCA model provider. Required when enable_trace_rca = true."
  type        = string
  default     = ""
  sensitive   = true
}

# 1) Register the remote text-generation model. The provider name prefixes the
#    provider-specific WITH keys; the api key is injected as a session secret.
resource "confluent_flink_statement" "trace_rca_model" {
  count = var.enable_trace_rca ? 1 : 0

  statement = <<-EOT
    CREATE MODEL trace_rca
    INPUT  (`prompt` STRING)
    OUTPUT (`analysis` STRING)
    WITH (
        'provider'                              = '${var.rca_model_provider}',
        'task'                                  = 'text_generation',
        '${var.rca_model_provider}.model_version' = '${var.rca_model_version}',
        '${var.rca_model_provider}.endpoint'      = '${var.rca_model_endpoint}',
        '${var.rca_model_provider}.api_key'       = '{{sessionconfig/sql.secrets.rca_api_key}}'
    );
  EOT

  # Inject the provider key as a statement-scoped secret referenced above.
  properties = merge(local.flink_statement_properties, {
    "sql.secrets.rca_api_key" = var.rca_model_api_key
  })
  rest_endpoint = data.confluent_flink_region.isotope.rest_endpoint

  credentials {
    key    = module.flink_api_key_rotation.active_api_key.id
    secret = module.flink_api_key_rotation.active_api_key.secret
  }

  organization { id = data.confluent_organization.current.id }
  environment { id = confluent_environment.isotope.id }
  principal { id = confluent_service_account.flink_sql_runner.id }
  compute_pool { id = confluent_flink_compute_pool.isotope.id }

  lifecycle {
    ignore_changes = [statement, compute_pool]
  }

  depends_on = [confluent_flink_compute_pool.isotope]
}

# 2) Sink table/topic for the AI report (Protobuf+SR, like the other reports).
resource "confluent_flink_statement" "isotope_report_trace_rca_1m" {
  count = var.enable_trace_rca ? 1 : 0

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS isotope_report_trace_rca_1m (
        `trace_id`     STRING,
        `pipeline`     STRING,
        `last_service` STRING,
        `last_topic`   STRING,
        `stuck_for_ms` BIGINT,
        `root_cause`   STRING
    ) WITH (
        'value.format' = 'proto-registry'
    );
  EOT

  properties    = local.flink_statement_properties
  rest_endpoint = data.confluent_flink_region.isotope.rest_endpoint

  credentials {
    key    = module.flink_api_key_rotation.active_api_key.id
    secret = module.flink_api_key_rotation.active_api_key.secret
  }

  organization { id = data.confluent_organization.current.id }
  environment { id = confluent_environment.isotope.id }
  principal { id = confluent_service_account.flink_sql_runner.id }
  compute_pool { id = confluent_flink_compute_pool.isotope.id }

  lifecycle {
    ignore_changes = [statement, compute_pool]
  }

  depends_on = [confluent_flink_compute_pool.isotope]
}

# 3) One model call per stuck-trace alert -> AI root-cause hypothesis. Reads the
#    existing stuck-trace report topic (low volume), not the source stream.
resource "confluent_flink_statement" "insert_trace_rca" {
  count = var.enable_trace_rca ? 1 : 0

  statement = <<-EOT
    INSERT INTO isotope_report_trace_rca_1m
    SELECT
        a.`trace_id`,
        a.`pipeline`,
        a.`last_service`,
        a.`last_topic`,
        a.`stuck_for_ms`,
        p.`analysis` AS `root_cause`
    FROM isotope_report_stuck_trace_1m AS a,
         LATERAL TABLE(ML_PREDICT('trace_rca',
             'You are an SRE analyzing a Kafka pipeline. A trace is STUCK.'
             || ' pipeline=' || a.`pipeline`
             || ' trace_id=' || a.`trace_id`
             || ' last_seen_at=' || a.`last_service` || ' -> ' || a.`last_topic`
             || ' hop_count=' || CAST(a.`last_hop_count` AS STRING)
             || ' idle_ms=' || CAST(a.`stuck_for_ms` AS STRING)
             || '. Expected path: orders.placed -> orders.enriched ->'
             || ' orders.fulfilled -> shipping-notification-service.'
             || ' In 2 sentences: most likely root cause, and one remediation.'
         )) AS p;
  EOT

  properties    = local.flink_statement_properties
  rest_endpoint = data.confluent_flink_region.isotope.rest_endpoint

  credentials {
    key    = module.flink_api_key_rotation.active_api_key.id
    secret = module.flink_api_key_rotation.active_api_key.secret
  }

  organization { id = data.confluent_organization.current.id }
  environment { id = confluent_environment.isotope.id }
  principal { id = confluent_service_account.flink_sql_runner.id }
  compute_pool { id = confluent_flink_compute_pool.isotope.id }

  lifecycle {
    ignore_changes = [statement, compute_pool]
  }

  depends_on = [
    confluent_flink_statement.trace_rca_model,
    confluent_flink_statement.isotope_report_trace_rca_1m,
    confluent_flink_statement.insert_stuck_trace_alerts,
  ]
}
