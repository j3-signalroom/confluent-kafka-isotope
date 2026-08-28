# =============================================================================
# Optional: AI-enriched root-cause analysis for the stuck-trace report (CCAF).
# =============================================================================
#
# It registers a remote text-generation model, then calls it once per
# stuck-trace alert (NOT per record — the source is the low-volume 1-min alert
# topic) to emit an LLM root-cause hypothesis to isotope_report_trace_rca_1m.
# The AI output is a hypothesis on its own topic; it never overwrites the
# deterministic reports.
#
# =============================================================================

variable "enable_trace_rca" {
  description = "Enable the optional AI root-cause-analysis Flink statements (model + sink + insert)."
  type        = bool
  default     = false
}

variable "aws_access_key" {
  description = "AWS access key for the Bedrock provider. Required when enable_trace_rca = true and rca_model_provider = bedrock."
  type        = string
  default     = ""
  sensitive   = true
}
variable "aws_secret_key" {
  description = "AWS secret key for the Bedrock provider. Required when enable_trace_rca = true and rca_model_provider = bedrock."
  type        = string
  default     = ""
  sensitive   = true
}
variable "aws_session_token" {
  description = "AWS session token for the Bedrock provider. Required when enable_trace_rca = true and rca_model_provider = bedrock."
  type        = string
  default     = ""
  sensitive   = true
}
variable "rca_model_provider" {
  description = "CCAF AI Model Inference provider for the RCA model (openai, bedrock, vertexai, azureopenai, googleai, sagemaker, azureml)."
  type        = string
  default     = "openai"
}

variable "rca_model_version" {
  description = "Provider model id/version used for RCA text generation (e.g. gpt-4o)."
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

variable "rca_model_system_prompt" {
  description = "Overrides the built-in RCA system prompt (behavioral instructions sent to the LLM once, at model-registration time). Leave empty to use the default in local.rca_default_system_prompt. Not all providers support a system prompt."
  type        = string
  default     = ""
}

locals {
  # The model references its credential by connection name, never inline.
  rca_connection_name = "trace-rca-connection"

  # Bedrock authenticates the Connection with AWS credentials; every other
  # provider uses a single API key. The two are mutually exclusive, so the
  # unused side is set to null (not "") — an empty string is a value the
  # provider would send, whereas null leaves the attribute unset.
  rca_is_bedrock = lower(var.rca_model_provider) == "bedrock"

  # CREATE MODEL WITH keys are prefixed with the provider name, so they have to
  # be built from var.rca_model_provider rather than hard-coded to `openai.*`.
  rca_model_connection_key    = "${var.rca_model_provider}.connection"
  rca_model_version_key       = "${var.rca_model_provider}.model_version"
  rca_model_system_prompt_key = "${var.rca_model_provider}.system_prompt"

  # Behavioral instructions sent once, at CREATE MODEL time. Everything invariant
  # lives here — persona, topology, failure vocabulary, output contract — so the
  # per-alert prompt in `insert_trace_rca` carries only the facts of that trace.
  # Joined into a single line because the value lands inside a single-quoted
  # Flink SQL string literal.
  rca_default_system_prompt = join(" ", [
    "You are a senior SRE performing root-cause analysis on a Kafka event-streaming pipeline.",
    "The pipeline is a linear chain: order-intake-service produces to orders.placed,",
    "order-enrichment-service consumes orders.placed and produces to orders.enriched,",
    "order-fulfillment-service consumes orders.enriched and produces to orders.fulfilled,",
    "and shipping-notification-service consumes orders.fulfilled as the terminal hop.",
    "Each input describes one trace that stopped advancing: the last service and topic",
    "that observed it, how many hops it completed, and how long it has been idle.",
    "Treat the last observed hop as the failure boundary and reason about which",
    "downstream component failed to pick the message up. Consider the usual causes in a",
    "Kafka chain: the downstream consumer being down, crash-looping, or stuck rebalancing;",
    "consumer lag or partition starvation; a poison message failing deserialization;",
    "an unhandled exception in the processing loop; a producer failing to publish the next",
    "event; or partition skew starving one key. Do not propose causes that the reported hop",
    "position cannot support, and do not claim any cause is confirmed. Respond with exactly",
    "two sentences of plain prose: the first states the single most likely root cause, the",
    "second states one concrete remediation an operator can act on immediately. Output only",
    "those two sentences, with no markdown, no bullet points, no preamble, no restating of",
    "the input values, and under 400 characters total.",
  ])

  # Single quotes would terminate the SQL literal early, so double them (SQL escape).
  rca_system_prompt = replace(
    coalesce(var.rca_model_system_prompt, local.rca_default_system_prompt),
  "'", "''")
}

# Models are Kafka-cluster-scoped RBAC resources
# (crn:.../kafka=<lkc>/model=<name>), so the org-level FlinkDeveloper binding is
# NOT enough to run CREATE MODEL — without this the statement fails to provision
# with "Permission denied to CREATE on Model". ResourceOwner covers
# create/drop/describe plus invoking the model from ML_PREDICT, matching how
# topic/group/transactional-id access is granted in setup-confluent-flink.tf.
# Gated on the same flag as the rest of the file, so a normal apply grants
# nothing extra.
resource "confluent_role_binding" "flink_sql_runner_as_resource_owner_model_access" {
  count = var.enable_trace_rca ? 1 : 0

  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.isotope.rbac_crn}/kafka=${confluent_kafka_cluster.isotope.id}/model=*"
}

resource "confluent_flink_connection" "trace_rca" {
  count = var.enable_trace_rca ? 1 : 0

  display_name = local.rca_connection_name
  type         = upper(var.rca_model_provider)
  endpoint     = var.rca_model_endpoint
  api_key      = local.rca_is_bedrock ? null : var.rca_model_api_key

  # AWS SigV4 credentials, Bedrock only. The session token is set only for
  # temporary credentials (STS / SSO / assume-role); long-lived IAM user keys
  # leave it null. Note that temporary credentials expire — the Connection
  # stores what it was given, so a session-token deploy stops authenticating
  # when the token does, and has to be re-applied with a fresh one.
  aws_access_key    = local.rca_is_bedrock ? var.aws_access_key : null
  aws_secret_key    = local.rca_is_bedrock ? var.aws_secret_key : null
  aws_session_token = local.rca_is_bedrock && var.aws_session_token != "" ? var.aws_session_token : null

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
    precondition {
      condition     = !local.rca_is_bedrock || (var.aws_access_key != "" && var.aws_secret_key != "")
      error_message = "rca_model_provider = \"bedrock\" requires aws_access_key, aws_secret_key and aws_session_token. deploy-cc-flink-reports.sh takes them as --aws-access-key / --aws-secret-key / --aws-session-token."
    }
  }

  depends_on = [confluent_flink_compute_pool.isotope]
}

# 1) Register the remote text-generation model. The provider name prefixes the
#    provider-specific WITH keys; auth comes from the Connection above.
resource "confluent_flink_statement" "trace_rca_model" {
  count = var.enable_trace_rca ? 1 : 0

  statement = <<-EOT
    CREATE MODEL trace_rca
    INPUT  (`prompt` STRING)
    OUTPUT (`analysis` STRING)
    WITH (
        'provider'                             = '${var.rca_model_provider}',
        'task'                                 = 'text_generation',
        '${local.rca_model_version_key}'       = '${var.rca_model_version}',
        '${local.rca_model_connection_key}'    = '${local.rca_connection_name}',
        '${local.rca_model_system_prompt_key}' = '${local.rca_system_prompt}'
    );
  EOT

  statement_name = "trace-rca-model"

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
    confluent_flink_compute_pool.isotope,
    confluent_flink_connection.trace_rca,
    confluent_role_binding.flink_sql_runner_as_resource_owner_model_access,
  ]
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
             'pipeline=' || a.`pipeline`
             || ' trace_id=' || a.`trace_id`
             || ' last_service=' || a.`last_service`
             || ' last_topic=' || a.`last_topic`
             || ' hop_count=' || CAST(a.`last_hop_count` AS STRING)
             || ' idle_ms=' || CAST(a.`stuck_for_ms` AS STRING)
         )) AS p;
  EOT

  statement_name = "isotope-report-trace-rca-1m"

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
    # The stuck-trace INSERT this reads from now runs inside the consolidated
    # statement set (setup-confluent-flink.tf), not as its own resource.
    confluent_flink_statement.insert_isotope_reports,
  ]
}
