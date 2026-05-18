# =====================================================================
# Service account, role bindings, compute pool, artifact, statements.
#
# Note on file split: Terraform doesn't care about file boundaries — all
# resources here are evaluated in one module graph alongside everything
# in setup-confluent-environment.tf, setup-confluent-kafka.tf, data.tf,
# variables.tf. The split is purely for human readability.
# =====================================================================

resource "confluent_service_account" "flink_sql_runner" {
  display_name = "isotope-flink-sql-runner"
  description  = "Service account for executing the isotope Flink SQL reports on CCAF."
}

resource "confluent_role_binding" "flink_sql_runner_as_flink_developer" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = data.confluent_organization.current.resource_name

  depends_on = [
    confluent_service_account.flink_sql_runner
  ]
}

resource "confluent_role_binding" "flink_sql_runner_as_resource_owner_topic_access" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.isotope.rbac_crn}/kafka=${confluent_kafka_cluster.isotope.id}/topic=*"

  depends_on = [
    confluent_role_binding.flink_sql_runner_as_flink_developer
  ]
}

resource "confluent_role_binding" "flink_sql_runner_as_assigner" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "Assigner"
  crn_pattern = "${data.confluent_organization.current.resource_name}/service-account=${confluent_service_account.flink_sql_runner.id}"

  depends_on = [
    confluent_role_binding.flink_sql_runner_as_resource_owner_topic_access
  ]
}

resource "confluent_role_binding" "flink_sql_runner_schema_registry_access" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.isotope.resource_name}/subject=*"

  depends_on = [
    confluent_role_binding.flink_sql_runner_as_assigner
  ]
}

resource "confluent_role_binding" "flink_sql_runner_as_resource_owner_transactional_access" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.isotope.rbac_crn}/kafka=${confluent_kafka_cluster.isotope.id}/transactional-id=*"

  depends_on = [
    confluent_role_binding.flink_sql_runner_schema_registry_access
  ]
}

# ---------------------------------------------------------------------------
# Compute pool — single small pool runs all the report INSERTs plus the
# six DDL CREATEs. 10 CFU is comfortable for the demo (each INSERT is
# parallelism 1, six in flight + headroom). Increase max_cfu if you scale
# the demo CLI's send rate or open many ad-hoc SELECTs concurrently.
# ---------------------------------------------------------------------------

resource "confluent_flink_compute_pool" "isotope" {
  display_name = "isotope-flink-statement-runner"
  cloud        = var.cloud
  region       = var.region
  max_cfu      = 10

  environment {
    id = confluent_environment.isotope.id
  }

  depends_on = [
    confluent_role_binding.flink_sql_runner_as_resource_owner_transactional_access
  ]
}

# Rotating Flink API key pair owned by the same service account. Used by
# every confluent_flink_statement resource's `credentials {}` block to
# authenticate against the Flink REST endpoint.
module "flink_api_key_rotation" {
  source = "github.com/j3-signalroom/iac-confluent-api_key_rotation-tf_module"

  owner = {
    id          = confluent_service_account.flink_sql_runner.id
    api_version = confluent_service_account.flink_sql_runner.api_version
    kind        = confluent_service_account.flink_sql_runner.kind
  }

  resource = {
    id          = data.confluent_flink_region.isotope.id
    api_version = data.confluent_flink_region.isotope.api_version
    kind        = data.confluent_flink_region.isotope.kind

    environment = {
      id = confluent_environment.isotope.id
    }
  }

  key_display_name             = "Flink Service Account API Key - {date} - Managed by Terraform (confluent-kafka-isotope)"
  number_of_api_keys_to_retain = var.number_of_api_keys_to_retain
  day_count                    = var.day_count
}

# ---------------------------------------------------------------------------
# Common properties bag — every `confluent_flink_statement` below sets
# `sql.current-catalog` = environment display name and `sql.current-database`
# = Kafka cluster display name, so unqualified table/view references resolve.
# ---------------------------------------------------------------------------

locals {
  flink_statement_properties = {
    "sql.current-catalog"  = confluent_environment.isotope.display_name
    "sql.current-database" = confluent_kafka_cluster.isotope.display_name
  }
}

# ---------------------------------------------------------------------------
# Flink artifact — uploads ptf/build/libs/isotope-flink-udf.jar to CCAF.
# Build it locally first with `./gradlew :ptf:shadowJar` (the wrapper
# script `scripts/deploy-cc-flink-reports.sh` handles this for you).
#
# The artifact ID is interpolated into the CREATE FUNCTION statements
# below via `confluent-artifact://${confluent_flink_artifact.isotope_udf.id}`.
# ---------------------------------------------------------------------------

resource "confluent_flink_artifact" "isotope_udf" {
  display_name   = "isotope-flink-udf"
  content_format = "JAR"
  cloud          = var.cloud
  region         = var.region
  artifact_file  = "${path.module}/${var.artifact_jar_path}"

  environment {
    id = confluent_environment.isotope.id
  }
}

# ---------------------------------------------------------------------------
# Statement 1 — `isotope_raw` view over the three iso-* topics.
# Templated from flink/sql/cc/00_source_table.fql (file is the source of
# truth so the same DDL works for ad-hoc Cloud Console submission too).
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "isotope_raw_view" {
  statement = file("${local.cc_sql_dir}/00_source_table.fql")

  properties    = local.flink_statement_properties
  rest_endpoint = data.confluent_flink_region.isotope.rest_endpoint

  credentials {
    key    = module.flink_api_key_rotation.active_api_key.id
    secret = module.flink_api_key_rotation.active_api_key.secret
  }

  organization {
    id = data.confluent_organization.current.id
  }

  environment {
    id = confluent_environment.isotope.id
  }

  principal {
    id = confluent_service_account.flink_sql_runner.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.isotope.id
  }

  lifecycle {
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_kafka_topic.isotope_event,
    confluent_flink_compute_pool.isotope,
  ]
}

# ---------------------------------------------------------------------------
# Statement 2 — `isotope` typed view (header decoder + latency).
# Templated from flink/sql/shared/05_isotope_view.fql (identical on CP and
# CC; the shared/ directory is the canonical source of truth).
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "isotope_view" {
  statement = file("${local.shared_sql_dir}/05_isotope_view.fql")

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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_statement.isotope_raw_view,
  ]
}

# ---------------------------------------------------------------------------
# Statements 3-8 — 6 sink tables (CCAF Protobuf+SR; CCAF auto-derives
# the Protobuf schema from the column types and registers it in SR under
# subject `<topic>-value` on first INSERT).
#
# DDL is duplicated inline from flink/sql/cc/05_report_sinks.fql to give
# each table its own `depends_on` chain. Keep that file in sync if you
# change a sink shape — see the header comment in 05_report_sinks.fql.
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "latency_report_sink" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS latency_report_1m (
        `window_start`    BIGINT,
        `window_end`      BIGINT,
        `origin_service`  STRING,
        `this_topic`      STRING,
        `sample_count`    BIGINT,
        `distinct_traces` BIGINT,
        `avg_latency_ms`  DOUBLE,
        `min_latency_ms`  BIGINT,
        `max_latency_ms`  BIGINT
    ) WITH (
        'kafka.topic'    = 'isotope-report-latency-1m',
        'value.format'   = 'protobuf-registry',
        'key.format'     = 'protobuf-registry',
        'changelog.mode' = 'append'
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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_kafka_topic.isotope_report["isotope-report-latency-1m"],
  ]
}

resource "confluent_flink_statement" "topology_report_sink" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS topology_report_1m (
        `window_start`     BIGINT,
        `window_end`       BIGINT,
        `origin_service`   STRING,
        `producer_service` STRING,
        `topic`            STRING,
        `records`          BIGINT,
        `distinct_traces`  BIGINT
    ) WITH (
        'kafka.topic'    = 'isotope-report-topology-1m',
        'value.format'   = 'protobuf-registry',
        'key.format'     = 'protobuf-registry',
        'changelog.mode' = 'append'
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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_kafka_topic.isotope_report["isotope-report-topology-1m"],
  ]
}

resource "confluent_flink_statement" "hop_distribution_sink" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS hop_distribution_1m (
        `window_start`    BIGINT,
        `window_end`      BIGINT,
        `this_topic`      STRING,
        `hop_count`       INT,
        `records`         BIGINT,
        `distinct_traces` BIGINT
    ) WITH (
        'kafka.topic'    = 'isotope-report-hop-distribution-1m',
        'value.format'   = 'protobuf-registry',
        'key.format'     = 'protobuf-registry',
        'changelog.mode' = 'append'
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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_kafka_topic.isotope_report["isotope-report-hop-distribution-1m"],
  ]
}

resource "confluent_flink_statement" "coverage_report_sink" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS coverage_report_1m (
        `window_start`    BIGINT,
        `window_end`      BIGINT,
        `this_topic`      STRING,
        `origin_service`  STRING,
        `distinct_traces` BIGINT,
        `records`         BIGINT
    ) WITH (
        'kafka.topic'    = 'isotope-report-coverage-1m',
        'value.format'   = 'protobuf-registry',
        'key.format'     = 'protobuf-registry',
        'changelog.mode' = 'append'
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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_kafka_topic.isotope_report["isotope-report-coverage-1m"],
  ]
}

resource "confluent_flink_statement" "stuck_trace_alerts_sink" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS stuck_trace_alerts_1m (
        `trace_id`        STRING,
        `origin_service`  STRING,
        `last_service`    STRING,
        `last_topic`      STRING,
        `last_hop_count`  INT,
        `last_seen_ts_ms` BIGINT,
        `stuck_for_ms`    BIGINT
    ) WITH (
        'kafka.topic'    = 'isotope-report-stuck-trace-1m',
        'value.format'   = 'protobuf-registry',
        'key.format'     = 'protobuf-registry',
        'changelog.mode' = 'append'
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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_kafka_topic.isotope_report["isotope-report-stuck-trace-1m"],
  ]
}

resource "confluent_flink_statement" "latency_percentiles_sink" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS latency_percentiles_flat_1m (
        `window_start`   BIGINT,
        `window_end`     BIGINT,
        `origin_service` STRING,
        `this_topic`     STRING,
        `p50_ms`         DOUBLE,
        `p95_ms`         DOUBLE,
        `p99_ms`         DOUBLE,
        `sample_count`   BIGINT
    ) WITH (
        'kafka.topic'    = 'isotope-report-latency-percentiles-1m',
        'value.format'   = 'protobuf-registry',
        'key.format'     = 'protobuf-registry',
        'changelog.mode' = 'append'
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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_kafka_topic.isotope_report["isotope-report-latency-percentiles-1m"],
  ]
}

# ---------------------------------------------------------------------------
# Statements 9-10 — CREATE FUNCTION for the two Phase-2 functions.
# `${confluent_flink_artifact.isotope_udf.id}` is interpolated by HCL into
# the `confluent-artifact://<id>` URI.
#
# The SQL shape matches flink/sql/cc/01_register_functions.fql which uses
# `${artifact_id}` as a templatefile placeholder; here the HCL interpolation
# fills it directly from the artifact resource.
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "register_latency_percentiles" {
  statement = <<-EOT
    CREATE FUNCTION IF NOT EXISTS LATENCY_PERCENTILES
        AS 'ai.signalroom.kafka.isotope.flink.LatencyPercentilesUDAF'
        USING JAR 'confluent-artifact://${confluent_flink_artifact.isotope_udf.id}';
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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_artifact.isotope_udf,
  ]
}

resource "confluent_flink_statement" "register_stuck_trace_ptf" {
  statement = <<-EOT
    CREATE FUNCTION IF NOT EXISTS STUCK_TRACE_PTF
        AS 'ai.signalroom.kafka.isotope.flink.StuckTracePTF'
        USING JAR 'confluent-artifact://${confluent_flink_artifact.isotope_udf.id}';
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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_artifact.isotope_udf,
  ]
}

# ---------------------------------------------------------------------------
# Statements 11-16 — 6 streaming INSERT INTO jobs from shared/. Each
# templatefile() reads the canonical FQL the CP runtime uses, then
# `replace()` strips the `SET 'pipeline.name' = '...';` line (CCAF
# rejects SET; the job name is set per-resource via the CCAF
# `statement_name` field or just left to auto-generate).
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "insert_latency_report" {
  statement = replace(
    file("${local.shared_sql_dir}/10_latency_report.fql"),
    local.set_pipeline_name_regex,
    ""
  )

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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.latency_report_sink,
  ]
}

resource "confluent_flink_statement" "insert_topology_report" {
  statement = replace(
    file("${local.shared_sql_dir}/20_topology_report.fql"),
    local.set_pipeline_name_regex,
    ""
  )

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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.topology_report_sink,
  ]
}

resource "confluent_flink_statement" "insert_hop_distribution" {
  statement = replace(
    file("${local.shared_sql_dir}/30_hop_distribution.fql"),
    local.set_pipeline_name_regex,
    ""
  )

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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.hop_distribution_sink,
  ]
}

resource "confluent_flink_statement" "insert_coverage_report" {
  statement = replace(
    file("${local.shared_sql_dir}/40_coverage_report.fql"),
    local.set_pipeline_name_regex,
    ""
  )

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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.coverage_report_sink,
  ]
}

resource "confluent_flink_statement" "insert_stuck_trace_alerts" {
  statement = replace(
    file("${local.shared_sql_dir}/60_stuck_trace_report.fql"),
    local.set_pipeline_name_regex,
    ""
  )

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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.stuck_trace_alerts_sink,
    confluent_flink_statement.register_stuck_trace_ptf,
  ]
}

resource "confluent_flink_statement" "insert_latency_percentiles" {
  statement = replace(
    file("${local.shared_sql_dir}/70_latency_percentiles_report.fql"),
    local.set_pipeline_name_regex,
    ""
  )

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
    ignore_changes = [compute_pool]
  }

  depends_on = [
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.latency_percentiles_sink,
    confluent_flink_statement.register_latency_percentiles,
  ]
}
