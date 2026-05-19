# =====================================================================
# Service account, role bindings, compute pool, artifact, statements.
#
# Pattern mirrors apache_flink-kickstarter-ii: every CCAF Flink statement
# lives inline in HCL as a `confluent_flink_statement` resource. The
# parallel CP runtime hardcodes its SQL in scripts/flink/sql/cp/ (no shared
# directory) — the two runtimes are independent.
# =====================================================================

resource "confluent_service_account" "flink_sql_runner" {
  display_name = "isotope-flink-sql-runner"
  description  = "Service account for executing the isotope Flink SQL reports on CCAF."
}

resource "confluent_role_binding" "flink_sql_runner_as_flink_developer" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = data.confluent_organization.current.resource_name

  depends_on = [confluent_service_account.flink_sql_runner]
}

resource "confluent_role_binding" "flink_sql_runner_as_resource_owner_topic_access" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.isotope.rbac_crn}/kafka=${confluent_kafka_cluster.isotope.id}/topic=*"

  depends_on = [confluent_role_binding.flink_sql_runner_as_flink_developer]
}

resource "confluent_role_binding" "flink_sql_runner_as_assigner" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "Assigner"
  crn_pattern = "${data.confluent_organization.current.resource_name}/service-account=${confluent_service_account.flink_sql_runner.id}"

  depends_on = [confluent_role_binding.flink_sql_runner_as_resource_owner_topic_access]
}

resource "confluent_role_binding" "flink_sql_runner_schema_registry_access" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.isotope.resource_name}/subject=*"

  depends_on = [confluent_role_binding.flink_sql_runner_as_assigner]
}

resource "confluent_role_binding" "flink_sql_runner_as_resource_owner_transactional_access" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.isotope.rbac_crn}/kafka=${confluent_kafka_cluster.isotope.id}/transactional-id=*"

  depends_on = [confluent_role_binding.flink_sql_runner_schema_registry_access]
}

# Consumer-group access — needed by the demo CLI's sink/hop modes (each
# instantiates a Kafka consumer with a fresh group id like
# `isotope-sink-<uuid>` / `isotope-hop-<service>-<uuid>`). Without this
# binding, the broker rejects the group registration with
# `GroupAuthorizationException: Not authorized to access group: ...`.
# Flink statements themselves don't trigger this — they read via the
# CCAF runtime's own group management — but external Kafka clients do.
resource "confluent_role_binding" "flink_sql_runner_as_resource_owner_group_access" {
  principal   = "User:${confluent_service_account.flink_sql_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.isotope.rbac_crn}/kafka=${confluent_kafka_cluster.isotope.id}/group=*"

  depends_on = [confluent_role_binding.flink_sql_runner_as_resource_owner_transactional_access]
}

# ---------------------------------------------------------------------------
# Compute pool — single small pool runs every report INSERT plus the
# DDL CREATEs. 10 CFU is comfortable for the demo.
# ---------------------------------------------------------------------------

resource "confluent_flink_compute_pool" "isotope" {
  display_name = "isotope-flink-statement-runner"
  cloud        = var.cloud
  region       = var.region
  max_cfu      = 10

  environment {
    id = confluent_environment.isotope.id
  }

  depends_on = [confluent_role_binding.flink_sql_runner_as_resource_owner_group_access]
}

# Rotating Flink API key for every confluent_flink_statement's credentials block.
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
# `sql.current-catalog` and `sql.current-database` so unqualified table /
# view references resolve to the cluster's topic catalog.
# ---------------------------------------------------------------------------

locals {
  flink_statement_properties = {
    "sql.current-catalog"  = confluent_environment.isotope.display_name
    "sql.current-database" = confluent_kafka_cluster.isotope.display_name
  }
}

# ---------------------------------------------------------------------------
# Flink artifact — uploads ptf/build/libs/isotope-flink-udf.jar to CCAF.
# Build it with Java-17-target bytecode (see ptf/build.gradle) — CCAF
# rejects artifacts compiled for Java versions > 17.
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
# Statements 1-3 — ALTER TABLE on each iso_* topic to expose Kafka record
# headers as a column. CCAF's auto-imported topic tables do not include
# `headers` by default; the source view below references it.
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "alter_iso_start_add_headers" {
  statement = <<-EOT
    ALTER TABLE `iso_start`
        ADD (`headers` MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL);
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
    confluent_kafka_topic.isotope_event["iso_start"],
    confluent_flink_compute_pool.isotope,
  ]
}

resource "confluent_flink_statement" "alter_iso_mid_add_headers" {
  statement = <<-EOT
    ALTER TABLE `iso_mid`
        ADD (`headers` MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL);
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
    confluent_kafka_topic.isotope_event["iso_mid"],
    confluent_flink_compute_pool.isotope,
  ]
}

resource "confluent_flink_statement" "alter_iso_final_add_headers" {
  statement = <<-EOT
    ALTER TABLE `iso_final`
        ADD (`headers` MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL);
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
    confluent_kafka_topic.isotope_event["iso_final"],
    confluent_flink_compute_pool.isotope,
  ]
}

# ---------------------------------------------------------------------------
# Statement 4 — `isotope_raw` view over the three iso_* topics.
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "isotope_raw_view" {
  statement = <<-EOT
    CREATE VIEW IF NOT EXISTS isotope_raw AS
    SELECT
        `$rowtime` AS `event_time`,
        `headers`  AS `headers`
    FROM `iso_start`
    UNION ALL
    SELECT
        `$rowtime` AS `event_time`,
        `headers`  AS `headers`
    FROM `iso_mid`
    UNION ALL
    SELECT
        `$rowtime` AS `event_time`,
        `headers`  AS `headers`
    FROM `iso_final`;
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
    confluent_flink_statement.alter_iso_start_add_headers,
    confluent_flink_statement.alter_iso_mid_add_headers,
    confluent_flink_statement.alter_iso_final_add_headers,
  ]
}

# ---------------------------------------------------------------------------
# Statement 5 — `isotope` typed view (header decoder + per-record latency).
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "isotope_view" {
  statement = <<-EOT
    CREATE VIEW IF NOT EXISTS isotope AS
    SELECT
        CAST(`headers`['x-isotope-trace-id']                       AS STRING) AS trace_id,
        CAST(CAST(`headers`['x-isotope-origin-ts']    AS STRING)   AS BIGINT) AS origin_ts_ms,
        CAST(`headers`['x-isotope-origin-service']                 AS STRING) AS origin_service,
        CAST(`headers`['x-isotope-this-service']                   AS STRING) AS this_service,
        CAST(`headers`['x-isotope-this-topic']                     AS STRING) AS this_topic,
        CAST(CAST(`headers`['x-isotope-hop-count']    AS STRING)   AS INT)    AS hop_count,
        `event_time`,
        TIMESTAMPDIFF(
            MILLISECOND,
            TO_TIMESTAMP_LTZ(
                CAST(CAST(`headers`['x-isotope-origin-ts'] AS STRING) AS BIGINT),
                3),
            `event_time`
        ) AS latency_ms
    FROM isotope_raw
    WHERE `headers`['x-isotope-trace-id'] IS NOT NULL;
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

  depends_on = [confluent_flink_statement.isotope_raw_view]
}

# ---------------------------------------------------------------------------
# Statements 6-11 — 6 sink tables. SR-framed Protobuf format is requested
# via `sql.tables.kafka.{value,key}.format` statement-level properties
# (see local.flink_statement_sink_properties). CCAF derives the Protobuf
# schema from the column types and registers it in SR under subject
# `<topic>-value` on first INSERT.
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "isotope_report_latency_1m" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS isotope_report_latency_1m (
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

  # No depends_on for the sink topic — see comment in
  # setup-confluent-kafka.tf where confluent_kafka_topic.isotope_report
  # was intentionally removed. CCAF's CREATE TABLE creates the topic.
  depends_on = [confluent_flink_compute_pool.isotope]
}

resource "confluent_flink_statement" "isotope_report_topology_1m" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS isotope_report_topology_1m (
        `window_start`     BIGINT,
        `window_end`       BIGINT,
        `origin_service`   STRING,
        `producer_service` STRING,
        `topic`            STRING,
        `records`          BIGINT,
        `distinct_traces`  BIGINT
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

resource "confluent_flink_statement" "isotope_report_hop_distribution_1m" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS isotope_report_hop_distribution_1m (
        `window_start`    BIGINT,
        `window_end`      BIGINT,
        `this_topic`      STRING,
        `hop_count`       INT,
        `records`         BIGINT,
        `distinct_traces` BIGINT
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

resource "confluent_flink_statement" "isotope_report_coverage_1m" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS isotope_report_coverage_1m (
        `window_start`    BIGINT,
        `window_end`      BIGINT,
        `this_topic`      STRING,
        `origin_service`  STRING,
        `distinct_traces` BIGINT,
        `records`         BIGINT
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

resource "confluent_flink_statement" "isotope_report_stuck_trace_1m" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS isotope_report_stuck_trace_1m (
        `trace_id`        STRING,
        `origin_service`  STRING,
        `last_service`    STRING,
        `last_topic`      STRING,
        `last_hop_count`  INT,
        `last_seen_ts_ms` BIGINT,
        `stuck_for_ms`    BIGINT
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

# ---------------------------------------------------------------------------
# Statement 12 — CREATE FUNCTION for STUCK_TRACE_PTF (a ProcessTableFunction).
#
# LATENCY_PERCENTILES (an AggregateFunction / UDAF) is intentionally NOT
# registered on CCAF — the platform rejects all UDAF registrations with
# "aggregate functions are not supported" regardless of the accumulator
# shape. The Java class still ships in the JAR for the CP runtime, which
# has no such restriction (see scripts/flink/sql/cp/01_register_functions.fql and
# scripts/flink/sql/cp/70_latency_percentiles_report.fql).
# ---------------------------------------------------------------------------

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
    ignore_changes = [statement, compute_pool]
  }

  depends_on = [confluent_flink_artifact.isotope_udf]
}

# ---------------------------------------------------------------------------
# Statements 14-19 — 6 streaming INSERT INTO jobs. Same business logic as
# the CP-side INSERTs in scripts/flink/sql/cp/{10,20,30,40,60,70}_*.fql; transcribed
# here without the `SET 'pipeline.name'` directive (CCAF rejects SET in
# submitted statements — it's a SQL-Client interactive command, not a Flink
# SQL statement).
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "insert_latency_report" {
  statement = <<-EOT
    INSERT INTO isotope_report_latency_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `origin_service`,
        `this_topic`,
        COUNT(*)                          AS `sample_count`,
        COUNT(DISTINCT trace_id)          AS `distinct_traces`,
        AVG(CAST(latency_ms AS DOUBLE))   AS `avg_latency_ms`,
        CAST(MIN(latency_ms) AS BIGINT)   AS `min_latency_ms`,
        CAST(MAX(latency_ms) AS BIGINT)   AS `max_latency_ms`
    FROM TABLE(
        TUMBLE(TABLE isotope, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
    )
    GROUP BY
        `window_start`,
        `window_end`,
        `origin_service`,
        `this_topic`;
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
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.isotope_report_latency_1m,
  ]
}

resource "confluent_flink_statement" "insert_topology_report" {
  statement = <<-EOT
    INSERT INTO isotope_report_topology_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `origin_service`,
        `this_service`            AS `producer_service`,
        `this_topic`              AS `topic`,
        COUNT(*)                  AS `records`,
        COUNT(DISTINCT trace_id)  AS `distinct_traces`
    FROM TABLE(
        TUMBLE(TABLE isotope, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
    )
    GROUP BY
        `window_start`,
        `window_end`,
        `origin_service`,
        `this_service`,
        `this_topic`;
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
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.isotope_report_topology_1m,
  ]
}

resource "confluent_flink_statement" "insert_hop_distribution" {
  statement = <<-EOT
    INSERT INTO isotope_report_hop_distribution_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `this_topic`,
        `hop_count`,
        COUNT(*)                  AS `records`,
        COUNT(DISTINCT trace_id)  AS `distinct_traces`
    FROM TABLE(
        TUMBLE(TABLE isotope, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
    )
    GROUP BY
        `window_start`,
        `window_end`,
        `this_topic`,
        `hop_count`;
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
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.isotope_report_hop_distribution_1m,
  ]
}

resource "confluent_flink_statement" "insert_coverage_report" {
  statement = <<-EOT
    INSERT INTO isotope_report_coverage_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `this_topic`,
        `origin_service`,
        COUNT(DISTINCT trace_id)  AS `distinct_traces`,
        COUNT(*)                  AS `records`
    FROM TABLE(
        TUMBLE(TABLE isotope, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
    )
    GROUP BY
        `window_start`,
        `window_end`,
        `this_topic`,
        `origin_service`;
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
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.isotope_report_coverage_1m,
  ]
}

resource "confluent_flink_statement" "insert_stuck_trace_alerts" {
  statement = <<-EOT
    INSERT INTO isotope_report_stuck_trace_1m
    SELECT
        trace_id,
        origin_service,
        last_service,
        last_topic,
        last_hop_count,
        last_seen_ts_ms,
        stuck_for_ms
    FROM TABLE(
        STUCK_TRACE_PTF(
            input   => TABLE isotope PARTITION BY trace_id,
            on_time => DESCRIPTOR(event_time),
            uid     => 'stuck-trace-v1'
        )
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
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.isotope_report_stuck_trace_1m,
    confluent_flink_statement.register_stuck_trace_ptf,
  ]
}

