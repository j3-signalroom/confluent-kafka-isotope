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
# Compute pool — a single pool runs every INSERT plus the DDL CREATEs.
#
# Sized at 20 CFU, roughly 2x the standing floor. The workload is nine
# concurrent statements: five windowed aggregations, two stateful PTFs
# (timers, T-Digest), the optional ML_PREDICT trace-RCA join, and the
# stateless collector projection. Every running statement has a floor of
# 1 CFU, so 10 CFU left nothing spare — a tenth ad-hoc statement sat
# PENDING indefinitely rather than being scheduled.
#
# Raising the ceiling is close to free: CCAF bills CFUs actually consumed,
# not max_cfu. The pool autoscales, so this only bounds how far it may go —
# headroom for ad-hoc SELECTs and for the aggregations to scale under load,
# while still capping blast radius if a statement misbehaves.
#
# Nine statements is a choice here, not a CCAF limit. CP runs all seven
# reports in ONE job via a StatementSet (see IsotopeReportsJob); CCAF has
# the same capability via EXECUTE STATEMENT SET BEGIN ... END, which runs
# multiple INSERTs as a single optimized statement and is explicitly meant
# for INSERTs that read the same table or share intermediate results —
# which is exactly the seven reports over the `isotope` view. Consolidating
# them would cut the CFU floor substantially, at the cost of one job whose
# failure stops every report. Sized for the current one-statement-per-INSERT
# layout; revisit if that consolidation ever happens.
# ---------------------------------------------------------------------------

resource "confluent_flink_compute_pool" "isotope" {
  display_name = "isotope-flink-statement-runner"
  cloud        = var.cloud
  region       = var.region
  max_cfu      = 20

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

    # Pinned, not inherited. Every report converts window bounds with
    # UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000, which formats and
    # parses through the session zone; ISOTOPE_MERGE_TRACE then derives merge
    # trace IDs from those milliseconds. An unpinned zone makes CP and CCAF
    # disagree on both, and a DST zone makes the fall-back hour ambiguous.
    # CP's twin is tableEnv.getConfig().setLocalTimeZone(...) in
    # IsotopeReportsJob.
    "sql.local-time-zone" = "UTC"
  }

  # Content hash of the shadow JAR. confluent_flink_artifact tracks only the
  # file PATH, not its bytes, so Terraform never notices rebuilt code on its
  # own. This drives the re-upload (see terraform_data.artifact_jar_hash and the
  # artifact's replace_triggered_by). The deploy script always rebuilds the JAR
  # before apply, so this reflects the current code.
  artifact_jar_md5 = filemd5("${path.module}/${var.artifact_jar_path}")
}

# ---------------------------------------------------------------------------
# Flink artifact — uploads ptf/build/libs/isotope-flink-udf.jar to CCAF.
# Build it with Java-17-target bytecode (see ptf/build.gradle) — CCAF
# rejects artifacts compiled for Java versions > 17.
#
# Re-upload on code change: the artifact only tracks the JAR path, so a content
# change wouldn't otherwise replace it. Embedding the JAR's md5 in display_name
# (a ForceNew attribute) makes a rebuilt JAR replace the artifact — uploading
# the new bytes under a new artifact id and cascading the drop/recreate of every
# PTF + restart of the consumer jobs (via their replace_triggered_by on this
# resource). create_before_destroy keeps the OLD artifact alive until the
# functions have rebound to the new one, so deleting it never hits "artifact in
# use"; the md5 in display_name keeps the two distinct while they briefly
# coexist. The deploy script always rebuilds the JAR before apply, so when the
# code is unchanged the md5 (and thus display_name) is stable and nothing churns.
# ---------------------------------------------------------------------------

resource "confluent_flink_artifact" "isotope_udf" {
  display_name   = "isotope-flink-udf-${substr(local.artifact_jar_md5, 0, 12)}"
  content_format = "JAR"
  cloud          = var.cloud
  region         = var.region
  artifact_file  = "${path.module}/${var.artifact_jar_path}"

  environment {
    id = confluent_environment.isotope.id
  }

  lifecycle {
    create_before_destroy = true
  }
}

# ---------------------------------------------------------------------------
# Statements 1-4 — ALTER TABLE on each isotope event topic to expose Kafka
# record headers as a column. CCAF's auto-imported topic tables do not
# include `headers` by default; the source view below references it.
#
# NOT IDEMPOTENT, and CCAF offers no per-column IF NOT EXISTS (only
# `ALTER TABLE IF EXISTS`, which guards the table, not the column). The added
# column lives with the TOPIC's catalog entry, so it survives destroying or
# replacing these statement resources — re-creating one against a table that
# already has `headers` fails with "Column `headers` already exists in the
# table." and taints the resource, blocking every downstream statement on the
# next apply. scripts/deploy-cc-flink-reports.sh untaints these four before
# `terraform apply` for exactly that reason; if you run terraform by hand,
# do the same:
#     terraform state list | grep _add_headers$ | xargs -n1 terraform untaint
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "alter_orders_placed_add_headers" {
  statement = <<-EOT
    ALTER TABLE `orders.placed`
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
    confluent_kafka_topic.isotope_event["orders.placed"],
    confluent_flink_compute_pool.isotope,
  ]
}

resource "confluent_flink_statement" "alter_orders_enriched_add_headers" {
  statement = <<-EOT
    ALTER TABLE `orders.enriched`
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
    confluent_kafka_topic.isotope_event["orders.enriched"],
    confluent_flink_compute_pool.isotope,
  ]
}

resource "confluent_flink_statement" "alter_orders_fulfilled_add_headers" {
  statement = <<-EOT
    ALTER TABLE `orders.fulfilled`
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
    confluent_kafka_topic.isotope_event["orders.fulfilled"],
    confluent_flink_compute_pool.isotope,
  ]
}

resource "confluent_flink_statement" "alter_consume_events_add_headers" {
  statement = <<-EOT
    ALTER TABLE `isotope_consume_edge_markers`
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
    confluent_kafka_topic.isotope_event["isotope_consume_edge_markers"],
    confluent_flink_compute_pool.isotope,
  ]
}

# ---------------------------------------------------------------------------
# Statement 5 — `isotope_raw` view over the isotope event topics, including
# the consume-marker topic. The downstream `isotope` view filters to records
# without x-isotope-consumer-service (real produces); `consume_events`
# filters to records *with* it (consume-edge markers).
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "isotope_raw_view" {
  statement = <<-EOT
    CREATE VIEW IF NOT EXISTS isotope_raw AS
    SELECT
        `$rowtime` AS `event_time`,
        `headers`  AS `headers`
    FROM `orders.placed`
    UNION ALL
    SELECT
        `$rowtime` AS `event_time`,
        `headers`  AS `headers`
    FROM `orders.enriched`
    UNION ALL
    SELECT
        `$rowtime` AS `event_time`,
        `headers`  AS `headers`
    FROM `orders.fulfilled`
    UNION ALL
    SELECT
        `$rowtime` AS `event_time`,
        `headers`  AS `headers`
    FROM `isotope_consume_edge_markers`;
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
    confluent_flink_statement.alter_orders_placed_add_headers,
    confluent_flink_statement.alter_orders_enriched_add_headers,
    confluent_flink_statement.alter_orders_fulfilled_add_headers,
    confluent_flink_statement.alter_consume_events_add_headers,
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
        CAST(`headers`['x-isotope-pipeline']                       AS STRING) AS pipeline,
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
    WHERE `headers`['x-isotope-trace-id']         IS NOT NULL
      AND `headers`['x-isotope-consumer-service'] IS NULL;
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
# Statement 5b — `consume_events` typed view (header decoder, consume side).
# Filters isotope_raw to records with x-isotope-consumer-service — the marker
# header IsotopeContext.recordConsume() adds — so this view's rows are the
# inbound edges of the bipartite topology graph.
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "consume_events_view" {
  statement = <<-EOT
    CREATE VIEW IF NOT EXISTS consume_events AS
    SELECT
        CAST(`headers`['x-isotope-trace-id']                       AS STRING) AS trace_id,
        CAST(CAST(`headers`['x-isotope-origin-ts']    AS STRING)   AS BIGINT) AS origin_ts_ms,
        CAST(`headers`['x-isotope-origin-service']                 AS STRING) AS origin_service,
        CAST(`headers`['x-isotope-pipeline']                       AS STRING) AS pipeline,
        CAST(`headers`['x-isotope-this-service']                   AS STRING) AS producer_service,
        CAST(`headers`['x-isotope-this-topic']                     AS STRING) AS consumed_topic,
        CAST(`headers`['x-isotope-consumer-service']               AS STRING) AS consumer_service,
        CAST(CAST(`headers`['x-isotope-hop-count']    AS STRING)   AS INT)    AS hop_count_at_consume,
        `event_time`
    FROM isotope_raw
    WHERE `headers`['x-isotope-consumer-service'] IS NOT NULL
      AND `headers`['x-isotope-trace-id']         IS NOT NULL;
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
# Statements 6-12 — 7 sink tables. SR-framed Protobuf format is requested
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
        `pipeline`        STRING,
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
        `pipeline`         STRING,
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

resource "confluent_flink_statement" "isotope_report_bipartite_topology_1m" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS isotope_report_bipartite_topology_1m (
        `window_start`      BIGINT,
        `window_end`        BIGINT,
        `pipeline`          STRING,
        `edge_type`         STRING,
        `producer_service`  STRING,
        `topic`             STRING,
        `consumer_service`  STRING,
        `origin_service`    STRING,
        `records`           BIGINT,
        `distinct_traces`   BIGINT
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
        `pipeline`        STRING,
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
        `pipeline`        STRING,
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
        `pipeline`        STRING,
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

resource "confluent_flink_statement" "isotope_report_latency_percentiles_1m" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS isotope_report_latency_percentiles_1m (
        `window_start`   BIGINT,
        `window_end`     BIGINT,
        `pipeline`       STRING,
        `origin_service` STRING,
        `this_topic`     STRING,
        `p50_ms`         DOUBLE,
        `p95_ms`         DOUBLE,
        `p99_ms`         DOUBLE,
        `sample_count`   BIGINT
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
# Statement 12 — CREATE FUNCTION for the two ProcessTableFunctions.
#
# STUCK_TRACE_PTF and LATENCY_PERCENTILES are both PTFs, which CCAF accepts.
# Percentiles are implemented as a PTF (not a user-defined aggregate)
# specifically because CCAF rejects all user-defined aggregate function
# registrations with "aggregate functions are not supported"; a PTF registers
# and runs on both CCAF and CP
# (see scripts/flink/sql/cp/01_register_functions.fql and
# scripts/flink/sql/cp/70_latency_percentiles_report.fql).
# ---------------------------------------------------------------------------

# Each PTF is dropped and recreated on every deploy so it always rebinds to the
# freshly uploaded JAR. The deploy script force-replaces the artifact (its
# content isn't tracked by Terraform), which fires the replace_triggered_by on
# the whole chain below. A plain CREATE FUNCTION — not CREATE FUNCTION IF NOT
# EXISTS — preceded by DROP FUNCTION IF EXISTS guarantees the new code is
# picked up; IF NOT EXISTS would silently keep the stale registration. The
# consuming INSERT job is also replace_triggered_by the artifact (see
# insert_stuck_trace_alerts) so it is stopped BEFORE the DROP — CCAF rejects
# dropping a function that a running statement still uses — and restarted on the
# new code AFTER the CREATE. Because the artifact is the root of the dependency
# chain, Terraform serializes this as: stop job → DROP → re-upload → CREATE →
# restart job.
resource "confluent_flink_statement" "drop_stuck_trace_ptf" {
  statement = <<-EOT
    DROP FUNCTION IF EXISTS STUCK_TRACE_PTF;
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
    ignore_changes       = [statement, compute_pool]
    replace_triggered_by = [confluent_flink_artifact.isotope_udf]
  }

  depends_on = [confluent_flink_artifact.isotope_udf]
}

resource "confluent_flink_statement" "register_stuck_trace_ptf" {
  statement = <<-EOT
    CREATE FUNCTION STUCK_TRACE_PTF
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
    ignore_changes       = [statement, compute_pool]
    replace_triggered_by = [confluent_flink_artifact.isotope_udf]
  }

  depends_on = [confluent_flink_statement.drop_stuck_trace_ptf]
}

resource "confluent_flink_statement" "drop_latency_percentiles" {
  statement = <<-EOT
    DROP FUNCTION IF EXISTS LATENCY_PERCENTILES;
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
    ignore_changes       = [statement, compute_pool]
    replace_triggered_by = [confluent_flink_artifact.isotope_udf]
  }

  depends_on = [confluent_flink_artifact.isotope_udf]
}

resource "confluent_flink_statement" "register_latency_percentiles" {
  statement = <<-EOT
    CREATE FUNCTION LATENCY_PERCENTILES
        AS 'ai.signalroom.kafka.isotope.flink.LatencyPercentilesPTF'
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
    ignore_changes       = [statement, compute_pool]
    replace_triggered_by = [confluent_flink_artifact.isotope_udf]
  }

  depends_on = [confluent_flink_statement.drop_latency_percentiles]
}

# ---------------------------------------------------------------------------
# ISOTOPE_APPEND_HOP — the collector-side scalar UDF (in-band propagation).
#
# The two PTFs above are INTERPRETERS: they read isotope headers that a
# service's IsotopeProducerInterceptor already wrote. This function is a
# COLLECTOR: it appends a Flink hop and rewrites the headers on the way out,
# so a Flink statement can be a traced stage rather than only a reader.
#
# It exists so Flink can participate in a trace without the out-of-band
# ThreadLocal model, which cannot survive a shuffle or the SQL planner. Same
# wire format as the interceptor, so the reports are unchanged.
#
# Registering it is inert on its own — nothing invokes it until a table's
# `headers` metadata column is made writable (ALTER TABLE ... MODIFY `headers`
# MAP<STRING, STRING> METADATA) and an INSERT calls it. See
# docs/flink-collector.md.
#
# Same drop-then-create discipline as the PTFs: rebind to the freshly uploaded
# artifact on every deploy rather than silently keeping a stale registration.
resource "confluent_flink_statement" "drop_isotope_append_hop" {
  statement = <<-EOT
    DROP FUNCTION IF EXISTS ISOTOPE_APPEND_HOP;
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
    ignore_changes       = [statement, compute_pool]
    replace_triggered_by = [confluent_flink_artifact.isotope_udf]
  }

  depends_on = [confluent_flink_artifact.isotope_udf]
}

resource "confluent_flink_statement" "register_isotope_append_hop" {
  statement = <<-EOT
    CREATE FUNCTION ISOTOPE_APPEND_HOP
        AS 'ai.signalroom.kafka.isotope.flink.IsotopeAppendHop'
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
    ignore_changes       = [statement, compute_pool]
    replace_triggered_by = [confluent_flink_artifact.isotope_udf]
  }

  depends_on = [confluent_flink_statement.drop_isotope_append_hop]
}

# ---------------------------------------------------------------------------
# All eight streaming INSERTs, executed as ONE statement.
#
# This mirrors what CP already does: IsotopeReportsJob builds a StatementSet
# and calls execute() once, so the seven reports plus the collector run as a
# single Flink job. CCAF's equivalent is EXECUTE STATEMENT SET BEGIN ... END,
# which Confluent documents as optimizing INSERTs that "read from the same
# table or share intermediate results" — exactly these, which all scan the
# `isotope` view or `orders.placed`.
#
# Why consolidate: each separate CCAF statement carries its own compute-pool
# floor of 1 CFU, so eight statements meant eight floors and a saturated pool
# (a ninth ad-hoc query would not schedule). It also makes the two runtimes
# structurally the same, which is the portability claim this repo makes.
#
# The tradeoff is one failure domain — a fault in any INSERT stops all of
# them. That matches CP's behavior, where the whole Application restarts.
#
# NOT included: isotope-report-trace-rca-1m, which lives in setup-ccaf-ai.tf
# and is gated on var.enable_trace_rca. It reads a report sink rather than the
# event stream and is optional, so it stays independent.
#
# replace_triggered_by covers the whole set now, not just the three INSERTs
# that call UDFs — the artifact changing restarts every INSERT in the job.
#
# Per-INSERT notes preserved from the resources this replaced:
#
# --- insert_latency_report ---
# ---------------------------------------------------------------------------
# 7 streaming INSERT INTO jobs. Same business logic as the CP-side INSERTs in
# scripts/flink/sql/cp/{10,20,25,30,40,60,70}_*.fql; transcribed here without
# the `SET 'pipeline.name'` directive (CCAF rejects SET in submitted
# statements — it's a SQL-Client interactive command, not a Flink SQL
# statement).
#
# Two CCAF constraints shape every resource below:
#
#   * `statement_name` is a Kubernetes-style name — lowercase alphanumerics and
#     hyphens only, starting with an alphanumeric, ≤ 100 chars. The sink TABLE
#     names keep their underscores (`isotope_report_latency_1m`); only the
#     statement names are hyphenated (`isotope-report-latency-1m`). Underscores
#     here fail at create with "400 Bad Request: Request is malformed. name
#     needs to contain lowercase alphanumeric characters and hyphens only".
#
#   * A statement cannot be UPDATED in place — the provider only accepts
#     changes to `stopped` / `properties_sensitive`. Editing the SQL below
#     produces a plan Terraform shows as an in-place update, which then fails at
#     apply. scripts/deploy-cc-flink-reports.sh detects those and re-runs them
#     as `-replace=` (see tf-statement-updates.py); driving terraform by hand
#     means passing `-replace=<address>` yourself.
# ---------------------------------------------------------------------------
#
# --- insert_latency_percentiles ---
# Percentiles report — p50/p95/p99 via the LATENCY_PERCENTILES PTF. Mirrors
# scripts/flink/sql/cp/70_latency_percentiles_report.fql without the SET
# 'pipeline.name' directive (CCAF rejects SET in submitted statements).
# PARTITION BY (pipeline, origin_service, this_topic) sets the aggregation grain.
#
# --- insert_flink_collector ---
# The collector INSERT. 1:1 projection only, and deliberately so: this is
# tracing, not provenance. An isotope is a path (one identity, its ordered
# hops); provenance is a DAG (each output naming the inputs that produced it).
# They coincide only while every step is 1:1 — a SUM over 1,000 records has
# 1,000 parents and the format can name one (see docs/flink-collector.md).
#
# The hop timestamp is passed IN as UNIX_TIMESTAMP() * 1000 rather than read
# inside eval(): Flink assumes a ScalarFunction is deterministic and may fold,
# reuse, or recompute it, so a clock read in the Java would drift on replay.
#
# Same replace_triggered_by discipline as insert_stuck_trace_alerts: this job is
# stopped before ISOTOPE_APPEND_HOP is dropped (CCAF rejects dropping an in-use
# function) and rebuilt on the new code after the function is recreated.
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "insert_isotope_reports" {
  statement = <<-EOT
    EXECUTE STATEMENT SET
    BEGIN

    -- latency report
    INSERT INTO isotope_report_latency_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `pipeline`,
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
        `pipeline`,
        `origin_service`,
        `this_topic`;

    -- topology report
    INSERT INTO isotope_report_topology_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `pipeline`,
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
        `pipeline`,
        `origin_service`,
        `this_service`,
        `this_topic`;

    -- bipartite topology report
    INSERT INTO isotope_report_bipartite_topology_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `pipeline`,
        CAST('produce' AS STRING)                             AS `edge_type`,
        `this_service`                                        AS `producer_service`,
        `this_topic`                                          AS `topic`,
        CAST(NULL AS STRING)                                  AS `consumer_service`,
        `origin_service`,
        COUNT(*)                                              AS `records`,
        COUNT(DISTINCT trace_id)                              AS `distinct_traces`
    FROM TABLE(
        TUMBLE(TABLE isotope, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
    )
    GROUP BY
        `window_start`,
        `window_end`,
        `pipeline`,
        `origin_service`,
        `this_service`,
        `this_topic`
    UNION ALL
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `pipeline`,
        CAST('consume' AS STRING)                             AS `edge_type`,
        CAST(NULL AS STRING)                                  AS `producer_service`,
        `consumed_topic`                                      AS `topic`,
        `consumer_service`,
        `origin_service`,
        COUNT(*)                                              AS `records`,
        COUNT(DISTINCT trace_id)                              AS `distinct_traces`
    FROM TABLE(
        TUMBLE(TABLE consume_events, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
    )
    GROUP BY
        `window_start`,
        `window_end`,
        `pipeline`,
        `origin_service`,
        `consumer_service`,
        `consumed_topic`;

    -- hop distribution
    INSERT INTO isotope_report_hop_distribution_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `pipeline`,
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
        `pipeline`,
        `this_topic`,
        `hop_count`;

    -- coverage report
    INSERT INTO isotope_report_coverage_1m
    SELECT
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        `pipeline`,
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
        `pipeline`,
        `this_topic`,
        `origin_service`;

    -- stuck trace alerts
    INSERT INTO isotope_report_stuck_trace_1m
    SELECT
        trace_id,
        origin_service,
        pipeline,
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

    -- latency percentiles
    INSERT INTO isotope_report_latency_percentiles_1m
    SELECT
        `window_start`,
        `window_end`,
        `pipeline`,
        `origin_service`,
        `this_topic`,
        `p50_ms`,
        `p95_ms`,
        `p99_ms`,
        `sample_count`
    FROM TABLE(
        LATENCY_PERCENTILES(
            input   => TABLE isotope PARTITION BY (`pipeline`, `origin_service`, `this_topic`),
            on_time => DESCRIPTOR(`event_time`),
            uid     => 'latency-pcts-v2'
        )
    );

    -- flink collector
    INSERT INTO `orders.flink_enriched`
    SELECT
        `key`,
        `val`,
        ISOTOPE_APPEND_HOP(
            CAST(`headers` AS MAP<STRING, STRING>),
            'flink-enrich',
            'orders.flink_enriched',
            'orders',
            UNIX_TIMESTAMP() * 1000
        )
    FROM `orders.placed`;

    END;
  EOT

  statement_name = "isotope-reports-1m"

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
    ignore_changes       = [compute_pool]
    replace_triggered_by = [confluent_flink_artifact.isotope_udf]
  }

  depends_on = [
    confluent_flink_statement.isotope_view,
    confluent_flink_statement.isotope_report_latency_1m,
    confluent_flink_statement.isotope_report_topology_1m,
    confluent_flink_statement.consume_events_view,
    confluent_flink_statement.isotope_report_bipartite_topology_1m,
    confluent_flink_statement.isotope_report_hop_distribution_1m,
    confluent_flink_statement.isotope_report_coverage_1m,
    confluent_flink_statement.isotope_report_stuck_trace_1m,
    confluent_flink_statement.register_stuck_trace_ptf,
    confluent_flink_statement.isotope_report_latency_percentiles_1m,
    confluent_flink_statement.register_latency_percentiles,
    confluent_flink_statement.flink_collector_writable_headers,
    confluent_flink_statement.register_isotope_append_hop,
  ]
}







# ---------------------------------------------------------------------------
# Flink as an isotope COLLECTOR (in-band propagation) — orders.flink_enriched.
#
# The seven statements above are INTERPRETERS: they read isotope headers a
# service's IsotopeProducerInterceptor already wrote. The three resources below
# make Flink a traced PRODUCER, so it finally appears in topology_1m and
# bipartite_topology_1m instead of being invisible in its own reports.
#
# Deliberately a NEW parallel topic. orders.{placed,enriched,fulfilled} stay
# entirely with out-of-band propagation; nothing about the three-stage service
# pipeline changes.
#
# Column shape mirrors what CCAF's Topic Catalog inferred for orders.placed —
# confirmed with DESCRIBE: (`key` BYTES BUCKET KEY, `val` BYTES) plus the
# headers metadata column. Note this differs from the CP side, which declares
# `value` BYTES locally in 07_flink_collector_sink.fql; each runtime keeps the
# table shape its own catalog produces.
#
# DISTRIBUTED BY (`key`) is not optional here. Declaring 'key.format' without
# it fails at create with "A key format 'key.format' requires the declaration
# of one or more of key fields using DISTRIBUTED BY." — which is also why
# DESCRIBE reports `key` as BUCKET KEY on the source table. Bucket count is
# left to CCAF's default rather than pinned.
# ---------------------------------------------------------------------------

resource "confluent_flink_statement" "flink_collector_sink" {
  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `orders.flink_enriched` (
        `key` BYTES,
        `val` BYTES
    ) DISTRIBUTED BY (`key`) WITH (
        'key.format'   = 'raw',
        'value.format' = 'raw'
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

# Two ALTERs, not one: MODIFY requires the column to exist, so it is ADDed as
# VIRTUAL first and then re-declared without VIRTUAL to persist it. Persisted
# is what makes it writable — virtual metadata columns are read-only and are
# excluded from the INSERT INTO target schema.
#
# MAP<STRING, STRING> is an implicit cast of the native MAP<BYTES, BYTES>, and
# is the type ISOTOPE_APPEND_HOP takes and returns.
#
# Consequence to remember: once persisted, `headers` is MANDATORY in every
# INSERT INTO this table.
resource "confluent_flink_statement" "flink_collector_add_headers" {
  statement = <<-EOT
    ALTER TABLE `orders.flink_enriched`
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

  depends_on = [confluent_flink_statement.flink_collector_sink]
}

resource "confluent_flink_statement" "flink_collector_writable_headers" {
  statement = <<-EOT
    ALTER TABLE `orders.flink_enriched`
        MODIFY `headers` MAP<STRING, STRING> METADATA;
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

  depends_on = [confluent_flink_statement.flink_collector_add_headers]
}

