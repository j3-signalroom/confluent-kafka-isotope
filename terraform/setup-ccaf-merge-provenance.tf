# =============================================================================
# Optional: fan-in (merge) provenance for the Flink collector (CCAF).
# =============================================================================
#
# The collector in setup-confluent-flink.tf is 1:1 and deliberately so: an
# isotope is a path, provenance is a DAG, and the two coincide only while every
# step has exactly one parent. This file adds the DAG case — a windowed
# aggregate whose output is a business event — without changing the isotope
# wire format.
#
# How it works (docs/flink-collector.md 3.1):
#
#   orders.flink_batched         the MERGED event. Carries a FRESH trace,
#                                because forwarding one of its 1,000 parents'
#                                trace IDs would fabricate provenance.
#   isotope_merge_edge_markers   the many-to-one edges, one row per
#                                contributing record. Same architectural
#                                pattern as isotope_consume_edge_markers.
#
# The merged record and its edge rows are emitted by two different statements
# over the same window, so both derive the SAME merge trace ID from the window
# via ISOTOPE_MERGE_TRACE / ISOTOPE_MERGE_TRACE_ID. That derivation is what
# joins them; see MergeTrace's javadoc for why it cannot be a random mint.
#
# Off by default. On, it adds two topics and one statement, and the edge topic
# writes one record per record ENTERING the merge — roughly doubling that
# stage's write volume. CP's equivalent switch is
# MERGE_PROVENANCE=true scripts/deploy-cmf-flink-reports.sh up.
#
# Kept in its own statement set rather than folded into insert_isotope_reports
# for the same reason the trace-RCA statement is: an optional feature must not
# share a failure domain with the seven reports that are always on.
#
# =============================================================================

variable "enable_merge_provenance" {
  description = "Enable the optional fan-in (merge) provenance statements — merged event topic, merge-edge sideband, and the two INSERTs that write them."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# 1) The merged business event.
#
# Topics are NOT pre-created as confluent_kafka_topic resources — CCAF's Topic
# Catalog would auto-import them as (key BYTES, val BYTES) and silently no-op
# this typed CREATE TABLE. Letting CREATE TABLE own both ends is the same
# discipline setup-confluent-kafka.tf documents for the report sinks.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "merge_batched_sink" {
  count = var.enable_merge_provenance ? 1 : 0

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `orders.flink_batched` (
        `pipeline`        STRING,
        `window_start`    BIGINT,
        `window_end`      BIGINT,
        `event_count`     BIGINT,
        `distinct_traces` BIGINT
    ) WITH (
        'value.format' = 'proto-registry'
    );
  EOT

  statement_name = "isotope-merge-batched-sink"

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

# Two ALTERs, not one — MODIFY requires the column to exist, so it is ADDed as
# VIRTUAL first and then re-declared without VIRTUAL to persist it. Persisted is
# what makes it writable. Identical to the flink_collector_add_headers /
# flink_collector_writable_headers pair in setup-confluent-flink.tf; see the
# comments there for why MAP<STRING, STRING> rather than the native
# MAP<BYTES, BYTES>.
resource "confluent_flink_statement" "merge_batched_add_headers" {
  count = var.enable_merge_provenance ? 1 : 0

  statement = <<-EOT
    ALTER TABLE `orders.flink_batched`
        ADD (`headers` MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL);
  EOT

  statement_name = "isotope-merge-batched-add-headers"

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

  depends_on = [confluent_flink_statement.merge_batched_sink]
}

resource "confluent_flink_statement" "merge_batched_writable_headers" {
  count = var.enable_merge_provenance ? 1 : 0

  statement = <<-EOT
    ALTER TABLE `orders.flink_batched`
        MODIFY `headers` MAP<STRING, STRING> METADATA;
  EOT

  statement_name = "isotope-merge-batched-writable-headers"

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

  depends_on = [confluent_flink_statement.merge_batched_add_headers]
}

# ---------------------------------------------------------------------------
# 2) The merge-edge sideband.
#
# One row per record entering the merge stage: `merge_trace_id` is the trace the
# merged record carries, `contributing_trace_id` is a parent that fed it. Join
# the two on merge_trace_id to recover a merged record's full parent set.
#
# Typed rather than headers-only (unlike isotope_consume_edge_markers, which is
# written by a Kafka client with only headers to work with): Flink writes this,
# so a schema is available — and section 3.2 rules out expressing a
# contributing-trace list as repeated same-key headers anyway.
#
# `operator` is a reserved word in Flink SQL, hence the backticks everywhere.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "merge_edge_markers_sink" {
  count = var.enable_merge_provenance ? 1 : 0

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `isotope_merge_edge_markers` (
        `merge_trace_id`        STRING,
        `window_start`          BIGINT,
        `window_end`            BIGINT,
        `operator`              STRING,
        `pipeline`              STRING,
        `contributing_trace_id` STRING,
        `contributing_service`  STRING,
        `contributing_topic`    STRING
    ) WITH (
        'value.format' = 'proto-registry'
    );
  EOT

  statement_name = "isotope-merge-edge-markers-sink"

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
# 3) The two merge UDFs.
#
# Same drop-then-create discipline as ISOTOPE_APPEND_HOP: rebind to the freshly
# uploaded artifact on every deploy rather than silently keeping a stale
# registration. Unlike the tables and INSERTs above these are NOT gated on the
# variable — registration is inert until a statement calls it, and keeping them
# always-registered means flipping the feature on does not require a JAR
# re-upload cycle to work.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "drop_isotope_merge_trace" {
  statement = <<-EOT
    DROP FUNCTION IF EXISTS ISOTOPE_MERGE_TRACE;
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

resource "confluent_flink_statement" "register_isotope_merge_trace" {
  statement = <<-EOT
    CREATE FUNCTION ISOTOPE_MERGE_TRACE
        AS 'ai.signalroom.kafka.isotope.flink.IsotopeMergeTrace'
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

  depends_on = [confluent_flink_statement.drop_isotope_merge_trace]
}

resource "confluent_flink_statement" "drop_isotope_merge_trace_id" {
  statement = <<-EOT
    DROP FUNCTION IF EXISTS ISOTOPE_MERGE_TRACE_ID;
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

  depends_on = [confluent_flink_statement.register_isotope_merge_trace]
}

resource "confluent_flink_statement" "register_isotope_merge_trace_id" {
  statement = <<-EOT
    CREATE FUNCTION ISOTOPE_MERGE_TRACE_ID
        AS 'ai.signalroom.kafka.isotope.flink.IsotopeMergeTraceId'
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

  depends_on = [confluent_flink_statement.drop_isotope_merge_trace_id]
}

# ---------------------------------------------------------------------------
# 4) The merge collector and its edge sideband, as ONE statement set.
#
# Both or neither: the edge rows are meaningless without the merged records, and
# the merged records are unattributable without the edges. A statement set also
# keeps them on one compute-pool floor rather than two, the same reason the
# seven reports were consolidated.
#
# The first five ISOTOPE_MERGE_TRACE arguments and the five
# ISOTOPE_MERGE_TRACE_ID arguments MUST stay identical — including the WHERE
# clause selecting the input rows. That agreement IS the join between a merged
# record and its parents; if the two drift, every edge points at a
# merge_trace_id no record carries. MergeTraceTest pins the Java side; these two
# INSERTs are the SQL side and are edited by hand.
#
# Mirrors scripts/flink/sql/cp/{80,81}_*.fql without the SET 'pipeline.name'
# directives (CCAF rejects SET in submitted statements).
#
# Why two INSERTs rather than one PTF emitting both row kinds: splitting a PTF's
# output by a tag column needs CREATE VIEW over the PTF, which the cp-flink
# 2.1.2 Expander round-trip rejects — and keeping both runtimes on the same
# shape is the portability claim this repo makes.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "insert_merge_provenance" {
  count = var.enable_merge_provenance ? 1 : 0

  statement = <<-EOT
    EXECUTE STATEMENT SET
    BEGIN

    -- merged business event: fresh trace, one hop, derived identity
    INSERT INTO `orders.flink_batched`
    SELECT
        `pipeline`,
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        COUNT(*)                 AS `event_count`,
        COUNT(DISTINCT trace_id) AS `distinct_traces`,
        ISOTOPE_MERGE_TRACE(
            `pipeline`,
            'orders-batch',
            UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000,
            UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000,
            `pipeline`,
            'flink-batch',
            'orders.flink_batched'
        )
    FROM TABLE(
        TUMBLE(TABLE `isotope`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
    )
    WHERE `this_topic` = 'orders.placed'
    GROUP BY
        `window_start`,
        `window_end`,
        `pipeline`;

    -- merge edges: one row per contributing record
    INSERT INTO `isotope_merge_edge_markers`
    SELECT
        ISOTOPE_MERGE_TRACE_ID(
            `pipeline`,
            'orders-batch',
            UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000,
            UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000,
            `pipeline`
        )                                                     AS `merge_trace_id`,
        UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
        UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
        'orders-batch'                                        AS `operator`,
        `pipeline`,
        `trace_id`                                            AS `contributing_trace_id`,
        `this_service`                                        AS `contributing_service`,
        `this_topic`                                          AS `contributing_topic`
    FROM TABLE(
        TUMBLE(TABLE `isotope`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
    )
    WHERE `this_topic` = 'orders.placed';

    END;
  EOT

  statement_name = "isotope-merge-provenance"

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
    confluent_flink_statement.merge_batched_writable_headers,
    confluent_flink_statement.merge_edge_markers_sink,
    confluent_flink_statement.register_isotope_merge_trace,
    confluent_flink_statement.register_isotope_merge_trace_id,
  ]
}
