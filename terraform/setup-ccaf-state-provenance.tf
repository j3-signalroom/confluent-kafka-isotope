# =============================================================================
# Optional: state-level provenance over the order topics (CCAF).
# =============================================================================
#
# The CCAF twin of scripts/flink/sql/cp/{09,85}_*.fql. The other two collectors
# record MESSAGE lineage — an isotope is an itinerary, a path of hops. A row in
# an upsert table has no itinerary: its value is the fold of every change that
# touched it, a DAG over versions. So this stamps no hops at all.
# STATE_PROVENANCE publishes one record per emitted state, identified by its
# content (StateVersion) and naming the versions it came from inline.
#
# Same IDs as CP, by construction (docs/state-provenance.md 5.0):
#
#   - The order topics are pre-created by setup-cc-kafka.tf before any producer
#     registers a schema, so the Topic Catalog imports them as
#     (key BYTES, val BYTES) — the raw value is right there, exactly as CP's
#     'value.format' = 'raw' tables expose it. `val` here is `value` there.
#   - StateVersion strips the Schema Registry framing (magic byte + 4-byte
#     schema ID) before hashing. Those four bytes are assigned by whichever
#     registry the producer talked to, so they are the one part of the value
#     that differs between CP and Confluent Cloud for the same record.
#
# Off by default. On, it adds one topic and five statements (sink CREATE TABLE,
# source view, DROP + CREATE FUNCTION, INSERT). CP's equivalent switch is
# STATE_PROVENANCE=true scripts/deploy-cmf-flink-reports.sh up.
#
# The INSERT is its own statement rather than a member of insert_isotope_reports
# for the same reason the merge collector is: an optional feature must not share
# a failure domain with the seven reports that are always on. It costs its own
# compute-pool floor, but only while the feature is on.
#
# =============================================================================

variable "enable_state_provenance" {
  description = "Enable the optional state-level provenance statements — the isotope_state_provenance topic, the entity_log view, and the STATE_PROVENANCE PTF INSERT that writes it."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# 1) The durable lineage record.
#
# Append-only, and that is the point: a version is never revised, so a stream
# describing an UPDATING table is itself an append stream. No PRIMARY KEY, so
# CCAF defaults the table to append mode.
#
# Retention is pinned, not inherited — the topic's retention is the real bound
# on lineage (docs/state-provenance.md 6.0). 'kafka.retention.time' = '0' is
# CCAF's spelling of infinite (Flink durations have no -1), and the cleanup
# policy is pinned to delete because compaction keeps one record per key while a
# version chain needs all of them. CP's twin is retain_forever in
# scripts/deploy-cmf-flink-reports.sh.
#
# Not pre-created as a confluent_kafka_topic, for the reason setup-cc-kafka.tf
# gives for the report sinks: the Topic Catalog would auto-import it as
# (key BYTES, val BYTES) and silently no-op this typed CREATE TABLE.
#
# Columns must stay in step with scripts/flink/sql/cp/09_state_provenance_sinks.fql.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "state_provenance_sink" {
  count = var.enable_state_provenance ? 1 : 0

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `isotope_state_provenance` (
        `version_id`      STRING,
        `entity_key`      STRING,
        `source_name`     STRING,
        `op`              STRING,
        `parents`         ARRAY<STRING>,
        `parent_overflow` INT,
        `emitted_at`      BIGINT
    ) WITH (
        'value.format'         = 'proto-registry',
        'kafka.retention.time' = '0',
        'kafka.cleanup-policy' = 'delete'
    );
  EOT

  statement_name = "isotope-state-provenance-sink"

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
# 2) entity_log — append-mode change log, one row per physical message.
#
# Reads the log, not an upsert view of it: the PTF rebuilds entity state itself,
# so no operator ever consumes a changelog. `content` is the raw `val` bytes,
# framing and all — StateVersion owns the strip, so the canonicalization lives
# in one place that both runtimes run and StateVersionTest pins.
#
# The demo keys the entity on the isotope trace ID, as CP does; see the
# THE DEMO'S ENTITY KEY note in scripts/flink/sql/cp/09_state_provenance_sinks.fql.
# `$rowtime` stands in for CP's declared `event_time` column, the same
# substitution isotope_raw makes.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "entity_log_view" {
  count = var.enable_state_provenance ? 1 : 0

  statement = <<-EOT
    CREATE VIEW IF NOT EXISTS `entity_log` AS
    SELECT
        CAST(`headers`['x-isotope-trace-id']   AS STRING) AS `entity_key`,
        CAST(`headers`['x-isotope-this-topic'] AS STRING) AS `source_name`,
        `val`                                             AS `content`,
        CASE WHEN `val` IS NULL THEN 'DELETE' ELSE 'UPSERT' END AS `op`,
        `$rowtime`                                        AS `event_time`
    FROM `orders.placed`
    WHERE `headers`['x-isotope-trace-id'] IS NOT NULL
    UNION ALL
    SELECT
        CAST(`headers`['x-isotope-trace-id']   AS STRING) AS `entity_key`,
        CAST(`headers`['x-isotope-this-topic'] AS STRING) AS `source_name`,
        `val`                                             AS `content`,
        CASE WHEN `val` IS NULL THEN 'DELETE' ELSE 'UPSERT' END AS `op`,
        `$rowtime`                                        AS `event_time`
    FROM `orders.enriched`
    WHERE `headers`['x-isotope-trace-id'] IS NOT NULL
    UNION ALL
    SELECT
        CAST(`headers`['x-isotope-trace-id']   AS STRING) AS `entity_key`,
        CAST(`headers`['x-isotope-this-topic'] AS STRING) AS `source_name`,
        `val`                                             AS `content`,
        CASE WHEN `val` IS NULL THEN 'DELETE' ELSE 'UPSERT' END AS `op`,
        `$rowtime`                                        AS `event_time`
    FROM `orders.fulfilled`
    WHERE `headers`['x-isotope-trace-id'] IS NOT NULL;
  EOT

  statement_name = "isotope-entity-log-view"

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
  ]
}

# ---------------------------------------------------------------------------
# 3) The STATE_PROVENANCE PTF.
#
# Same drop-then-create discipline as the other functions: rebind to the freshly
# uploaded artifact on every deploy rather than silently keeping a stale
# registration.
#
# Gated, unlike the merge UDFs. CCAF validates a PTF's state type at CREATE
# FUNCTION time (no MapView/ListView, no byte[] map values), so a registration
# that CCAF rejects would fail the default deploy for everyone — including the
# seven reports — even with the feature off. Gating keeps that risk inside the
# opt-in. StateProvenancePTF's EntityState is written to those rules.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "drop_state_provenance" {
  count = var.enable_state_provenance ? 1 : 0

  statement = <<-EOT
    DROP FUNCTION IF EXISTS STATE_PROVENANCE;
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

resource "confluent_flink_statement" "register_state_provenance" {
  count = var.enable_state_provenance ? 1 : 0

  statement = <<-EOT
    CREATE FUNCTION STATE_PROVENANCE
        AS 'ai.signalroom.kafka.isotope.flink.StateProvenancePTF'
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

  depends_on = [confluent_flink_statement.drop_state_provenance]
}

# ---------------------------------------------------------------------------
# 4) The state collector.
#
# Identical to scripts/flink/sql/cp/85_state_provenance.fql without the
# SET 'pipeline.name' directive (CCAF rejects SET in submitted statements).
# PARTITION BY entity_key is what makes the state per-entity; the uid matches
# CP's so the two runtimes name the same state shape the same way.
#
# replace_triggered_by the artifact so the job is stopped BEFORE the DROP above —
# CCAF rejects dropping a function a running statement still uses — and
# restarted on the new code AFTER the CREATE.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "insert_state_provenance" {
  count = var.enable_state_provenance ? 1 : 0

  statement = <<-EOT
    INSERT INTO `isotope_state_provenance`
    SELECT
        `version_id`,
        `entity_key`,
        `source_name`,
        `op`,
        `parents`,
        `parent_overflow`,
        `emitted_at`
    FROM TABLE(
        STATE_PROVENANCE(
            input   => TABLE `entity_log` PARTITION BY `entity_key`,
            on_time => DESCRIPTOR(`event_time`),
            uid     => 'state-provenance-v2'
        )
    );
  EOT

  statement_name = "isotope-state-provenance"

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
    confluent_flink_statement.state_provenance_sink,
    confluent_flink_statement.entity_log_view,
    confluent_flink_statement.register_state_provenance,
  ]
}
