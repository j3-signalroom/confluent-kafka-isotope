# =============================================================================
# Optional: reports health monitor (CCAF).
# =============================================================================
#
# The CCAF twin of IsotopeHealthJob and scripts/flink/sql/cp/{97,98}_*.fql. It
# alerts when traffic on orders.placed keeps flowing but the reports stop
# publishing; ReportsHealthPTF has the rule. It runs the same function as CP from
# the same JAR.
#
# Off by default. On, it adds one topic and five statements (sink CREATE TABLE,
# signals view, DROP + CREATE FUNCTION, INSERT). CP's equivalent switch is
# HEALTH_MONITOR=true scripts/deploy-cmf-flink-reports.sh up.
#
# The INSERT is its own statement, not a member of insert_isotope_reports, so it
# keeps running when that statement fails. It still shares the compute pool, the
# Kafka cluster and Confluent Cloud itself with the reports, and it publishes to
# Kafka, so it can't report an outage of any of those.
#
# =============================================================================

variable "enable_health_monitor" {
  description = "Enable the optional reports health monitor statements — the isotope_reports_health topic, the health_signals view, and the REPORTS_HEALTH PTF INSERT that writes it."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# 1) The alerts. Not pre-created as a confluent_kafka_topic, for the reason
# setup-cc-kafka.tf gives for the report sinks. Columns must stay in step with
# reports_health_alerts in scripts/flink/sql/cp/97_reports_health_tables.fql.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "reports_health_sink" {
  count = var.enable_health_monitor ? 1 : 0

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `isotope_reports_health` (
        `monitored_job`            STRING,
        `status`                   STRING,
        `overdue_window_end`       BIGINT,
        `last_reported_window_end` BIGINT,
        `last_traffic_ts_ms`       BIGINT,
        `event_ts_ms`              BIGINT,
        `stalled_for_ms`           BIGINT
    ) WITH (
        'value.format' = 'proto-registry'
    );
  EOT

  statement_name = "isotope-reports-health-sink"

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
# 2) health_signals — traffic and report rows, interleaved. Same shape and filter
# as CP's view; `$rowtime` stands in for CP's declared `event_time` column, the
# same substitution isotope_raw makes.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "health_signals_view" {
  count = var.enable_health_monitor ? 1 : 0

  statement = <<-EOT
    CREATE VIEW IF NOT EXISTS `health_signals` AS
    SELECT
        'isotope-reports'    AS `monitored_job`,
        'traffic'            AS `kind`,
        CAST(NULL AS BIGINT) AS `window_end`,
        `$rowtime`           AS `event_time`
    FROM `orders.placed`
    WHERE `headers`['x-isotope-trace-id']         IS NOT NULL
      AND `headers`['x-isotope-consumer-service'] IS NULL
    UNION ALL
    SELECT
        'isotope-reports'    AS `monitored_job`,
        'report'             AS `kind`,
        `window_end`,
        `$rowtime`           AS `event_time`
    FROM `isotope_report_topology_1m`;
  EOT

  statement_name = "isotope-health-signals-view"

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
    confluent_flink_statement.isotope_report_topology_1m,
  ]
}

# ---------------------------------------------------------------------------
# 3) The REPORTS_HEALTH PTF. Gated, like STATE_PROVENANCE and for the same reason:
# CCAF checks a PTF's state type at CREATE FUNCTION time, so a rejection must stay
# inside the opt-in.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "drop_reports_health" {
  count = var.enable_health_monitor ? 1 : 0

  statement = <<-EOT
    DROP FUNCTION IF EXISTS REPORTS_HEALTH;
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

resource "confluent_flink_statement" "register_reports_health" {
  count = var.enable_health_monitor ? 1 : 0

  statement = <<-EOT
    CREATE FUNCTION REPORTS_HEALTH
        AS 'ai.signalroom.kafka.isotope.flink.ReportsHealthPTF'
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

  depends_on = [confluent_flink_statement.drop_reports_health]
}

# ---------------------------------------------------------------------------
# 4) The monitor. Identical to scripts/flink/sql/cp/98_reports_health.fql without
# the SET 'pipeline.name' directive, and with these properties standing in for
# what the CP job sets in code and DDL:
#
#   - idle-timeout: required. The report topic going quiet is the condition being
#     detected, and CCAF's default idleness detection can grow to 5 minutes, which
#     would hold this statement's watermark back and delay the alert by that long.
#   - startup.mode: judges the reports from now on, not by replaying traffic from
#     before the monitor existed. Applies to new statements, so a restart from
#     Flink's own state keeps its position.
#
# replace_triggered_by the artifact so the job is stopped BEFORE the DROP above and
# restarted on the new code AFTER the CREATE.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "insert_reports_health" {
  count = var.enable_health_monitor ? 1 : 0

  statement = <<-EOT
    INSERT INTO `isotope_reports_health`
    SELECT
        `monitored_job`,
        `status`,
        `overdue_window_end`,
        `last_reported_window_end`,
        `last_traffic_ts_ms`,
        `event_ts_ms`,
        `stalled_for_ms`
    FROM TABLE(
        REPORTS_HEALTH(
            input   => TABLE `health_signals` PARTITION BY `monitored_job`,
            on_time => DESCRIPTOR(`event_time`),
            uid     => 'reports-health-v1'
        )
    );
  EOT

  statement_name = "isotope-reports-health"

  properties = merge(local.flink_statement_properties, {
    "sql.tables.scan.idle-timeout" = "30 s"
    "sql.tables.scan.startup.mode" = "latest-offset"
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
    ignore_changes       = [compute_pool]
    replace_triggered_by = [confluent_flink_artifact.isotope_udf]
  }

  depends_on = [
    confluent_flink_statement.reports_health_sink,
    confluent_flink_statement.health_signals_view,
    confluent_flink_statement.register_reports_health,
  ]
}
