-- Sink table for the latency report. CMF auto-creates the Kafka topic
-- (`isotope-report-latency-1m`) and auto-registers a Protobuf schema in
-- Schema Registry as subject `isotope-report-latency-1m-value`. Control
-- Center deserializes it natively.
--
-- Window times are emitted as BIGINT epoch millis on the wire for easy
-- consumption from any client. Rehydrate in queries with
-- TO_TIMESTAMP_LTZ(window_start, 3).
CREATE TABLE IF NOT EXISTS `isotope-report-latency-1m` (
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
-- Broker note: Flink's Kafka EOS sink defaults to transaction.timeout.ms
-- = 1h. The broker's transaction.max.timeout.ms must be >= that; the
-- Kafka CR (k8s/base/confluent-platform-c3++.yaml) overrides it to 1h.
-- CMF doesn't expose per-table options that would let us lower Flink's
-- side, so the broker is the only knob.
