-- Sink table for the topology report. CMF auto-creates Kafka topic
-- `isotope-report-topology-1m` and registers a Protobuf schema in SR.
CREATE TABLE IF NOT EXISTS `isotope-report-topology-1m` (
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
