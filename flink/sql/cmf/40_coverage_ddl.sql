-- Sink table for the coverage report.
CREATE TABLE IF NOT EXISTS `isotope-report-coverage-1m` (
  `window_start`    BIGINT,
  `window_end`      BIGINT,
  `this_topic`      STRING,
  `origin_service`  STRING,
  `distinct_traces` BIGINT,
  `records`         BIGINT
) WITH (
  'value.format' = 'proto-registry'
);
