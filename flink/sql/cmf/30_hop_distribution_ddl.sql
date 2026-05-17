-- Sink table for the hop-distribution report.
CREATE TABLE IF NOT EXISTS `isotope-report-hop-distribution-1m` (
  `window_start`    BIGINT,
  `window_end`      BIGINT,
  `this_topic`      STRING,
  `hop_count`       INT,
  `records`         BIGINT,
  `distinct_traces` BIGINT
) WITH (
  'value.format' = 'proto-registry'
);
