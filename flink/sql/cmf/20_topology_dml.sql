-- Topology report — per-minute produce-edge counts (producer_service →
-- topic), as seen at each record's most-recent hop. See cp-flink
-- counterpart for caveats about consume-side blindness.
INSERT INTO `isotope-report-topology-1m`
WITH `iso_decoded` AS (
    SELECT
        CAST(`headers`['x-isotope-trace-id']        AS STRING) AS `trace_id`,
        CAST(`headers`['x-isotope-origin-service']  AS STRING) AS `origin_service`,
        CAST(`headers`['x-isotope-this-service']    AS STRING) AS `this_service`,
        CAST(`headers`['x-isotope-this-topic']      AS STRING) AS `this_topic`,
        `$rowtime`                                             AS `event_time`
    FROM `iso-start`
    WHERE `headers`['x-isotope-trace-id'] IS NOT NULL
)
SELECT
    UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
    UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
    `origin_service`,
    `this_service`            AS `producer_service`,
    `this_topic`              AS `topic`,
    COUNT(*)                  AS `records`,
    COUNT(DISTINCT `trace_id`) AS `distinct_traces`
FROM TABLE(TUMBLE(TABLE `iso_decoded`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE))
GROUP BY
    `window_start`,
    `window_end`,
    `origin_service`,
    `this_service`,
    `this_topic`;
