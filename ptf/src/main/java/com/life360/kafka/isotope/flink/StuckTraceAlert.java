package com.life360.kafka.isotope.flink;

import org.apache.flink.table.annotation.DataTypeHint;

/**
 * Output row of {@link StuckTracePTF} — fires once per trace that did
 * not progress within the configured staleness window. Carries the
 * last-observed hop so an operator can triage where the trace got stuck.
 */
@DataTypeHint("ROW<"
    + "trace_id STRING, "
    + "origin_service STRING, "
    + "last_service STRING, "
    + "last_topic STRING, "
    + "last_hop_count INT, "
    + "last_seen_ts_ms BIGINT, "
    + "stuck_for_ms BIGINT"
    + ">")
public class StuckTraceAlert {
    public String trace_id;
    public String origin_service;
    public String last_service;
    public String last_topic;
    public Integer last_hop_count;
    public Long last_seen_ts_ms;
    public Long stuck_for_ms;

    public StuckTraceAlert() {}

    public StuckTraceAlert(String trace_id, String origin_service, String last_service,
                           String last_topic, Integer last_hop_count, Long last_seen_ts_ms,
                           Long stuck_for_ms) {
        this.trace_id = trace_id;
        this.origin_service = origin_service;
        this.last_service = last_service;
        this.last_topic = last_topic;
        this.last_hop_count = last_hop_count;
        this.last_seen_ts_ms = last_seen_ts_ms;
        this.stuck_for_ms = stuck_for_ms;
    }
}
