package com.life360.kafka.isotope.flink;

import org.apache.flink.table.annotation.DataTypeHint;

/**
 * Return type of {@link LatencyPercentilesUDAF} — the three percentiles
 * customers actually look at on a latency report.
 */
@DataTypeHint("ROW<p50_ms DOUBLE, p95_ms DOUBLE, p99_ms DOUBLE, sample_count BIGINT>")
public class Percentiles {
    public Double p50_ms;
    public Double p95_ms;
    public Double p99_ms;
    public Long sample_count;

    public Percentiles() {}

    public Percentiles(Double p50_ms, Double p95_ms, Double p99_ms, Long sample_count) {
        this.p50_ms = p50_ms;
        this.p95_ms = p95_ms;
        this.p99_ms = p99_ms;
        this.sample_count = sample_count;
    }

    @Override
    public String toString() {
        return "Percentiles{p50=" + p50_ms + "ms, p95=" + p95_ms + "ms, p99=" + p99_ms
            + "ms, n=" + sample_count + "}";
    }
}
