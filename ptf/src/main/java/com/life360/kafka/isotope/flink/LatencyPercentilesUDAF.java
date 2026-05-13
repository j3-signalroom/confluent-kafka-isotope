package com.life360.kafka.isotope.flink;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Streaming p50/p95/p99 over latency_ms, computed via a T-Digest sketch.
 *
 * Use in SQL as:
 *   SELECT origin_service, this_topic,
 *          window_start, window_end,
 *          LATENCY_PERCENTILES(latency_ms) AS pcts
 *   FROM TABLE(TUMBLE(...))
 *   GROUP BY origin_service, this_topic, window_start, window_end;
 *
 * Then unpack with `pcts.p50_ms`, `pcts.p95_ms`, `pcts.p99_ms`.
 *
 * Why T-Digest: standard Flink SQL has no portable approx-percentile
 * aggregate that works on both CCAF and CP Flink. T-Digest gives bounded
 * memory (default compression = 100 → a few KB per accumulator), accurate
 * tail percentiles, and merge semantics that play well with parallel
 * accumulators.
 */
public class LatencyPercentilesUDAF
        extends AggregateFunction<Percentiles, LatencyPercentilesUDAF.TDigestAccumulator> {

    /** Compression parameter for T-Digest. 100 gives ~5% error at p99, ~few KB per accumulator. */
    private static final double COMPRESSION = 100.0;

    /**
     * Mutable accumulator wrapping a serializable T-Digest sketch.
     *
     * MergingDigest is the recommended T-Digest variant for streaming
     * aggregation — bounded memory, merge() is fast, and the implementation
     * is the same one used by Druid and many other percentile-in-streaming
     * systems.
     */
    public static class TDigestAccumulator {
        public MergingDigest digest = new MergingDigest(COMPRESSION);
        public long sampleCount = 0L;
    }

    @Override
    public TDigestAccumulator createAccumulator() {
        return new TDigestAccumulator();
    }

    public void accumulate(TDigestAccumulator acc, Long latencyMs) {
        if (latencyMs == null) return;
        acc.digest.add((double) latencyMs);
        acc.sampleCount++;
    }

    public void retract(TDigestAccumulator acc, Long latencyMs) {
        // T-Digest does not support retraction; with append-only Kafka
        // streams (no UPDATE/DELETE) this method is never called by Flink
        // for a tumbling-window aggregation. We define it (no-op) to keep
        // the contract complete in case the function is used over a stream
        // that produces retractions.
    }

    public void merge(TDigestAccumulator acc, Iterable<TDigestAccumulator> others) {
        for (TDigestAccumulator o : others) {
            if (o.sampleCount == 0L) continue;
            acc.digest.add(o.digest);
            acc.sampleCount += o.sampleCount;
        }
    }

    public void resetAccumulator(TDigestAccumulator acc) {
        acc.digest = new MergingDigest(COMPRESSION);
        acc.sampleCount = 0L;
    }

    @Override
    public Percentiles getValue(TDigestAccumulator acc) {
        if (acc.sampleCount == 0L) {
            return new Percentiles(null, null, null, 0L);
        }
        return new Percentiles(
            acc.digest.quantile(0.50),
            acc.digest.quantile(0.95),
            acc.digest.quantile(0.99),
            acc.sampleCount
        );
    }
}
