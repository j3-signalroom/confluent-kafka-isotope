/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Accumulator-level test — verifies that {@code accumulate} + {@code
 * getValue} produce sensible percentiles for a known input distribution,
 * and that {@code merge} produces results indistinguishable from a single
 * accumulator fed the union of inputs (the property Flink relies on for
 * parallel pre-aggregation).
 *
 * We don't spin up a Flink runtime here — that would require
 * flink-table-planner + a mini-cluster, and the algorithmic correctness
 * we care about is entirely in the accumulator class.
 */
class LatencyPercentilesUDAFTest {

    @Test
    void emptyAccumulatorReturnsNullPercentilesAndZeroCount() {
        LatencyPercentilesUDAF udaf = new LatencyPercentilesUDAF();
        LatencyPercentilesUDAF.TDigestAccumulator acc = udaf.createAccumulator();

        Percentiles p = Percentiles.fromRow(udaf.getValue(acc));
        assertNotNull(p);
        assertEquals(0L, p.sample_count);
        assertNull(p.p50_ms);
        assertNull(p.p95_ms);
        assertNull(p.p99_ms);
    }

    @Test
    void uniformDistributionGivesExpectedPercentiles() {
        LatencyPercentilesUDAF udaf = new LatencyPercentilesUDAF();
        LatencyPercentilesUDAF.TDigestAccumulator acc = udaf.createAccumulator();

        // 0..999 → p50 ≈ 500, p95 ≈ 950, p99 ≈ 990
        for (long i = 0; i < 1000; i++) {
            udaf.accumulate(acc, i);
        }

        Percentiles p = Percentiles.fromRow(udaf.getValue(acc));
        assertEquals(1000L, p.sample_count);
        // T-Digest with compression=100 gives ~few-% error; check rough bounds.
        assertTrue(Math.abs(p.p50_ms - 500.0) < 20.0,
            "p50 should be ~500, got " + p.p50_ms);
        assertTrue(Math.abs(p.p95_ms - 950.0) < 20.0,
            "p95 should be ~950, got " + p.p95_ms);
        assertTrue(Math.abs(p.p99_ms - 990.0) < 20.0,
            "p99 should be ~990, got " + p.p99_ms);
    }

    @Test
    void mergeProducesSameDistributionAsUnionFedToOneAccumulator() {
        LatencyPercentilesUDAF udaf = new LatencyPercentilesUDAF();

        // Reference: single accumulator gets the full sequence.
        LatencyPercentilesUDAF.TDigestAccumulator ref = udaf.createAccumulator();
        for (long i = 0; i < 1000; i++) udaf.accumulate(ref, i);

        // Merge case: split the sequence across two accumulators, then merge
        // them into a third.
        LatencyPercentilesUDAF.TDigestAccumulator a = udaf.createAccumulator();
        LatencyPercentilesUDAF.TDigestAccumulator b = udaf.createAccumulator();
        for (long i = 0;   i < 500;  i++) udaf.accumulate(a, i);
        for (long i = 500; i < 1000; i++) udaf.accumulate(b, i);

        LatencyPercentilesUDAF.TDigestAccumulator merged = udaf.createAccumulator();
        udaf.merge(merged, List.of(a, b));

        Percentiles refP    = Percentiles.fromRow(udaf.getValue(ref));
        Percentiles mergedP = Percentiles.fromRow(udaf.getValue(merged));

        assertEquals(refP.sample_count, mergedP.sample_count);
        // Mergeability: percentile estimates should be close (not identical;
        // T-Digest's centroid set differs based on insertion order, but the
        // quantile estimates should agree within the sketch's error bound).
        assertTrue(Math.abs(refP.p50_ms - mergedP.p50_ms) < 20.0);
        assertTrue(Math.abs(refP.p95_ms - mergedP.p95_ms) < 20.0);
        assertTrue(Math.abs(refP.p99_ms - mergedP.p99_ms) < 20.0);
    }

    @Test
    void resetAccumulatorClearsState() {
        LatencyPercentilesUDAF udaf = new LatencyPercentilesUDAF();
        LatencyPercentilesUDAF.TDigestAccumulator acc = udaf.createAccumulator();
        for (long i = 0; i < 100; i++) udaf.accumulate(acc, i);
        assertEquals(100L, Percentiles.fromRow(udaf.getValue(acc)).sample_count);

        udaf.resetAccumulator(acc);
        assertEquals(0L, Percentiles.fromRow(udaf.getValue(acc)).sample_count);
        assertNull(Percentiles.fromRow(udaf.getValue(acc)).p50_ms);
    }

    @Test
    void accumulateIgnoresNullLatency() {
        LatencyPercentilesUDAF udaf = new LatencyPercentilesUDAF();
        LatencyPercentilesUDAF.TDigestAccumulator acc = udaf.createAccumulator();
        udaf.accumulate(acc, 100L);
        udaf.accumulate(acc, null);
        udaf.accumulate(acc, 200L);
        assertEquals(2L, Percentiles.fromRow(udaf.getValue(acc)).sample_count);
    }
}
