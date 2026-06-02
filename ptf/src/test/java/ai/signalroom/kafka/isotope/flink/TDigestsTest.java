/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tdunning.math.stats.MergingDigest;
import org.junit.jupiter.api.Test;

/**
 * Locks the shared T-Digest (de)serialization contract used by BOTH
 * {@link LatencyPercentilesUDAF} (CP-only UDAF) and {@link LatencyPercentilesPTF}
 * (CP+CCAF PTF). If these two ever diverge in compression or byte format, the
 * 70_ and 71_ reports would stop producing matching percentiles for the same
 * input — this test guards against that.
 */
class TDigestsTest {

    @Test
    void loadOfNullReturnsEmptyDigest() {
        MergingDigest d = TDigests.load(null);
        assertNotNull(d);
        assertEquals(0L, d.size());
    }

    @Test
    void saveThenLoadRoundTripsPercentiles() {
        MergingDigest src = TDigests.load(null);
        for (long i = 0; i < 1000; i++) src.add((double) i);

        MergingDigest restored = TDigests.load(TDigests.save(src));

        assertEquals(src.size(), restored.size());
        // Same sketch in, same quantiles out — bit-for-bit on the centroid set.
        assertEquals(src.quantile(0.50), restored.quantile(0.50));
        assertEquals(src.quantile(0.95), restored.quantile(0.95));
        assertEquals(src.quantile(0.99), restored.quantile(0.99));
    }

    @Test
    void compressionGivesExpectedAccuracyOnUniform() {
        // Same distribution the UDAF test asserts on, exercised through the
        // shared helper the PTF uses directly in onTimer().
        MergingDigest d = TDigests.load(null);
        for (long i = 0; i < 1000; i++) d.add((double) i);
        d = TDigests.load(TDigests.save(d));

        assertTrue(Math.abs(d.quantile(0.50) - 500.0) < 20.0, "p50 ~500, got " + d.quantile(0.50));
        assertTrue(Math.abs(d.quantile(0.95) - 950.0) < 20.0, "p95 ~950, got " + d.quantile(0.95));
        assertTrue(Math.abs(d.quantile(0.99) - 990.0) < 20.0, "p99 ~990, got " + d.quantile(0.99));
    }
}
