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
 * Locks the T-Digest (de)serialization contract used by {@link
 * LatencyPercentilesPTF}. If the compression parameter or the byte format ever
 * changes, the percentile values (and their cross-restart stability) would
 * shift silently — this test guards against that.
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
        // Uniform 0..999 → p50 ≈ 500, p95 ≈ 950, p99 ≈ 990, exercised through
        // the shared helper the PTF uses directly in onTimer().
        MergingDigest d = TDigests.load(null);
        for (long i = 0; i < 1000; i++) d.add((double) i);
        d = TDigests.load(TDigests.save(d));

        assertTrue(Math.abs(d.quantile(0.50) - 500.0) < 20.0, "p50 ~500, got " + d.quantile(0.50));
        assertTrue(Math.abs(d.quantile(0.95) - 950.0) < 20.0, "p95 ~950, got " + d.quantile(0.95));
        assertTrue(Math.abs(d.quantile(0.99) - 990.0) < 20.0, "p99 ~990, got " + d.quantile(0.99));
    }
}
