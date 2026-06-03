/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import java.nio.ByteBuffer;

import com.tdunning.math.stats.MergingDigest;

/**
 * Shared T-Digest (de)serialization for {@link LatencyPercentilesPTF}.
 *
 * <h2>What is a T-Digest?</h2>
 * A T-Digest is a probabilistic data structure (a "sketch") for estimating
 * quantiles/percentiles (e.g. p50, p95, p99) of a stream of numbers without
 * storing every value. Exact percentiles require keeping and sorting all
 * values, which is infeasible for a high-volume stream; a T-Digest instead
 * keeps a small, bounded set of clustered summaries (centroids), where each
 * centroid records roughly "this many points sit near this average value," and
 * reconstructs the cumulative distribution from those centroids on demand.
 *
 * <h2>Core idea</h2>
 * The clustering is adaptive: centroids near the tails of the distribution
 * (p1, p99, p999) are kept small for high accuracy, while centroids in the
 * dense middle are allowed to grow large. This is exactly what latency
 * monitoring wants — high resolution in the p99 tail, less around the median.
 * The resulting sketch is bounded in memory, mergeable (two digests from
 * separate partitions or windows can be combined into one, which is essential
 * for distributed/parallel aggregation), and streaming (values are added
 * incrementally with no buffering or re-sorting). The trade-off is that the
 * percentiles are approximate — very accurate at the tails, slightly less so
 * mid-distribution — which is the standard, accepted bargain for observability
 * metrics.
 *
 * The PTF stores its sketch as a Flink-native {@code byte[]} accumulator field
 * and round-trips through this helper on every record. Keeping the compression
 * parameter and the {@code asSmallBytes}/{@code fromBytes} contract in one
 * place keeps the sketch behavior in a single, testable spot (see
 * {@code TDigestsTest}).
 */
final class TDigests {

    private TDigests() {}

    /**
     * Compression parameter for T-Digest. 100 gives ~5% error at p99 with a
     * few-KB accumulator. Raise it for tighter tails at the cost of memory.
     */
    static final double COMPRESSION = 100.0;

    /** Deserializes a sketch, or returns a fresh empty one when {@code bytes} is null. */
    static MergingDigest load(byte[] bytes) {
        if (bytes == null) return new MergingDigest(COMPRESSION);
        return MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
    }

    /** Serializes a sketch via {@link MergingDigest#asSmallBytes(ByteBuffer)}. */
    static byte[] save(MergingDigest d) {
        ByteBuffer buf = ByteBuffer.allocate(d.smallByteSize());
        d.asSmallBytes(buf);
        return buf.array();
    }
}
