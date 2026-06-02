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
