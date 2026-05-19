package ai.signalroom.kafka.isotope.flink;

import java.nio.ByteBuffer;

import com.tdunning.math.stats.MergingDigest;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

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
 * Runtime-portability note: this UDAF is currently <b>only deployed on
 * Confluent Platform Flink</b>. Confluent Cloud for Apache Flink (CCAF)
 * rejects all UDAF CREATE FUNCTION statements with "aggregate functions
 * are not supported" — a hard platform limitation, not something the
 * accumulator shape can work around. The CC Terraform module
 * (terraform/setup-confluent-flink.tf) intentionally omits this function;
 * see the comment block above `register_stuck_trace_ptf` there.
 *
 * The accumulator nonetheless stores the T-Digest as `byte[]` (Flink-native,
 * POJO-clean) rather than as a {@link MergingDigest} field, so the JAR is
 * ready to deploy on CCAF the day Confluent lifts the UDAF restriction.
 * Round-tripping through byte[] on every {@code accumulate}/{@code merge}/
 * {@code getValue} costs a few microseconds per record (MergingDigest.
 * smallByteSize is a few hundred bytes).
 *
 * Why we return {@code Row} (not a POJO): Flink 2.1.2 tightened structured-
 * type extraction. Declaring a POJO output forced the planner to derive a
 * STRUCTURED logical type, and downstream codegen (specifically, the sink-
 * write path used by {@code INSERT INTO}) emits a cast of the POJO to {@code
 * Row} which fails at Janino-compile time. Returning {@code Row} with an
 * explicit {@code @FunctionHint(output = @DataTypeHint("ROW<...>"))} declares
 * the logical type AND the conversion class consistently. See {@link
 * Percentiles} for the POJO retained for test readability.
 */
@FunctionHint(output = @DataTypeHint(
    "ROW<p50_ms DOUBLE, p95_ms DOUBLE, p99_ms DOUBLE, sample_count BIGINT>"))
public class LatencyPercentilesUDAF
        extends AggregateFunction<Row, LatencyPercentilesUDAF.TDigestAccumulator> {

    /** Compression parameter for T-Digest. 100 gives ~5% error at p99, ~few KB per accumulator. */
    private static final double COMPRESSION = 100.0;

    /**
     * POJO-shaped accumulator: a serialized T-Digest blob plus a sample count.
     *
     * Both fields are public, Flink-serializable (byte[] and long are native),
     * and the class has the no-arg ctor Flink's POJO extractor needs. CCAF's
     * UDF validator accepts this shape; on CP Flink it works identically.
     */
    public static class TDigestAccumulator {
        /** Serialized {@link MergingDigest} (via {@link MergingDigest#asSmallBytes}). Null = empty digest. */
        public byte[] digestBytes;
        public long sampleCount = 0L;
    }

    @Override
    public TDigestAccumulator createAccumulator() {
        return new TDigestAccumulator();
    }

    public void accumulate(TDigestAccumulator acc, Long latencyMs) {
        if (latencyMs == null) return;
        MergingDigest d = loadDigest(acc.digestBytes);
        d.add((double) latencyMs);
        acc.digestBytes = saveDigest(d);
        acc.sampleCount++;
    }

    public void retract(TDigestAccumulator acc, Long latencyMs) {
        // T-Digest does not support retraction; with append-only Kafka streams
        // (no UPDATE/DELETE) this method is never called by Flink for a
        // tumbling-window aggregation. Defined (no-op) to keep the contract
        // complete in case the function is used over a retracting stream.
    }

    public void merge(TDigestAccumulator acc, Iterable<TDigestAccumulator> others) {
        MergingDigest accD = null;
        boolean changed = false;
        for (TDigestAccumulator o : others) {
            if (o.sampleCount == 0L) continue;
            if (accD == null) accD = loadDigest(acc.digestBytes);
            accD.add(loadDigest(o.digestBytes));
            acc.sampleCount += o.sampleCount;
            changed = true;
        }
        if (changed) acc.digestBytes = saveDigest(accD);
    }

    public void resetAccumulator(TDigestAccumulator acc) {
        acc.digestBytes = null;
        acc.sampleCount = 0L;
    }

    @Override
    public Row getValue(TDigestAccumulator acc) {
        if (acc.sampleCount == 0L) {
            return new Percentiles(null, null, null, 0L).toRow();
        }
        MergingDigest d = loadDigest(acc.digestBytes);
        return new Percentiles(
            d.quantile(0.50),
            d.quantile(0.95),
            d.quantile(0.99),
            acc.sampleCount
        ).toRow();
    }

    private static MergingDigest loadDigest(byte[] bytes) {
        if (bytes == null) return new MergingDigest(COMPRESSION);
        return MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
    }

    private static byte[] saveDigest(MergingDigest d) {
        ByteBuffer buf = ByteBuffer.allocate(d.smallByteSize());
        d.asSmallBytes(buf);
        return buf.array();
    }
}
