package ai.signalroom.kafka.isotope.flink;

import org.apache.flink.types.Row;

/**
 * Plain-Java view of {@link LatencyPercentilesUDAF}'s output — the three
 * percentiles customers actually look at on a latency report, plus the
 * underlying sample count. Used only inside the UDAF (for clarity in
 * {@code getValue}) and from the unit tests; the wire-side Flink type is
 * a {@code ROW<p50_ms DOUBLE, p95_ms DOUBLE, p99_ms DOUBLE, sample_count BIGINT>}
 * declared via {@code @FunctionHint} on the UDAF class.
 *
 * Why a POJO and a Row coexist: returning a POJO directly from
 * {@code AggregateFunction.getValue} would force Flink to derive a
 * STRUCTURED logical type from the class. Flink 2.1.2's tightened type
 * extraction handles that poorly when the result feeds an
 * {@code INSERT INTO} sink (the codegen path tries to cast the POJO to
 * {@code Row} and fails). Returning {@code Row} from {@code getValue}
 * sidesteps that entirely — the POJO stays in test code where it makes
 * assertions readable.
 */
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

    public Row toRow() {
        return Row.of(p50_ms, p95_ms, p99_ms, sample_count);
    }

    public static Percentiles fromRow(Row r) {
        return new Percentiles(
            (Double) r.getField(0),
            (Double) r.getField(1),
            (Double) r.getField(2),
            (Long)   r.getField(3)
        );
    }

    @Override
    public String toString() {
        return "Percentiles{p50=" + p50_ms + "ms, p95=" + p95_ms + "ms, p99=" + p99_ms
            + "ms, n=" + sample_count + "}";
    }
}
