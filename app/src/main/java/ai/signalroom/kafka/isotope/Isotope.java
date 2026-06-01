/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class Isotope {

    public static final String HEADER_KEY = "x-isotope";

    /**
     * Scalar reporting headers, written alongside {@link #HEADER_KEY} by
     * {@code IsotopeProducerInterceptor}. All values are UTF-8 strings so
     * Flink SQL can cast them directly to STRING (or BIGINT for the numeric
     * ones) without a UDF.
     */
    public static final String HEADER_TRACE_ID        = "x-isotope-trace-id";
    public static final String HEADER_ORIGIN_TS       = "x-isotope-origin-ts";
    public static final String HEADER_ORIGIN_SERVICE  = "x-isotope-origin-service";
    public static final String HEADER_PIPELINE        = "x-isotope-pipeline";
    public static final String HEADER_THIS_SERVICE    = "x-isotope-this-service";
    public static final String HEADER_THIS_TOPIC      = "x-isotope-this-topic";
    public static final String HEADER_HOP_COUNT       = "x-isotope-hop-count";

    /**
     * Consume-side marker header. Written by
     * {@code IsotopeContext.recordConsume} when a consumer emits a marker to
     * the {@code platform.observability.consume_events} topic, on top of the
     * seven forwarded headers above. Its presence is what Flink uses to
     * distinguish a consume-event marker from a regular produced record.
     */
    public static final String HEADER_CONSUMER_SERVICE = "x-isotope-consumer-service";

    /**
     * Bounded ring for the hop list &mdash; a payload guard, not a routing cap.
     * On overflow the oldest hop is evicted and {@code truncated} latches true;
     * the trace keeps flowing through as many services as you want.
     */
    public static final int MAX_HOPS = 32;
    public static final int TRACE_ID_BYTES = 16;

    private static final ObjectMapper JSON = new ObjectMapper();

    @JsonProperty("t") private final byte[] traceId;
    @JsonProperty("o") private final long originTsMs;
    @JsonProperty("s") private final String originService;
    // Pipeline name — set once at the origin (like originService / originTsMs)
    // and forwarded unchanged through every hop, so reports can slice traces
    // by which logical pipeline they belong to (e.g. orders vs location).
    @JsonProperty("p") private final String pipeline;
    @JsonProperty("h") private final List<Hop> hops;
    @JsonProperty("x") private boolean truncated;

    @JsonCreator
    Isotope(
        @JsonProperty("t") byte[] traceId,
        @JsonProperty("o") long originTsMs,
        @JsonProperty("s") String originService,
        @JsonProperty("p") String pipeline,
        @JsonProperty("h") List<Hop> hops,
        @JsonProperty("x") boolean truncated
    ) {
        this.traceId = traceId;
        this.originTsMs = originTsMs;
        this.originService = originService;
        this.pipeline = pipeline;
        this.hops = hops != null ? new ArrayList<>(hops) : new ArrayList<>();
        this.truncated = truncated;
    }

    /** Starts a fresh trace with an unnamed pipeline ({@code "unknown"}). */
    public static Isotope newTrace(String originService) {
        return newTrace(originService, "unknown");
    }

    public static Isotope newTrace(String originService, String pipeline) {
        long now = System.currentTimeMillis();
        return new Isotope(uuidV7Bytes(now), now,
            Objects.requireNonNullElse(originService, "unknown"),
            Objects.requireNonNullElse(pipeline, "unknown"),
            new ArrayList<>(), false);
    }

    /**
     * Builds a 16-byte RFC 9562 UUIDv7: 48-bit Unix-ms timestamp in the
     * high bits, followed by 74 bits of random plus the version (4) and
     * variant (2) bit patches. Lexicographic byte order matches creation
     * order.
     *
     * <p>Uses {@link ThreadLocalRandom} rather than {@link java.security.SecureRandom}
     * because trace IDs are public observability identifiers carried in Kafka
     * headers, not security tokens. The requirement here is collision avoidance,
     * not unpredictability, and {@code ThreadLocalRandom} delivers that without
     * the per-call cost of {@code SecureRandom} on every produced record.
     */
    @SuppressWarnings("java:S2245")
    static byte[] uuidV7Bytes(long unixMs) {
        byte[] b = new byte[TRACE_ID_BYTES];
        // bytes 0-5: 48-bit timestamp, big-endian
        b[0] = (byte) (unixMs >>> 40);
        b[1] = (byte) (unixMs >>> 32);
        b[2] = (byte) (unixMs >>> 24);
        b[3] = (byte) (unixMs >>> 16);
        b[4] = (byte) (unixMs >>>  8);
        b[5] = (byte)  unixMs;
        // bytes 6-15: random, then patch version & variant bits
        byte[] rand = new byte[10];
        ThreadLocalRandom.current().nextBytes(rand);
        System.arraycopy(rand, 0, b, 6, 10);
        b[6] = (byte) ((b[6] & 0x0F) | 0x70); // version = 0111
        b[8] = (byte) ((b[8] & 0x3F) | 0x80); // variant = 10 (RFC 4122/9562)
        return b;
    }

    /** Convenience: a fresh UUIDv7 in canonical {@code xxxxxxxx-...-7xxx-yxxx-...} string form. */
    public static String uuidV7String() {
        byte[] b = uuidV7Bytes(System.currentTimeMillis());
        long msb = 0L;
        long lsb = 0L;
        for (int i = 0; i < 8;  i++) {
            msb = (msb << 8) | (b[i] & 0xFFL);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (b[i] & 0xFFL);
        }
        return new UUID(msb, lsb).toString();
    }

    public byte[] traceId()        { return traceId; }
    public String traceIdHex()     { return HexFormat.of().formatHex(traceId); }
    public long originTsMs()       { return originTsMs; }
    public String originService()  { return originService; }
    public String pipeline()       { return pipeline; }
    public List<Hop> hops()        { return Collections.unmodifiableList(hops); }
    public boolean truncated()     { return truncated; }

    public Isotope appendHop(Hop hop) {
        if (hops.size() >= MAX_HOPS) {
            hops.remove(0);
            truncated = true;
        }
        hops.add(hop);
        return this;
    }

    public byte[] toJsonBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("failed to encode isotope as JSON", e);
        }
    }

    public static Isotope fromJsonBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, Isotope.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode isotope from JSON", e);
        }
    }

    /** Returns the isotope carried by the given headers, or {@code null} if absent. */
    public static Isotope fromHeaders(Headers headers) {
        if (headers == null) {
            return null;
        }
        Header h = headers.lastHeader(HEADER_KEY);
        return h == null ? null : fromJsonBytes(h.value());
    }

    public record Hop(
        @JsonProperty("s") String service,
        @JsonProperty("t") String topic,
        @JsonProperty("m") long tsMs
    ) {}
}