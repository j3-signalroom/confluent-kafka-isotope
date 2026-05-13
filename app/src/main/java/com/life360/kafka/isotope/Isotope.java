package com.life360.kafka.isotope;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class Isotope {

    public static final String HEADER_KEY = "x-isotope";

    // Scalar reporting headers, written alongside HEADER_KEY by
    // IsotopeProducerInterceptor. All values are UTF-8 strings so that
    // Flink SQL can do `CAST(headers['x-isotope-…'] AS STRING)` (and
    // CAST(... AS BIGINT) for the numeric ones) without a UDF.
    public static final String HEADER_TRACE_ID       = "x-isotope-trace-id";
    public static final String HEADER_ORIGIN_TS      = "x-isotope-origin-ts";
    public static final String HEADER_ORIGIN_SERVICE = "x-isotope-origin-service";
    public static final String HEADER_THIS_SERVICE   = "x-isotope-this-service";
    public static final String HEADER_THIS_TOPIC     = "x-isotope-this-topic";
    public static final String HEADER_HOP_COUNT      = "x-isotope-hop-count";

    public static final int MAX_HOPS = 32;
    public static final int TRACE_ID_BYTES = 16;

    private static final ObjectMapper JSON = new ObjectMapper();

    @JsonProperty("t") private final byte[] traceId;
    @JsonProperty("o") private final long originTsMs;
    @JsonProperty("s") private final String originService;
    @JsonProperty("h") private final List<Hop> hops;
    @JsonProperty("x") private boolean truncated;

    @JsonCreator
    Isotope(
        @JsonProperty("t") byte[] traceId,
        @JsonProperty("o") long originTsMs,
        @JsonProperty("s") String originService,
        @JsonProperty("h") List<Hop> hops,
        @JsonProperty("x") boolean truncated
    ) {
        this.traceId = traceId;
        this.originTsMs = originTsMs;
        this.originService = originService;
        this.hops = hops != null ? new ArrayList<>(hops) : new ArrayList<>();
        this.truncated = truncated;
    }

    public static Isotope newTrace(String originService) {
        byte[] id = new byte[TRACE_ID_BYTES];
        ThreadLocalRandom.current().nextBytes(id);
        return new Isotope(id, System.currentTimeMillis(),
            Objects.requireNonNullElse(originService, "unknown"),
            new ArrayList<>(), false);
    }

    public byte[] traceId()        { return traceId; }
    public String traceIdHex()     { return HexFormat.of().formatHex(traceId); }
    public long originTsMs()       { return originTsMs; }
    public String originService()  { return originService; }
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
        if (headers == null) return null;
        Header h = headers.lastHeader(HEADER_KEY);
        return h == null ? null : fromJsonBytes(h.value());
    }

    public record Hop(
        @JsonProperty("s") String service,
        @JsonProperty("t") String topic,
        @JsonProperty("m") long tsMs
    ) {}
}
