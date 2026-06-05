/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Micrometer emission for the three <em>stateless-aggregation</em> isotope
 * reports, as a metrics-native alternative to the Flink {@code latency_1m},
 * {@code topology_1m}, and {@code hop_distribution_1m} SQL jobs.
 *
 * <h2>Why this exists</h2>
 * Those three reports are pure scalar aggregation keyed on bounded-cardinality
 * dimensions (service / topic / hop_count — never {@code trace_id}), so they
 * don't need a stream processor: the producer interceptor already has every
 * value in scope on each {@code send()}, and Prometheus can do the 1-minute
 * windowing at query time ({@code rate()}/{@code increase()}). The remaining
 * four reports (percentiles-merged, coverage, bipartite topology, stuck-trace)
 * are per-{@code trace_id} stateful or absence-of-event problems that Prometheus
 * can't express, so they stay in Flink.
 *
 * <h2>What it emits</h2>
 * One {@link Timer} and one {@link Counter}, both registered on the
 * exporter's dedicated Prometheus registry:
 * <ul>
 *   <li>{@value #LATENCY_TIMER} tagged {@code (pipeline, origin_service,
 *       this_service, this_topic)} — records origin&rarr;hop latency. Its
 *       {@code count}/{@code sum}/{@code max} cover both the latency report
 *       (avg, max) <em>and</em> the topology produce-edge record count (the
 *       count, grouped down to {@code this_service}).</li>
 *   <li>{@value #HOP_RECORDS} tagged {@code (pipeline, this_topic, hop_count)} —
 *       one increment per produced record, giving the hop distribution.
 *       {@code hop_count} is bounded by {@link Isotope#MAX_HOPS}, so it's safe
 *       as a tag.</li>
 * </ul>
 *
 * <h2>Two deliberate gaps vs. the Flink reports</h2>
 * <ul>
 *   <li><b>No {@code distinct_traces}.</b> A counter can't dedup and
 *       {@code trace_id} is unbounded-cardinality, so the
 *       {@code COUNT(DISTINCT trace_id)} columns have no Prometheus equivalent.</li>
 *   <li><b>No windowed {@code min} latency.</b> A {@code Timer} exposes max but
 *       not a per-window min; avg and max port cleanly, min does not.</li>
 * </ul>
 *
 * <p>Emission is a no-op until {@link #start(int)} binds the Prometheus
 * registry, so the interceptor is free to call {@link #recordHop} unguarded on
 * every send when metrics are off — see {@link #isEnabled()}.
 */
public final class IsotopeMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(IsotopeMetrics.class);

    /**
     * Per-hop latency timer (origin&rarr;this-hop). Its {@code count} also
     * serves as the topology produce-edge record count.
     */
    static final String LATENCY_TIMER = "isotope.hop.latency";

    /** Records per {@code (pipeline, this_topic, hop_count)} — hop distribution. */
    static final String HOP_RECORDS = "isotope.hop.records";

    // The exporter owns a dedicated Prometheus registry rather than the
    // process-global one: the meters here are the only thing on the /metrics
    // endpoint, and a self-contained registry keeps binding/teardown (and the
    // unit test) trivial. Both fields are volatile — the producer interceptor
    // reads {@code registry} from Kafka's send threads.
    private static volatile PrometheusMeterRegistry registry;
    private static volatile HttpServer server;

    private IsotopeMetrics() {}

    /** True once the registry is bound; gates emission in {@link #recordHop}. */
    public static boolean isEnabled() {
        return registry != null;
    }

    /**
     * Idempotently binds the Prometheus registry and serves it at
     * {@code GET /metrics} on {@code port}. Safe to call more than once (e.g.
     * from multiple modes) — only the first call opens the listener.
     *
     * <p>The exporter is an optional sidecar, so a failure to bind {@code port}
     * (commonly a sibling stage already holding the default 9404) is logged at
     * WARN and swallowed — the registry is left unbound ({@link #isEnabled()}
     * stays false, {@link #recordHop} stays a no-op) and the caller's data
     * pipeline runs on unaffected. It does <em>not</em> throw.
     */
    public static synchronized void start(int port) {
        if (server != null) return;

        // Open the listener BEFORE binding the registry, so a bind failure
        // leaves nothing half-wired (no registry accumulating meters that
        // nothing can scrape).
        HttpServer s;
        try {
            s = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            LOG.warn("isotope metrics exporter NOT started: port {} unavailable ({}). "
                + "Another stage may already own it — set -Dmetrics.prometheus.port "
                + "to a free port. Continuing without metrics.", port, e.toString());
            return;
        }

        ensureRegistry();
        s.createContext("/metrics", exchange -> {
            byte[] body = scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders()
                .add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        s.start();
        server = s;
        LOG.info("isotope metrics exporter listening on http://0.0.0.0:{}/metrics", port);
    }

    /** Binds the Prometheus registry without serving HTTP. Idempotent. */
    static synchronized void ensureRegistry() {
        if (registry == null) {
            registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        }
    }

    /** Current Prometheus exposition text, or {@code ""} before the registry binds. */
    static String scrape() {
        PrometheusMeterRegistry r = registry;
        return r == null ? "" : r.scrape();
    }

    /**
     * Emits the stateless-aggregation metrics for one produced hop. No-op
     * unless the exporter has been bound (via {@link #start(int)}).
     *
     * @param pipeline       the trace's pipeline (origin-set, forwarded)
     * @param originService  the service that originated the trace
     * @param thisService    the service producing this hop
     * @param thisTopic      the topic this hop is produced to
     * @param latencyMs      {@code thisHopTs - originTs}; clamped at 0 to absorb
     *                       cross-service clock skew
     * @param hopCount       number of hops accumulated including this one
     */
    public static void recordHop(String pipeline, String originService,
            String thisService, String thisTopic, long latencyMs, int hopCount) {
        PrometheusMeterRegistry r = registry;
        if (r == null) return;

        Timer.builder(LATENCY_TIMER)
            .tag("pipeline",       pipeline)
            .tag("origin_service", originService)
            .tag("this_service",   thisService)
            .tag("this_topic",     thisTopic)
            .register(r)
            .record(Math.max(0L, latencyMs), TimeUnit.MILLISECONDS);

        Counter.builder(HOP_RECORDS)
            .tag("pipeline",   pipeline)
            .tag("this_topic", thisTopic)
            .tag("hop_count",  Integer.toString(hopCount))
            .register(r)
            .increment();
    }

    /** Test-only: stop the server, close the registry, and reset all state. */
    static synchronized void resetForTest() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
        if (registry != null) {
            registry.close();
            registry = null;
        }
    }
}
