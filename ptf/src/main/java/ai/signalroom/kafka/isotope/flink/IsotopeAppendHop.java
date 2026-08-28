/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import ai.signalroom.kafka.isotope.Isotope;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Collector-side propagation for Flink — the SQL-native counterpart to
 * {@code IsotopeProducerInterceptor}.
 *
 * <h2>Why a second propagation model</h2>
 * The interceptor propagates <em>ambiently</em>: {@code IsotopeContext} holds
 * the in-flight {@link Isotope} in a {@link ThreadLocal}, and
 * {@code onSend()} reads it on the same thread that called {@code send()}.
 * That model is correct for the services in {@code app/}, where one thread
 * owns a whole consume→produce hop, and it is left untouched.
 *
 * It cannot work in Flink. A shuffle serializes a record and resumes it on a
 * different thread — usually in a different TaskManager JVM — so nothing set
 * beside the record survives. Flink SQL makes that stricter still: there is no
 * user-visible thread to attach to, and no way to smuggle an object past the
 * planner.
 *
 * So Flink propagates <em>in-band</em>: the isotope rides in the record's own
 * Kafka headers, exposed to SQL as a writable {@code headers} metadata column,
 * and this function is the hop-append step. Two models, one wire format —
 * every header this writes is byte-identical to what the interceptor writes,
 * so {@code 05_isotope_view.fql} and all seven reports read Flink-produced
 * records without knowing a second collector exists.
 *
 * <h2>SQL usage</h2>
 * The target table's headers column must be persisted (writable) rather than
 * VIRTUAL, and typed as {@code MAP<STRING, STRING>} — CCAF's native header
 * type is {@code MAP<BYTES, BYTES>} and it implicitly casts:
 *
 * <pre>
 *   ALTER TABLE `orders.enriched`
 *       MODIFY `headers` MAP&lt;STRING, STRING&gt; METADATA;
 * </pre>
 *
 * Then the hop is appended on the way out. Note the headers column becomes
 * mandatory in INSERT INTO once persisted:
 *
 * <pre>
 *   INSERT INTO `orders.enriched`
 *   SELECT
 *       payload,
 *       ISOTOPE_APPEND_HOP(
 *           `headers`,
 *           'flink-enrich',                    -- this_service
 *           'orders.enriched',                 -- this_topic
 *           'orders',                          -- pipeline (origin mint only)
 *           UNIX_TIMESTAMP() * 1000)           -- hop timestamp, ms
 *   FROM `orders.placed`;
 * </pre>
 *
 * <h2>Determinism</h2>
 * The hop timestamp is an <em>argument</em> rather than a {@code
 * System.currentTimeMillis()} call inside {@link #eval}. That keeps the
 * function deterministic — Flink assumes UDFs are, and is free to re-evaluate
 * or reorder them; a clock read inside would silently produce different hop
 * timestamps on a replay.
 *
 * @see StuckTracePTF the interpreter-side counterpart, which reads these headers
 */
public class IsotopeAppendHop extends ScalarFunction {

    /**
     * Appends one produce-edge hop to the isotope carried in {@code headers}.
     *
     * @param headers   inbound headers, as read from the source table's
     *                  {@code headers} metadata column. May be null (a topic
     *                  written by an untraced producer).
     * @param service   the logical service name for this Flink statement —
     *                  the interceptor's {@code SERVICE_NAME_CONFIG} analogue.
     * @param topic     the topic this record is being produced to.
     * @param pipeline  pipeline name, used only when minting a fresh trace.
     * @param hopTsMs   hop timestamp in epoch millis; pass from SQL, never read
     *                  the clock here (see class javadoc).
     * @return a new header map: every inbound header preserved, with the
     *         isotope JSON and the seven scalar reporting headers rewritten.
     */
    public @DataTypeHint("MAP<STRING, STRING>") Map<String, String> eval(
            @DataTypeHint("MAP<STRING, STRING>") Map<String, String> headers,
            String service,
            String topic,
            String pipeline,
            Long hopTsMs) {

        final long tsMs = hopTsMs == null ? 0L : hopTsMs;

        // Rehydrate the inbound trace, or mint one if this record entered the
        // system untraced — Flink is then the origin, exactly as a service
        // would be on its first send().
        Isotope iso = null;
        if (headers != null) {
            String json = headers.get(Isotope.HEADER_KEY);
            if (json != null && !json.isEmpty()) {
                iso = Isotope.fromJsonBytes(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        if (iso == null) {
            iso = Isotope.newTrace(service, pipeline, tsMs);
        }

        iso = iso.appendHop(new Isotope.Hop(service, topic, tsMs));

        // Preserve unrelated headers — this function owns the x-isotope-* keys
        // and nothing else. LinkedHashMap keeps ordering stable for tests.
        Map<String, String> out = new LinkedHashMap<>();
        if (headers != null) out.putAll(headers);

        out.put(Isotope.HEADER_KEY,            new String(iso.toJsonBytes(),
                                                   java.nio.charset.StandardCharsets.UTF_8));
        out.put(Isotope.HEADER_TRACE_ID,       iso.traceIdHex());
        out.put(Isotope.HEADER_ORIGIN_TS,      Long.toString(iso.originTsMs()));
        out.put(Isotope.HEADER_ORIGIN_SERVICE, iso.originService());
        out.put(Isotope.HEADER_PIPELINE,       iso.pipeline());
        out.put(Isotope.HEADER_THIS_SERVICE,   service);
        out.put(Isotope.HEADER_THIS_TOPIC,     topic);
        out.put(Isotope.HEADER_HOP_COUNT,      Integer.toString(iso.hops().size()));

        return out;
    }
}
