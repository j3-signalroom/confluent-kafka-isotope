/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * State-level provenance collector — a version chain per entity, with each
 * version naming the versions it was derived from.
 *
 * <h2>What this is for</h2>
 * The other two collectors here record <em>message</em> lineage: an isotope is
 * an itinerary, one identity and its ordered hops, and
 * {@code ISOTOPE_APPEND_HOP} is restricted to 1:1 statements because a path is
 * a truthful derivation record only while every step has one parent
 * (docs/flink-collector.md 3.1). A row in an upsert table has no itinerary. Its
 * current value is the fold of every change that touched it — a DAG over
 * versions, not a path over messages — and asking whether a {@code -U}/{@code +U}
 * pair is one hop or two has no good answer because the question is wrong.
 *
 * <p>So this function does not stamp hops. It publishes one record per emitted
 * state, identified by {@link StateVersion} and carrying the versions it came
 * from. The two models coexist on the same pipeline without either lying: the
 * hop list stays a message-level artifact riding the log, and provenance is a
 * parallel record keyed by version.
 *
 * <h2>Why one operator, and not two statements</h2>
 * The windowed merge collector emits the merged record and its edge rows from
 * two statements that must independently agree on a derived ID, which is
 * fragile in a way {@code 81_merge_edge_markers.fql} documents at length. Here
 * the parent set is a <em>column of the record it describes</em>, produced by
 * one operator from one piece of state, so drift between an output and its
 * parents is not expressible. A flat edge table, if wanted, is an {@code UNNEST}
 * projection of this topic — downstream of a record that is already consistent.
 *
 * <h2>Why the input must be append-only</h2>
 * The input is the physical log, read in append mode, not an upsert view of it.
 * Every restriction Flink places on updating streams — window TVFs refuse them,
 * time attributes do not survive them, plain sinks reject them — is avoided by
 * rebuilding entity state here instead of asking the planner to do it. That is
 * the trade: this function owns the state machine {@code upsert-kafka} would
 * otherwise run, and in exchange nothing downstream ever sees a changelog.
 *
 * <h2>CCAF state constraints</h2>
 * {@link EntityState} deliberately uses plain {@code String}/{@code List}/{@code Map}
 * fields, each with a default. CCAF rejects {@code MapView}/{@code ListView} in PTF state (a plain
 * {@code Map}/{@code List} is the documented replacement) and still rejects a
 * {@code byte[]} map <em>value</em>, where a Base64 {@code String} works. Both
 * fail at {@code CREATE FUNCTION} time rather than at runtime, so a green run
 * on CP proves nothing about CCAF. See docs/state-provenance.md 5.0.
 */
@FunctionHint(output = @DataTypeHint("ROW<"
    + "version_id STRING, entity_key STRING, source_name STRING, op STRING, "
    + "parents ARRAY<STRING>, parent_overflow INT, emitted_at BIGINT>"))
public class StateProvenancePTF extends ProcessTableFunction<Row> {

    /**
     * Inline parent-set cap. Entity-scoped derivations sit far below this; a
     * global aggregate would blow through it, and that case wants a count and a
     * sketch rather than the members. Overflow is reported, never silently
     * dropped.
     */
    private static final int PARENT_CAP = 512;

    /**
     * Separator inside an {@link EntityState#unpublished} value. NUL cannot
     * occur in a topic name or in {@code op}, the same reasoning
     * {@link StateVersion} applies to its preimage.
     */
    private static final String SEP = "\u0000";

    /** Encodes a null {@code source_name} inside an unpublished value. */
    private static final String NULL_SOURCE = "\u0001";

    /**
     * Per-entity state. Mutated in place during {@link #eval} and
     * {@link #onTimer}; the framework persists it across invocations. Plain
     * fields with defaults only — see the class javadoc on CCAF's PTF state rules.
     */
    public static class EntityState {
        /** The partition key, captured once so {@link #onTimer} (no input row) can emit it. */
        public String entityKey;
        /** Version ID of the last state published for this entity. */
        public String currentVersionId;
        /** Input versions folded in since that publication. */
        public List<String> pendingParents = new ArrayList<>();
        /** Parents dropped past {@link #PARENT_CAP} since that publication. */
        public Integer pendingOverflow = 0;
        /**
         * Versions seen but not yet published, keyed by version ID; the value is
         * {@code emitted_at SEP op SEP source_name}. A plain {@code Map} with a
         * scalar {@code String} value — the shape CCAF accepts (see the class
         * javadoc). Published in event-time order by {@link #onTimer}.
         */
        public Map<String, String> unpublished = new HashMap<>();
    }

    /**
     * Parks the version until the watermark passes its event time.
     *
     * <p>Publishing here — as this function once did — chains versions in
     * <em>arrival</em> order. {@code entity_log} is a {@code UNION ALL} of one
     * table per topic, so an entity's records reach this operator in whatever
     * order the three source readers happen to deliver them, and the chain came
     * out as {@code enriched <- fulfilled <- placed} as often as the true
     * {@code placed <- enriched <- fulfilled}. {@code REQUIRE_ON_TIME} gives the
     * function a time; it does not sort its input. The event-time timer does:
     * it fires only once the watermark guarantees nothing earlier is still in
     * flight.
     */
    public void eval(
            Context ctx,
            @StateHint EntityState state,
            @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                Row input) {

        final Instant eventTime = ctx.timeContext(Instant.class).time();
        final long    eventMs   = eventTime.toEpochMilli();

        final String entityKey  = input.getFieldAs("entity_key");
        final String sourceName = input.getFieldAs("source_name");
        final byte[] content    = input.getFieldAs("content");
        final String op         = input.getFieldAs("op");

        final String versionId =
                StateVersion.versionIdHex(sourceName, entityKey, content, eventMs);

        // Idempotence, not an optimization. The same bytes at the same event
        // time are the same state, so a redelivered record must not append a
        // second, self-referential version to the chain — whether the first
        // copy is already published or still waiting for its timer.
        if (versionId.equals(state.currentVersionId) || state.unpublished.containsKey(versionId)) {
            return;
        }

        state.entityKey = entityKey;
        state.unpublished.put(versionId,
                eventMs + SEP + op + SEP + (sourceName == null ? NULL_SOURCE : sourceName));
        // Unnamed timers coalesce per timestamp, so versions sharing an event
        // time share one firing. A record already behind the watermark gets a
        // timer in the past, which fires on the next watermark: late versions
        // are still published, after everything already on the chain.
        ctx.timeContext(Instant.class).registerOnTime(eventTime);
    }

    /**
     * Publishes every parked version at or before the firing time, oldest
     * first. Version IDs lead with 48 bits of event time, so their hex sorts
     * chronologically, and ties at one millisecond break on the content digest
     * — the same order on every run and every runtime.
     */
    public void onTimer(OnTimerContext ctx, EntityState state) {
        final long firedMs = ctx.timeContext(Instant.class).time().toEpochMilli();

        final List<String> due = new ArrayList<>();
        for (Map.Entry<String, String> e : state.unpublished.entrySet()) {
            if (Long.parseLong(e.getValue().substring(0, e.getValue().indexOf(SEP))) <= firedMs) {
                due.add(e.getKey());
            }
        }
        Collections.sort(due);

        for (String versionId : due) {
            final String[] f = state.unpublished.remove(versionId).split(SEP, 3);
            publish(state, versionId, Long.parseLong(f[0]), f[1], NULL_SOURCE.equals(f[2]) ? null : f[2]);
        }
    }

    private void publish(EntityState state, String versionId, long eventMs, String op, String sourceName) {
        // The state this version supersedes is its parent. A fan-in stage folds
        // several inputs before publishing, which is why this is a set rather
        // than a single column; the demo pipeline is 1:1, so it holds one.
        if (state.currentVersionId != null
                && !state.pendingParents.contains(state.currentVersionId)) {
            if (state.pendingParents.size() < PARENT_CAP) {
                state.pendingParents.add(state.currentVersionId);
            } else {
                state.pendingOverflow = state.pendingOverflow + 1;
            }
        }

        collect(Row.of(
                versionId,
                state.entityKey,
                sourceName,
                op,
                state.pendingParents.toArray(new String[0]),
                state.pendingOverflow,
                eventMs));

        state.currentVersionId = versionId;
        state.pendingParents   = new ArrayList<>();
        state.pendingOverflow  = 0;
    }
}
