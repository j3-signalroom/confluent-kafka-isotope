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
import java.util.List;

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
 * {@link EntityState} deliberately uses plain {@code String}/{@code List}
 * fields. CCAF rejects {@code MapView}/{@code ListView} in PTF state (a plain
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
     * Per-entity state. Mutated in place during {@link #eval}; the framework
     * persists it across invocations. Plain fields only — see the class javadoc
     * on CCAF's PTF state rules.
     */
    public static class EntityState {
        /** Version ID of the last state published for this entity. */
        public String currentVersionId;
        /** Input versions folded in since that publication. */
        public List<String> pendingParents;
        /** Parents dropped past {@link #PARENT_CAP} since that publication. */
        public Integer pendingOverflow;
    }

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
        // second, self-referential version to the chain.
        if (versionId.equals(state.currentVersionId)) {
            return;
        }

        if (state.pendingParents == null) {
            state.pendingParents = new ArrayList<>();
            state.pendingOverflow = 0;
        }

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
                entityKey,
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
