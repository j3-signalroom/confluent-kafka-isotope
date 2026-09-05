/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import ai.signalroom.kafka.isotope.Isotope;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Content-addressed version identity for state-level provenance — the
 * counterpart to {@link MergeTrace} for derivations that have no window to key
 * on.
 *
 * <h2>Why content addressing</h2>
 * {@code ISOTOPE_MERGE_TRACE} derives a merged record's identity from its
 * window bounds, which works only because a tumbling window is a bounded,
 * deterministic, nameable input set. An unbounded {@code GROUP BY}, a regular
 * join, and a dedup offer none of those: the output row is revised as inputs
 * arrive, and each revision has a different parent set. Deriving identity from
 * the <em>content</em> of the emitted state removes the need for a window
 * entirely, and buys three properties that matter more here than they did for
 * the merge:
 *
 * <ul>
 *   <li><b>Deterministic across runtimes and replays.</b> The same content at
 *       the same event time yields the same ID on CP, on CCAF, and six months
 *       later during a backfill.</li>
 *   <li><b>Idempotent under at-least-once delivery.</b> A redelivered record
 *       carries the same Kafka timestamp and the same bytes, so recomputing its
 *       version yields the ID already published rather than a second version of
 *       the same state. {@link StateProvenancePTF} uses exactly that equality
 *       to suppress no-op emissions.</li>
 *   <li><b>Retraction becomes append.</b> A version is never mutated; a new one
 *       is published. That is what lets a provenance stream describing an
 *       <em>updating</em> table stay append-only end to end, so no operator in
 *       the pipeline has to consume a changelog.</li>
 * </ul>
 *
 * <h2>Layout — a real UUIDv7, like every other identity here</h2>
 * The high 48 bits carry the event-time milliseconds exactly as
 * {@code Isotope.uuidV7Bytes} and {@link MergeTrace} lay them out, so version
 * IDs sort chronologically and interleave correctly with interceptor-minted
 * trace IDs; the low bytes are the content digest. Version and variant bits are
 * patched, so these are valid UUIDv7s rather than merely 16 hashed bytes.
 *
 * <p>The consequence worth stating plainly: identity is
 * <em>(event time, content)</em>, not content alone. Two byte-identical states
 * at the same event time are the same version — the idempotence case that
 * actually matters. A row that revisits an earlier value at a later time is a
 * distinct version, which is what a version <em>chain</em> should say.
 *
 * <h2>The preimage is the contract</h2>
 * {@code sourceName} is part of the preimage, so the same payload observed on
 * two different tables is two versions — one derived from the other — rather
 * than one version seen twice. Field order, the NUL separator, and the null
 * encoding below are pinned by {@code StateVersionTest}; changing any of them
 * renumbers every version ever issued, so treat it the way {@link MergeTrace}
 * treats its own preimage.
 */
public final class StateVersion {

    /** Field separator for the digest preimage. Cannot occur in a table or key name. */
    private static final char SEP = '\0';

    /** Distinguishes a null field from an empty one in the preimage. */
    private static final String NULL_MARKER = "null";

    private StateVersion() {
    }

    /**
     * Derives the version ID for one emitted state.
     *
     * @param sourceName  the table (or topic) this state belongs to — part of
     *                    the preimage, so the same content on two tables is two
     *                    versions
     * @param entityKey   the upsert primary key this state is a value of
     * @param content     canonical bytes of the state; {@code null} for a
     *                    tombstone, which is a state ("deleted") like any other
     * @param eventTimeMs event time of the change that produced this state
     */
    public static byte[] versionId(String sourceName,
                                   String entityKey,
                                   byte[] content,
                                   long eventTimeMs) {

        final String prefix = nullSafe(sourceName) + SEP
                            + nullSafe(entityKey)  + SEP;

        final byte[] body = content == null
                ? NULL_MARKER.getBytes(StandardCharsets.UTF_8)
                : content;

        final MessageDigest md = sha256();
        md.update(prefix.getBytes(StandardCharsets.UTF_8));
        md.update(body);
        final byte[] digest = md.digest();

        final byte[] id = new byte[Isotope.TRACE_ID_BYTES];

        // Bytes 0..5 — 48-bit big-endian Unix-ms event time, so version IDs are
        // chronologically sortable and interleave with interceptor-minted ones.
        id[0] = (byte) (eventTimeMs >>> 40);
        id[1] = (byte) (eventTimeMs >>> 32);
        id[2] = (byte) (eventTimeMs >>> 24);
        id[3] = (byte) (eventTimeMs >>> 16);
        id[4] = (byte) (eventTimeMs >>> 8);
        id[5] = (byte)  eventTimeMs;

        // Bytes 6..15 — content digest standing in for the random field.
        System.arraycopy(digest, 0, id, 6, 10);

        // Version (7) and variant (10) patches — same as the RFC and the
        // upstream minter.
        id[6] = (byte) ((id[6] & 0x0F) | 0x70);
        id[8] = (byte) ((id[8] & 0x3F) | 0x80);

        return id;
    }

    /** {@link #versionId} as a lowercase hex string — the form written to Kafka. */
    public static String versionIdHex(String sourceName,
                                      String entityKey,
                                      byte[] content,
                                      long eventTimeMs) {
        return HexFormat.of().formatHex(
                versionId(sourceName, entityKey, content, eventTimeMs));
    }

    private static String nullSafe(String s) {
        return s == null ? NULL_MARKER : s;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory on every conforming JRE.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
