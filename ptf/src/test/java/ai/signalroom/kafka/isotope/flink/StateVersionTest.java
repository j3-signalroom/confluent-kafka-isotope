/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.flink;

import ai.signalroom.kafka.isotope.Isotope;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The state-provenance contract, pinned.
 *
 * <p>{@link StateVersion}'s preimage is a wire contract, not an implementation
 * detail: every version ID ever published is a function of it, so a change to
 * field order, the separator, or the null encoding renumbers history and
 * silently breaks every parent link already written to Kafka. These tests are
 * what make that a build failure.
 */
class StateVersionTest {

    private static final long T0 = 1781291101966L;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("same content at the same event time is the same version")
    void deterministic() {
        assertEquals(
                StateVersion.versionIdHex("orders.placed", "k1", bytes("v"), T0),
                StateVersion.versionIdHex("orders.placed", "k1", bytes("v"), T0));
    }

    @Test
    @DisplayName("content changes the version")
    void contentAddressed() {
        assertNotEquals(
                StateVersion.versionIdHex("orders.placed", "k1", bytes("v1"), T0),
                StateVersion.versionIdHex("orders.placed", "k1", bytes("v2"), T0));
    }

    @Test
    @DisplayName("the same payload on two tables is two versions, not one seen twice")
    void sourceNameIsPartOfIdentity() {
        assertNotEquals(
                StateVersion.versionIdHex("orders.placed", "k1", bytes("v"), T0),
                StateVersion.versionIdHex("orders.enriched", "k1", bytes("v"), T0));
    }

    @Test
    @DisplayName("entity key separates versions")
    void entityKeySeparates() {
        assertNotEquals(
                StateVersion.versionIdHex("orders.placed", "k1", bytes("v"), T0),
                StateVersion.versionIdHex("orders.placed", "k2", bytes("v"), T0));
    }

    @Test
    @DisplayName("a later event time is a distinct version of identical content")
    void eventTimeSeparates() {
        assertNotEquals(
                StateVersion.versionIdHex("orders.placed", "k1", bytes("v"), T0),
                StateVersion.versionIdHex("orders.placed", "k1", bytes("v"), T0 + 1));
    }

    @Test
    @DisplayName("separator cannot be forged by concatenating field values")
    void separatorIsNotForgeable() {
        // Without a separator that cannot occur in the fields, ("ab","c") and
        // ("a","bc") would hash identically and two entities would share a
        // version chain.
        assertNotEquals(
                StateVersion.versionIdHex("ab", "c", bytes("v"), T0),
                StateVersion.versionIdHex("a", "bc", bytes("v"), T0));
    }

    @Test
    @DisplayName("a null field is distinct from an empty one")
    void nullIsNotEmpty() {
        assertNotEquals(
                StateVersion.versionIdHex(null, "k1", bytes("v"), T0),
                StateVersion.versionIdHex("", "k1", bytes("v"), T0));
    }

    @Test
    @DisplayName("a tombstone is a state, and a stable one")
    void tombstoneHasAVersion() {
        String a = StateVersion.versionIdHex("orders.placed", "k1", null, T0);
        String b = StateVersion.versionIdHex("orders.placed", "k1", null, T0);
        assertEquals(a, b);
        assertNotEquals(a, StateVersion.versionIdHex("orders.placed", "k1", bytes(""), T0));
    }

    @Test
    @DisplayName("the ID is a valid UUIDv7 carrying the event time, like every other identity here")
    void isAUuidV7() {
        byte[] id = StateVersion.versionId("orders.placed", "k1", bytes("v"), T0);
        assertEquals(Isotope.TRACE_ID_BYTES, id.length);
        assertEquals(0x70, id[6] & 0xF0, "version nibble must be 7");
        assertEquals(0x80, id[8] & 0xC0, "variant bits must be 10");

        long ts = 0L;
        for (int i = 0; i < 6; i++) {
            ts = (ts << 8) | (id[i] & 0xFFL);
        }
        assertEquals(T0, ts, "high 48 bits must carry the event-time millis");
    }

    /** Confluent wire format: magic byte, 4-byte big-endian schema ID, payload. */
    private static byte[] framed(int schemaId, byte[] payload) {
        byte[] out = new byte[5 + payload.length];
        out[0] = 0x00;
        out[1] = (byte) (schemaId >>> 24);
        out[2] = (byte) (schemaId >>> 16);
        out[3] = (byte) (schemaId >>> 8);
        out[4] = (byte)  schemaId;
        System.arraycopy(payload, 0, out, 5, payload.length);
        return out;
    }

    @Test
    @DisplayName("the registry's schema ID is not part of identity — CP and CCAF agree")
    void schemaIdIsNotContent() {
        // Same record, two registries: CP's SR and Confluent Cloud's assign
        // different IDs to the same schema. Protobuf's message-index byte (0x00
        // for the first message) is schema-derived and rides in the payload.
        byte[] payload = {0x00, 0x0A, 0x03, 'a', 'b', 'c'};
        assertEquals(
                StateVersion.versionIdHex("orders.placed", "k1", framed(100001, payload), T0),
                StateVersion.versionIdHex("orders.placed", "k1", framed(7, payload), T0));
    }

    @Test
    @DisplayName("a framed value and its bare payload are the same version")
    void framingIsStripped() {
        // What a header-mode producer (schema ID in a Kafka header) writes.
        byte[] payload = {0x0A, 0x03, 'a', 'b', 'c'};
        assertEquals(
                StateVersion.versionIdHex("orders.placed", "k1", framed(42, payload), T0),
                StateVersion.versionIdHex("orders.placed", "k1", payload, T0));
        assertArrayEquals(payload, StateVersion.canonicalContent(framed(42, payload)));
    }

    @Test
    @DisplayName("the payload after the prefix still addresses the version")
    void payloadAfterPrefixIsContent() {
        assertNotEquals(
                StateVersion.versionIdHex("orders.placed", "k1", framed(42, bytes("v1")), T0),
                StateVersion.versionIdHex("orders.placed", "k1", framed(42, bytes("v2")), T0));
    }

    @Test
    @DisplayName("only the registry shape is stripped — anything else hashes whole")
    void unframedContentIsUntouched() {
        // No magic byte, and a leading 0x00 too short to carry a schema ID.
        byte[] plain = bytes("v");
        byte[] shortZero = {0x00, 0x01, 0x02, 0x03};
        assertSame(plain, StateVersion.canonicalContent(plain));
        assertSame(shortZero, StateVersion.canonicalContent(shortZero));
        assertNotEquals(
                StateVersion.versionIdHex("orders.placed", "k1", shortZero, T0),
                StateVersion.versionIdHex("orders.placed", "k1", new byte[0], T0));
        assertNull(StateVersion.canonicalContent(null));
    }

    @Test
    @DisplayName("IDs sort chronologically, so a version chain sorts into order")
    void sortsChronologically() {
        String early = StateVersion.versionIdHex("orders.placed", "k1", bytes("v"), T0);
        String later = StateVersion.versionIdHex("orders.placed", "k1", bytes("v"), T0 + 60_000L);
        assertTrue(early.compareTo(later) < 0, early + " should sort before " + later);
    }
}
