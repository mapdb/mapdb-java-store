package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link Utf8#compareUtf8} must be sign-identical to {@link String#compareTo}
 * (UTF-16 code-unit order) — including the supplementary-character range where
 * raw UTF-8 byte order (code-point order) DISAGREES with compareTo — and must
 * obey the torn-read discipline: bounded, exception on malformed input, never
 * reading past byteLen.
 */
public class Utf8CompareTest {

    /**
     * Corpus crossing every interesting boundary: ASCII, Latin-1, CJK,
     * halfwidth forms (U+FF61), supplementary (U+10000, emoji) — U+FF61 vs
     * U+10000 is the canonical pair where UTF-8 order (U+FF61 first) differs
     * from UTF-16 order (U+FF61 last) — empty strings, shared prefixes.
     */
    private static final String[] CORPUS = {
            "", "a", "ab", "abc", "b", "B", "0", "~",
            "é", "à", "eé", "ÿ",
            "你", "你好", "好",
            "｡",                       // U+FF61 halfwidth ideographic full stop
            "𐀀",                 // U+10000, the first supplementary code point
            "😀", "😀a", "👍", "🎉",      // supplementary (emoji)
            "prefix", "prefixa", "prefix｡", "prefix😀", "prefix𐀀",
            "p", "pre", "prefiy",
            "￿", "퟿", "",   // BMP extremes around the surrogate block
    };

    private interface InFactory { DataInput2 make(byte[] b, int pos); }

    private static final InFactory[] FACTORIES = {
            DataInput2.ByteArray::new,
            (b, pos) -> {
                ByteBuffer bb = ByteBuffer.allocate(b.length);
                bb.put(b); bb.clear();
                return new DataInput2.ByteBuf(bb, pos);
            },
            (b, pos) -> {
                ByteBuffer bb = ByteBuffer.allocateDirect(b.length);
                bb.put(b); bb.clear();
                return new DataInput2.ByteBuf(bb, pos);
            },
    };

    private static int compare(String stored, String key, InFactory f) {
        byte[] utf8 = stored.getBytes(StandardCharsets.UTF_8);
        byte[] framed = new byte[utf8.length + 8]; // trailing garbage that must never be read
        System.arraycopy(utf8, 0, framed, 0, utf8.length);
        for (int i = utf8.length; i < framed.length; i++) framed[i] = (byte) 0x81; // bad continuation
        DataInput2 in = f.make(framed, 0);
        int c = Utf8.compareUtf8(in, utf8.length, key);
        assertTrue("must consume at most byteLen bytes", in.pos() <= utf8.length);
        return c;
    }

    @Test
    public void signIdenticalToStringCompareTo_fullCorpusCrossProduct() {
        for (String stored : CORPUS) {
            for (String key : CORPUS) {
                int expected = Integer.signum(stored.compareTo(key));
                for (InFactory f : FACTORIES) {
                    assertEquals("compareUtf8(\"" + stored + "\", \"" + key + "\")",
                            expected, Integer.signum(compare(stored, key, f)));
                }
            }
        }
    }

    @Test
    public void supplementaryVsBmp_distinguishesUtf8FromUtf16Order() {
        // U+FF61 > U+10000 in UTF-16 (0xFF61 > 0xD800) though its UTF-8 bytes
        // (EF BD A1) sort BEFORE U+10000's (F0 90 80 80): byte order would say -1
        String bmp = "｡", supp = "𐀀";
        assertTrue(bmp.compareTo(supp) > 0);
        assertTrue(java.util.Arrays.compareUnsigned(
                bmp.getBytes(StandardCharsets.UTF_8), supp.getBytes(StandardCharsets.UTF_8)) < 0);
        for (InFactory f : FACTORIES) {
            assertTrue(compare(bmp, supp, f) > 0);
            assertTrue(compare(supp, bmp, f) < 0);
        }
    }

    @Test
    public void randomizedAgainstCompareTo() {
        Random rnd = new Random(0x0704);
        for (int i = 0; i < 3000; i++) {
            String a = randomString(rnd), b = rnd.nextInt(4) == 0 ? a : randomString(rnd);
            assertEquals("\"" + a + "\" vs \"" + b + "\"",
                    Integer.signum(a.compareTo(b)),
                    Integer.signum(compare(a, b, FACTORIES[0])));
        }
    }

    private static String randomString(Random rnd) {
        int len = rnd.nextInt(8);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            switch (rnd.nextInt(5)) {
                case 0 -> sb.append((char) ('a' + rnd.nextInt(26)));
                case 1 -> sb.append((char) (0x80 + rnd.nextInt(0x80)));      // Latin-1 supplement
                case 2 -> sb.append((char) (0x4E00 + rnd.nextInt(0x100)));   // CJK
                case 3 -> sb.appendCodePoint(0x10000 + rnd.nextInt(0x1000)); // supplementary
                case 4 -> sb.append((char) (0xFF00 + rnd.nextInt(0xF0)));    // halfwidth forms
            }
        }
        return sb.toString();
    }

    // ---- torn-read discipline -------------------------------------------------

    private static void assertCorrupt(byte[] bytes, int byteLen) {
        for (InFactory f : FACTORIES) {
            try {
                Utf8.compareUtf8(f.make(bytes, 0), byteLen, "probe");
                fail("expected IllegalStateException for " + java.util.Arrays.toString(bytes));
            } catch (IllegalStateException expected) { /* ok */ }
        }
    }

    @Test
    public void tornBytes_failFastNeverOverrun() {
        assertCorrupt(new byte[]{(byte) 0xFF}, 1);                       // invalid lead
        assertCorrupt(new byte[]{(byte) 0xF8}, 1);                       // 5-byte lead (invalid)
        assertCorrupt(new byte[]{(byte) 0x80}, 1);                       // bare continuation
        assertCorrupt(new byte[]{(byte) 0xC3}, 1);                       // truncated 2-byte seq
        assertCorrupt(new byte[]{(byte) 0xE4, (byte) 0xBD}, 2);          // truncated 3-byte seq
        assertCorrupt(new byte[]{(byte) 0xF0, (byte) 0x90, (byte) 0x80}, 3); // truncated 4-byte seq
        assertCorrupt(new byte[]{(byte) 0xC3, 0x41}, 2);                 // bad continuation byte
        assertCorrupt(new byte[]{(byte) 0xF4, (byte) 0xBF, (byte) 0xBF, (byte) 0xBF}, 4); // > U+10FFFF
        // RFC 3629 well-formedness: overlong forms and surrogate
        // code points are malformed — must fail fast, not decode to a comparable key
        assertCorrupt(new byte[]{(byte) 0xC0, (byte) 0x80}, 2);          // overlong U+0000 (2-byte)
        assertCorrupt(new byte[]{(byte) 0xC1, (byte) 0xBF}, 2);          // overlong U+007F (2-byte)
        assertCorrupt(new byte[]{(byte) 0xE0, (byte) 0x80, (byte) 0x80}, 3); // overlong U+0000 (3-byte)
        assertCorrupt(new byte[]{(byte) 0xED, (byte) 0xA0, (byte) 0x80}, 3); // U+D800 surrogate
        assertCorrupt(new byte[]{(byte) 0xED, (byte) 0xBF, (byte) 0xBF}, 3); // U+DFFF surrogate
        assertCorrupt(new byte[]{(byte) 0xF0, (byte) 0x80, (byte) 0x80, (byte) 0x80}, 4); // overlong U+0000 (4-byte)
        assertCorrupt(new byte[]{(byte) 0xF0, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF}, 4); // overlong U+FFFF (4-byte)
        // truncation must throw even when VALID continuation bytes sit just past byteLen
        byte[] full = "你".getBytes(StandardCharsets.UTF_8); // E4 BD A0
        assertCorrupt(full, 2);
        assertCorrupt(full, 1);
        // negative length
        for (InFactory f : FACTORIES) {
            try {
                Utf8.compareUtf8(f.make(new byte[]{0x41}, 0), -1, "probe");
                fail("expected IllegalStateException for negative length");
            } catch (IllegalStateException expected) { /* ok */ }
        }
    }
}
