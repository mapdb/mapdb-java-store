package org.mapdb.ser;

import org.mapdb.io.DataInput2;

/**
 * In-place comparison of stored UTF-8 bytes against a probe {@link String},
 * EXACTLY {@link String#compareTo}-consistent, zero allocation.
 *
 * Raw UTF-8 byte order is Unicode CODE-POINT order, which differs from
 * String.compareTo (UTF-16 CODE-UNIT order) for supplementary characters:
 * e.g. U+FF61 sorts BEFORE U+10000 in UTF-8/code-point order but AFTER it in
 * UTF-16 order (0xFF61 &gt; 0xD800, the high surrogate). A memcmp over stored
 * UTF-8 would therefore disagree with the object-side String order. This
 * helper decodes the stored bytes incrementally, expands supplementary code
 * points to surrogate pairs, and compares char-by-char against the probe —
 * used by the String formats' byte-side search.
 *
 * Torn-read discipline: consumes AT MOST {@code byteLen} bytes,
 * every multi-byte sequence is length- and shape-checked and must be RFC 3629
 * well-formed (shortest form, no surrogate code points, ≤ U+10FFFF — the
 * serializers never produce anything else) — malformed input throws, never
 * overruns, never allocates, never loops unboundedly.
 */
final class Utf8 {

    private Utf8() {}

    /**
     * Sign of {@code stored.compareTo(key)} where {@code stored} is the UTF-8
     * string spanning exactly {@code byteLen} bytes at {@code in}'s position.
     * Consumes at most {@code byteLen} bytes (fewer on early difference; the
     * caller re-seeks).
     */
    static int compareUtf8(DataInput2 in, int byteLen, String key) {
        if (byteLen < 0) throw new IllegalStateException("corrupt utf8 length");
        int rem = byteLen;
        int ci = 0;
        final int keyLen = key.length();
        while (rem > 0) {
            int b0 = in.readByte() & 0xFF;
            rem--;
            int cp, need;
            if (b0 < 0x80) { cp = b0; need = 0; }
            else if ((b0 & 0xE0) == 0xC0) { cp = b0 & 0x1F; need = 1; }
            else if ((b0 & 0xF0) == 0xE0) { cp = b0 & 0x0F; need = 2; }
            else if ((b0 & 0xF8) == 0xF0) { cp = b0 & 0x07; need = 3; }
            else throw new IllegalStateException("corrupt utf8 lead byte");
            if (need > rem) throw new IllegalStateException("corrupt utf8 truncated sequence");
            for (int i = 0; i < need; i++) {
                int b = in.readByte() & 0xFF;
                if ((b & 0xC0) != 0x80) throw new IllegalStateException("corrupt utf8 continuation byte");
                cp = (cp << 6) | (b & 0x3F);
            }
            rem -= need;
            // RFC 3629 well-formedness: shortest form per sequence length, no
            // surrogate code points, nothing above U+10FFFF — torn/corrupt bytes
            // must fail fast, not decode to a legitimate-looking key
            switch (need) {
                case 1: if (cp < 0x80) throw new IllegalStateException("corrupt utf8 overlong sequence"); break;
                case 2: if (cp < 0x800 || (cp >= 0xD800 && cp <= 0xDFFF))
                            throw new IllegalStateException("corrupt utf8 overlong or surrogate sequence");
                        break;
                case 3: if (cp < 0x10000 || cp > 0x10FFFF)
                            throw new IllegalStateException("corrupt utf8 overlong or out-of-range sequence");
                        break;
                default: break; // need == 0: ASCII
            }
            if (cp < 0x10000) { // BMP: one UTF-16 unit
                if (ci == keyLen) return 1; // key is a strict prefix of stored
                int c = (char) cp - key.charAt(ci++);
                if (c != 0) return c;
            } else { // supplementary: expand to the surrogate pair String would hold
                int v = cp - 0x10000;
                char hiSur = (char) (0xD800 | (v >>> 10));
                char loSur = (char) (0xDC00 | (v & 0x3FF));
                if (ci == keyLen) return 1;
                int c = hiSur - key.charAt(ci++);
                if (c != 0) return c;
                if (ci == keyLen) return 1;
                c = loSur - key.charAt(ci++);
                if (c != 0) return c;
            }
        }
        return ci == keyLen ? 0 : -1; // stored exhausted: equal, or stored is a strict prefix
    }
}
