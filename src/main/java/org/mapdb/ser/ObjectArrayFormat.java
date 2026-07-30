package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

/**
 * Generic fallback group format: Object[] group, elements encoded by the element
 * serializer. Declares {@code supportsBinary() == false} — callers must deserialize
 * before searching (explicitly: no silent fallback).
 */
public final class ObjectArrayFormat<A> implements GroupFormat<A> {

    private final Serializer<A> element;

    public ObjectArrayFormat(Serializer<A> element) { this.element = element; }

    @Override public Serializer<A> element() { return element; }

    @Override public Object empty() { return new Object[0]; }

    @Override public int size(Object group) { return ((Object[]) group).length; }

    @SuppressWarnings("unchecked")
    @Override public A get(Object group, int pos) { return (A) ((Object[]) group)[pos]; }

    @SuppressWarnings("unchecked")
    @Override public int search(Object group, A key) {
        Object[] g = (Object[]) group;
        int lo = 0, hi = g.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = element.compare((A) g[mid], key);
            if (c == 0) return mid;
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return -(lo + 1);
    }

    @Override public Object insert(Object group, int pos, A newValue) {
        Object[] g = (Object[]) group;
        Object[] r = new Object[g.length + 1];
        System.arraycopy(g, 0, r, 0, pos);
        r[pos] = newValue;
        System.arraycopy(g, pos, r, pos + 1, g.length - pos);
        return r;
    }

    @Override public Object set(Object group, int pos, A newValue) {
        Object[] r = ((Object[]) group).clone();
        r[pos] = newValue;
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        Object[] g = (Object[]) group;
        Object[] r = new Object[g.length - 1];
        System.arraycopy(g, 0, r, 0, pos);
        System.arraycopy(g, pos + 1, r, pos, g.length - pos - 1);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return Arrays.copyOfRange((Object[]) group, from, to);
    }

    @Override public Object fromArray(Object[] values) { return values.clone(); }

    @SuppressWarnings("unchecked")
    @Override public void serialize(DataOutput2 out, Object group) {
        for (Object o : (Object[]) group) element.serialize(out, (A) o);
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        Object[] r = new Object[size];
        for (int i = 0; i < size; i++) r[i] = element.deserialize(in, -1);
        return r;
    }
}
