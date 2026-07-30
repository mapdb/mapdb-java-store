package org.mapdb.db;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.mapdb.ser.ArraySerializer;
import org.mapdb.ser.ByteArrayFormat;
import org.mapdb.ser.ByteArrayPrefixFormat;
import org.mapdb.ser.CharFormat;
import org.mapdb.ser.CompressionSerializer;
import org.mapdb.ser.ColumnarValueFormat;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.IntDeltaFormat;
import org.mapdb.ser.IntFormat;
import org.mapdb.ser.LongDeltaFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.ShortFormat;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.ser.StringPrefixFormat;
import org.mapdb.ser.TupleComponent;
import org.mapdb.ser.TupleFormat;
import org.mapdb.ser.UUIDFormat;

/**
 * Stable string ids for the built-in {@link GroupFormat} and {@link Serializer}
 * singletons, so the name catalog can persist which codec a collection uses.
 *
 * <p>This registry also emits recursive descriptors for supported parameterized
 * codecs ({@link ObjectArrayFormat}, {@link TupleFormat}, {@link ArraySerializer},
 * {@link CompressionSerializer}). Other custom codecs retain the explicit
 * re-supply-on-open contract. Ids are storage format — never renumber or reuse them.
 */
public final class SerializerRegistry {

    private SerializerRegistry() {}

    private static final Map<String, GroupFormat<?>> FORMAT_BY_ID = new HashMap<>();
    private static final Map<GroupFormat<?>, String> ID_BY_FORMAT = new IdentityHashMap<>();
    private static final Map<String, Serializer<?>> SER_BY_ID = new HashMap<>();
    private static final Map<Serializer<?>, String> ID_BY_SER = new IdentityHashMap<>();

    private static void format(String id, GroupFormat<?> f) {
        FORMAT_BY_ID.put(id, f);
        ID_BY_FORMAT.put(f, id);
    }

    private static void ser(String id, Serializer<?> s) {
        SER_BY_ID.put(id, s);
        ID_BY_SER.put(s, id);
    }

    static {
        // GroupFormat singletons (BTreeMap key/value codecs)
        format("LONG", LongFormat.INSTANCE);
        format("INT", IntFormat.INSTANCE);
        format("SHORT", ShortFormat.INSTANCE);
        format("CHAR", CharFormat.INSTANCE);
        format("UUID", UUIDFormat.INSTANCE);
        format("STRING", StringGroupFormat.INSTANCE);
        format("STRING_PREFIX", StringPrefixFormat.INSTANCE);
        format("BYTE_ARRAY", ByteArrayFormat.INSTANCE);
        format("BYTE_ARRAY_PREFIX", ByteArrayPrefixFormat.INSTANCE);
        format("INT_DELTA", IntDeltaFormat.INSTANCE);
        format("LONG_DELTA", LongDeltaFormat.INSTANCE);

        // Element Serializer singletons (HTreeMap / IndexTreeList / Atomic.Var codecs)
        ser("LONG", Serializers.LONG);
        ser("INTEGER", Serializers.INTEGER);
        ser("SHORT", Serializers.SHORT);
        ser("CHAR", Serializers.CHAR);
        ser("UUID", Serializers.UUID);
        ser("STRING", Serializers.STRING);
        ser("BYTE_ARRAY", Serializers.BYTE_ARRAY);
        ser("BYTE_ARRAY_UNSIGNED", Serializers.BYTE_ARRAY_UNSIGNED);
        ser("BOOLEAN", Serializers.BOOLEAN);
        ser("BYTE", Serializers.BYTE);
        ser("FLOAT", Serializers.FLOAT);
        ser("DOUBLE", Serializers.DOUBLE);
        ser("INTEGER_PACKED", Serializers.INTEGER_PACKED);
        ser("LONG_PACKED", Serializers.LONG_PACKED);
        ser("BYTE_ARRAY_NOSIZE", Serializers.BYTE_ARRAY_NOSIZE);
        ser("STRING_NOSIZE", Serializers.STRING_NOSIZE);
        ser("STRING_ASCII", Serializers.STRING_ASCII);
        ser("STRING_INTERN", Serializers.STRING_INTERN);
        ser("RECID", Serializers.RECID);
        ser("RECID_ARRAY", Serializers.RECID_ARRAY);
        ser("BOOLEAN_ARRAY", Serializers.BOOLEAN_ARRAY);
        ser("CHAR_ARRAY", Serializers.CHAR_ARRAY);
        ser("SHORT_ARRAY", Serializers.SHORT_ARRAY);
        ser("INT_ARRAY", Serializers.INT_ARRAY);
        ser("LONG_ARRAY", Serializers.LONG_ARRAY);
        ser("FLOAT_ARRAY", Serializers.FLOAT_ARRAY);
        ser("DOUBLE_ARRAY", Serializers.DOUBLE_ARRAY);
        ser("BIG_INTEGER", Serializers.BIG_INTEGER);
        ser("BIG_DECIMAL", Serializers.BIG_DECIMAL);
        ser("DATE", Serializers.DATE);
        ser("CLASS", Serializers.CLASS);
        ser("JAVA", Serializers.JAVA);
    }

    /** Catalog id for a group format, or {@code null} if it is not a registered singleton. */
    public static String groupFormatId(GroupFormat<?> f) {
        String builtin = ID_BY_FORMAT.get(f);
        if (builtin != null) return builtin;
        if (f instanceof ObjectArrayFormat) {
            String element = serializerId(f.element());
            return element == null ? null : "OBJECT_ARRAY:" + encode(element);
        }
        if (f instanceof TupleFormat) {
            TupleComponent[] schema = ((TupleFormat) f).schema();
            StringBuilder descriptor = new StringBuilder("TUPLE:");
            for (int i = 0; i < schema.length; i++) {
                if (i > 0) descriptor.append(',');
                descriptor.append(schema[i].name());
            }
            return descriptor.toString();
        }
        if (f instanceof ColumnarValueFormat) {
            ColumnarValueFormat columns = (ColumnarValueFormat) f;
            StringBuilder descriptor = new StringBuilder("COLUMNAR:");
            for (int i = 0; i < columns.columnCount(); i++) {
                if (i > 0) descriptor.append(',');
                descriptor.append(columns.columnType(i).name());
            }
            return descriptor.toString();
        }
        return null;
    }

    /** Registered group format for an id, or {@code null} if unknown/empty. */
    public static GroupFormat<?> groupFormatById(String id) {
        if (id == null || id.isEmpty()) return null;
        GroupFormat<?> builtin = FORMAT_BY_ID.get(id);
        if (builtin != null) return builtin;
        if (id.startsWith("OBJECT_ARRAY:")) {
            try {
                Serializer<?> element = serializerById(decode(id.substring("OBJECT_ARRAY:".length())));
                return element == null ? null : new ObjectArrayFormat<>(element);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        if (id.startsWith("TUPLE:")) {
            String body = id.substring("TUPLE:".length());
            if (body.isEmpty()) return null;
            String[] names = body.split(",", -1);
            TupleComponent[] schema = new TupleComponent[names.length];
            try {
                for (int i = 0; i < names.length; i++) schema[i] = TupleComponent.valueOf(names[i]);
            } catch (IllegalArgumentException e) {
                return null;
            }
            return TupleFormat.of(schema);
        }
        if (id.startsWith("COLUMNAR:")) {
            String body = id.substring("COLUMNAR:".length());
            if (body.isEmpty()) return null;
            String[] names = body.split(",", -1);
            ColumnarValueFormat.ColumnType[] schema =
                    new ColumnarValueFormat.ColumnType[names.length];
            try {
                for (int i = 0; i < names.length; i++)
                    schema[i] = ColumnarValueFormat.ColumnType.valueOf(names[i]);
            } catch (IllegalArgumentException e) {
                return null;
            }
            return ColumnarValueFormat.of(schema);
        }
        return null;
    }

    /** Catalog id for an element serializer, or {@code null} if it is not a registered singleton. */
    public static String serializerId(Serializer<?> s) {
        String builtin = ID_BY_SER.get(s);
        if (builtin != null) return builtin;
        if (s instanceof CompressionSerializer) {
            CompressionSerializer<?> compressed = (CompressionSerializer<?>) s;
            String nested = serializerId(compressed.delegate());
            return nested == null ? null
                    : "DEFLATE:" + compressed.level() + ":" + encode(nested);
        }
        if (s instanceof ArraySerializer) {
            ArraySerializer<?> array = (ArraySerializer<?>) s;
            String nested = serializerId(array.elementSerializer());
            return nested == null ? null : "ARRAY:" + encode(array.componentType().getName())
                    + ":" + encode(nested);
        }
        return null;
    }

    /** Registered element serializer for an id, or {@code null} if unknown/empty. */
    public static Serializer<?> serializerById(String id) {
        if (id == null || id.isEmpty()) return null;
        Serializer<?> builtin = SER_BY_ID.get(id);
        if (builtin != null) return builtin;
        if (id.startsWith("DEFLATE:")) {
            int separator = id.indexOf(':', "DEFLATE:".length());
            if (separator < 0) return null;
            try {
                int level = Integer.parseInt(id.substring("DEFLATE:".length(), separator));
                Serializer<?> nested = serializerById(decode(id.substring(separator + 1)));
                return nested == null ? null : new CompressionSerializer<>(nested, level);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        if (id.startsWith("ARRAY:")) {
            int separator = id.indexOf(':', "ARRAY:".length());
            if (separator < 0) return null;
            try {
                String className = decode(id.substring("ARRAY:".length(), separator));
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                if (loader == null) loader = SerializerRegistry.class.getClassLoader();
                Class<?> component = Class.forName(className, false, loader);
                Serializer<?> nested = serializerById(decode(id.substring(separator + 1)));
                return nested == null ? null : arraySerializer(component, nested);
            } catch (ClassNotFoundException | IllegalArgumentException e) {
                return null;
            }
        }
        return null;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Serializer<?> arraySerializer(Class<?> component, Serializer<?> nested) {
        return new ArraySerializer(component, nested);
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
