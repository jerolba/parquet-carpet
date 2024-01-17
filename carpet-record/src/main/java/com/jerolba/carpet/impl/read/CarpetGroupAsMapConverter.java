/**
 * Copyright 2023 Jerónimo López Bezanilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jerolba.carpet.impl.read;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.converter.BooleanGenericConverter;
import com.jerolba.carpet.impl.read.converter.StringGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToByteGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToIntegerGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToLongGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToShortGenericConverter;
import com.jerolba.carpet.impl.read.converter.UuidToUuidGenericConverter;

public class CarpetGroupAsMapConverter extends GroupConverter {

    private final Converter[] converters;
    private final Consumer<Object> groupConsumer;
    private final GroupMapHolder mapHolder;

    public CarpetGroupAsMapConverter(GroupType schema, Consumer<Object> groupConsumer) {
        this.groupConsumer = groupConsumer;
        Map<String, Integer> indexByName = getSchemaFields(schema);
        this.mapHolder = new GroupMapHolder(() -> new ParquetGroupMap<>(indexByName));

        converters = new Converter[schema.getFields().size()];
        int cont = 0;
        for (var schemaField : schema.getFields()) {
            converters[cont] = converterFor(cont, schemaField, mapHolder);
            cont++;
        }
    }

    private static Converter converterFor(int idx, Type schemaField, GroupMapHolder mapHolder) {
        var name = schemaField.getName();
        Consumer<Object> consumer = value -> mapHolder.add(idx, value);
        if (schemaField.isRepetition(Repetition.REPEATED)) {
            return createSingleLevelConverter(idx, schemaField, mapHolder, name);
        }
        if (schemaField.isPrimitive()) {
            return PrimitiveConverterFactory.buildConverters(schemaField, consumer);
        }
        GroupType asGroupType = schemaField.asGroupType();
        LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
        if (listType().equals(logicalType)) {
            return new CarpetListAsMapConverter(asGroupType, consumer);
        }
        if (mapType().equals(logicalType)) {
            return new CarpetMapAsMapConverter(asGroupType, consumer);
        }
        return new CarpetGroupAsMapConverter(asGroupType, consumer);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        mapHolder.create();
    }

    @Override
    public void end() {
        groupConsumer.accept(mapHolder.getMap());
    }

    private static class PrimitiveConverterFactory {

        static Converter buildConverters(Type parquetField, Consumer<Object> consumer) {
            PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
            return switch (type) {
            case INT32 -> buildFromIntegerConverter(parquetField, consumer);
            case INT64 -> new ToLongGenericConverter(consumer);
            case FLOAT -> new ToFloatGenericConverter(consumer);
            case DOUBLE -> new ToDoubleGenericConverter(consumer);
            case BOOLEAN -> new BooleanGenericConverter(consumer);
            case BINARY -> buildFromBinaryConverter(parquetField, consumer);
            case FIXED_LEN_BYTE_ARRAY -> buildFromByteArrayConverter(parquetField, consumer);
            case INT96 -> throw new RecordTypeConversionException(type + " deserialization not supported");
            default -> throw new RecordTypeConversionException(type + " deserialization not supported");
            };
        }

        private static Converter buildFromIntegerConverter(Type parquetField, Consumer<Object> consumer) {
            LogicalTypeAnnotation logicalType = parquetField.getLogicalTypeAnnotation();
            if (logicalType instanceof IntLogicalTypeAnnotation intType) {
                return switch (intType.getBitWidth()) {
                case 8 -> new ToByteGenericConverter(consumer);
                case 16 -> new ToShortGenericConverter(consumer);
                default -> new ToIntegerGenericConverter(consumer);
                };
            }
            return new ToIntegerGenericConverter(consumer);
        }

        private static Converter buildFromBinaryConverter(Type parquetField, Consumer<Object> consumer) {
            LogicalTypeAnnotation logicalType = parquetField.getLogicalTypeAnnotation();
            if (stringType().equals(logicalType)) {
                return new StringGenericConverter(consumer);
            }
            if (enumType().equals(logicalType)) {
                return new StringGenericConverter(consumer);
            }
            throw new RecordTypeConversionException(parquetField + " deserialization not supported");
        }

        private static Converter buildFromByteArrayConverter(Type parquetField, Consumer<Object> consumer) {
            if (!uuidType().equals(parquetField.getLogicalTypeAnnotation())) {
                throw new RecordTypeConversionException(parquetField + " deserialization not supported");
            }
            return new UuidToUuidGenericConverter(consumer);
        }

    }

    private static Converter createSingleLevelConverter(int idx, Type parquetField,
            GroupMapHolder mapHolder, String name) {
        Consumer<Object> consumer = v -> {
            ParquetGroupMap<String, Object> map = mapHolder.getMap();
            List<Object> list = (List<Object>) map.getValue(idx);
            if (list == null) {
                list = new ArrayList<>();
                map.add(idx, list);
            }
            list.add(v);
        };

        if (parquetField.isPrimitive()) {
            return PrimitiveConverterFactory.buildConverters(parquetField, consumer);
        }
        var asGroupType = parquetField.asGroupType();
        LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
        if (mapType().equals(logicalType)) {
            return new CarpetMapAsMapConverter(asGroupType, consumer);
        }
        return new CarpetGroupAsMapConverter(asGroupType, consumer);
    }

    private static class CarpetListAsMapConverter extends GroupConverter {

        private final Consumer<Object> groupConsumer;
        private final CollectionHolder collectionHolder;
        private final Converter converter;

        CarpetListAsMapConverter(GroupType schema, Consumer<Object> consumer) {
            this.groupConsumer = consumer;
            this.collectionHolder = new CollectionHolder(ArrayList::new);

            Type listChild = schema.getFields().get(0);
            boolean threeLevel = SchemaValidation.isThreeLevel(listChild);
            if (threeLevel) {
                converter = new CarpetListAsMapIntermediateConverter(listChild, collectionHolder);
            } else {
                converter = createCollectionConverter(listChild, collectionHolder::add);
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converter;
        }

        @Override
        public void start() {
            collectionHolder.create();
        }

        @Override
        public void end() {
            groupConsumer.accept(collectionHolder.getCollection());
        }

    }

    private static Converter createCollectionConverter(Type listElement, Consumer<Object> consumer) {
        if (listElement.isPrimitive()) {
            return PrimitiveConverterFactory.buildConverters(listElement, consumer);
        }
        GroupType groupType = listElement.asGroupType();
        LogicalTypeAnnotation logicalType = listElement.getLogicalTypeAnnotation();
        if (logicalType != null) {
            if (listType().equals(logicalType)) {
                return new CarpetListAsMapConverter(groupType, consumer);
            }
            if (mapType().equals(logicalType)) {
                return new CarpetMapAsMapConverter(groupType, consumer);
            }
        }
        return new CarpetGroupAsMapConverter(groupType, consumer);
    }

    private static class CarpetListAsMapIntermediateConverter extends GroupConverter {

        private final CollectionHolder collectionHolder;
        private final Converter converter;
        private Object elementValue;

        CarpetListAsMapIntermediateConverter(Type rootListType, CollectionHolder collectionHolder) {
            this.collectionHolder = collectionHolder;

            var schema = rootListType.asGroupType();
            List<Type> fields = schema.getFields();
            if (fields.size() > 1) {
                throw new RecordTypeConversionException(
                        schema.getName() + " LIST child element can not have more than one field");
            }
            converter = createCollectionConverter(fields.get(0), value -> elementValue = value);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converter;
        }

        @Override
        public void start() {
            elementValue = null;
        }

        @Override
        public void end() {
            collectionHolder.add(elementValue);
        }

    }

    private static class CarpetMapAsMapConverter extends GroupConverter {

        private final Consumer<Object> consumer;
        private final Converter converter;
        private final MapHolder mapHolder;

        CarpetMapAsMapConverter(GroupType schema, Consumer<Object> consumer) {
            this.consumer = consumer;
            this.mapHolder = new MapHolder(HashMap::new);
            List<Type> fields = schema.getFields();
            if (fields.size() > 1) {
                throw new RecordTypeConversionException(schema.getName() + " MAP can not have more than one field");
            }
            GroupType mapChild = fields.get(0).asGroupType();
            this.converter = new CarpetMapAsMapIntermediateConverter(mapChild, mapHolder);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converter;
        }

        @Override
        public void start() {
            mapHolder.create();
        }

        @Override
        public void end() {
            consumer.accept(mapHolder.getMap());
        }

    }

    private static class CarpetMapAsMapIntermediateConverter extends GroupConverter {

        private final Converter converterValue;
        private final Converter converterKey;
        private final MapHolder mapHolder;
        private Object elementValue;
        private Object elementKey;

        CarpetMapAsMapIntermediateConverter(GroupType schema, MapHolder mapHolder) {
            this.mapHolder = mapHolder;

            List<Type> fields = schema.getFields();
            if (fields.size() != 2) {
                throw new RecordTypeConversionException(schema.getName() + " MAP child element must have two fields");
            }

            // Key
            Type mapKeyType = fields.get(0);
            if (mapKeyType.isPrimitive()) {
                converterKey = PrimitiveConverterFactory.buildConverters(mapKeyType, this::consumeKey);
            } else {
                converterKey = new CarpetGroupAsMapConverter(mapKeyType.asGroupType(), this::consumeKey);
            }

            // Value
            Type mapValueType = fields.get(1);
            if (mapValueType.isPrimitive()) {
                converterValue = PrimitiveConverterFactory.buildConverters(mapValueType, this::consumeValue);
            } else {
                LogicalTypeAnnotation logicalType = mapValueType.getLogicalTypeAnnotation();
                if (listType().equals(logicalType)) {
                    converterValue = new CarpetListAsMapConverter(mapValueType.asGroupType(), this::consumeValue);
                } else if (mapType().equals(logicalType)) {
                    converterValue = new CarpetMapAsMapConverter(mapValueType.asGroupType(), this::consumeValue);
                } else {
                    converterValue = new CarpetGroupAsMapConverter(mapValueType.asGroupType(), this::consumeValue);
                }
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (fieldIndex == 0) {
                return converterKey;
            }
            return converterValue;
        }

        @Override
        public void start() {
            elementKey = null;
            elementValue = null;
        }

        @Override
        public void end() {
            mapHolder.put(elementKey, elementValue);
        }

        private void consumeKey(Object value) {
            elementKey = value;
        }

        private void consumeValue(Object value) {
            elementValue = value;
        }

    }

    private static Map<String, Integer> getSchemaFields(GroupType schema) {
        List<Type> fields = schema.getFields();
        Map<String, Integer> indexByName = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            indexByName.put(fields.get(i).getName(), i);
        }
        return indexByName;
    }

    private static class GroupMapHolder {

        private final Supplier<ParquetGroupMap<String, Object>> mapFactory;
        private ParquetGroupMap<String, Object> map;

        GroupMapHolder(Supplier<ParquetGroupMap<String, Object>> mapFactory) {
            this.mapFactory = mapFactory;
        }

        void create() {
            map = mapFactory.get();
        }

        void add(int idx, Object value) {
            map.add(idx, value);
        }

        ParquetGroupMap<String, Object> getMap() {
            return map;
        }

    }

    private static class ParquetGroupMap<K, V> implements Map<K, V> {

        private final Map<K, Integer> index;
        private final Object[] values;

        ParquetGroupMap(Map<K, Integer> index) {
            this.index = index;
            this.values = new Object[index.size()];

        }

        void add(int idx, Object value) {
            this.values[idx] = value;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean containsKey(Object key) {
            return index.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            Objects.requireNonNull(value);
            for (int i = 0; i < values.length; i++) {
                if (value.equals(values[i])) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public V get(Object key) {
            Integer idx = index.get(key);
            if (idx != null) {
                return getValue(idx);
            }
            return null;
        }

        public V getValue(int idx) {
            return (V) values[idx];
        }

        @Override
        public V put(K key, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public V remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<K> keySet() {
            return index.keySet();
        }

        @Override
        public Collection<V> values() {
            return Arrays.asList((V[]) values);
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return index.entrySet().stream()
                    .map(e -> new SimpleImmutableEntry<>(e.getKey(), (V) values[e.getValue()]))
                    .collect(toUnmodifiableSet());
        }

        // From AbstractMap
        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }

            if (!(o instanceof Map<?, ?> m)) {
                return false;
            }
            if (m.size() != size()) {
                return false;
            }

            try {
                for (Entry<K, Integer> e : index.entrySet()) {
                    K key = e.getKey();
                    Integer idx = e.getValue();
                    V value = getValue(idx);
                    if (value == null) {
                        if (!(m.get(key) == null && m.containsKey(key))) {
                            return false;
                        }
                    } else if (!value.equals(m.get(key))) {
                        return false;
                    }
                }
            } catch (ClassCastException | NullPointerException unused) {
                return false;
            }

            return true;
        }

        // From AbstractMap
        @Override
        public String toString() {
            if (isEmpty()) {
                return "{}";
            }

            Iterator<Entry<K, Integer>> i = index.entrySet().iterator();
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            for (;;) {
                Entry<K, Integer> e = i.next();
                K key = e.getKey();
                Integer idx = e.getValue();
                V value = getValue(idx);
                sb.append(key == this ? "(this Map)" : key);
                sb.append('=');
                sb.append(value == this ? "(this Map)" : value);
                if (!i.hasNext()) {
                    return sb.append('}').toString();
                }
                sb.append(',').append(' ');
            }
        }

        @Override
        public int hashCode() {
            int h = 0;
            for (Entry<K, V> entry : entrySet()) {
                h += entry.hashCode();
            }
            return h;
        }
    }
}