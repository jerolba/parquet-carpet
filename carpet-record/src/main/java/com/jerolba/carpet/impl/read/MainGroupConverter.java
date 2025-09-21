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

import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildPrimitiveConverter;
import static com.jerolba.carpet.impl.read.ReadReflection.collectionFactory;
import static com.jerolba.carpet.impl.read.ReadReflection.mapFactory;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.lang.reflect.RecordComponent;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.VariantLogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

class MainGroupConverter {

    private final ColumnToFieldMapper columnToFieldMapper;

    public MainGroupConverter(ColumnToFieldMapper columnToFieldMapper) {
        this.columnToFieldMapper = columnToFieldMapper;
    }

    GroupConverter newCarpetGroupConverter(GroupType schema, Class<?> groupClass,
            Consumer<Object> groupConsumer) {
        return new CarpetGroupConverter(schema, groupClass, groupConsumer);
    }

    class CarpetGroupConverter extends GroupConverter {

        private final Converter[] converters;
        private final ConstructorParams constructor;
        private final Consumer<Object> groupConsumer;

        CarpetGroupConverter(GroupType schema, Class<?> groupClass, Consumer<Object> groupConsumer) {
            this.groupConsumer = groupConsumer;
            this.constructor = new ConstructorParams(groupClass);

            GroupFieldsMapper mapper = new GroupFieldsMapper(schema, groupClass, columnToFieldMapper);
            converters = new Converter[schema.getFields().size()];
            int cont = 0;
            for (var schemaField : schema.getFields()) {
                String name = schemaField.getName();
                var recordComponent = mapper.getRecordComponent(name);
                int idx = mapper.getIndex(name);
                Consumer<Object> consumer = value -> constructor.set(idx, value);
                converters[cont++] = converterFor(schemaField, constructor, mapper.getIndex(name), consumer,
                        recordComponent);
            }
        }

        Converter converterFor(Type schemaField, ConstructorParams constructor, int index, Consumer<Object> consumer,
                RecordComponent recordComponent) {

            if (schemaField.isRepetition(Repetition.REPEATED)) {
                return createSingleLevelConverter(schemaField, constructor, index, recordComponent);
            }
            if (schemaField.isPrimitive()) {
                return buildPrimitiveConverter(schemaField, recordComponent.getType(), consumer);
            }
            GroupType asGroupType = schemaField.asGroupType();
            LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
            if (listType().equals(logicalType)) {
                var parameterized = getParameterizedCollection(recordComponent);
                return new CarpetListConverter(asGroupType, parameterized, consumer);
            }
            if (mapType().equals(logicalType)) {
                var parameterized = getParameterizedMap(recordComponent);
                return new CarpetMapConverter(asGroupType, parameterized, consumer);
            }
            if (Map.class.isAssignableFrom(recordComponent.getType())) {
                return new CarpetGroupAsMapConverter(recordComponent.getType(), asGroupType, consumer);
            }
            if (logicalType instanceof VariantLogicalTypeAnnotation) {
                return new VariantConverter(asGroupType, consumer);
            }
            return new CarpetGroupConverter(asGroupType, recordComponent.getType(), consumer);
        }

        Object getCurrentRecord() {
            return constructor.create();
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {
            constructor.resetParams();
        }

        @Override
        public void end() {
            Object currentRecord = getCurrentRecord();
            groupConsumer.accept(currentRecord);
        }

    }

    class CarpetListConverter extends GroupConverter {

        private final Consumer<Object> groupConsumer;
        private final CollectionHolder collectionHolder;
        private final Converter converter;

        CarpetListConverter(GroupType schema, ParameterizedCollection parameterized, Consumer<Object> groupConsumer) {
            this.groupConsumer = groupConsumer;
            this.collectionHolder = new CollectionHolder(collectionFactory(parameterized.getCollectionType()));

            Type listChild = schema.getFields().get(0);
            boolean threeLevel = SchemaValidation.isThreeLevel(listChild);
            if (threeLevel) {
                converter = new CarpetListIntermediateConverter(listChild, parameterized, collectionHolder);
            } else {
                converter = createCollectionConverter(listChild, parameterized, collectionHolder::add);
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

    class CarpetListIntermediateConverter extends GroupConverter {

        private final CollectionHolder collectionHolder;
        private final Converter converter;
        private Object elementValue;

        CarpetListIntermediateConverter(Type rootListType, ParameterizedCollection parameterized,
                CollectionHolder collectionHolder) {
            this.collectionHolder = collectionHolder;
            List<Type> fields = rootListType.asGroupType().getFields();
            converter = createCollectionConverter(fields.get(0), parameterized, value -> elementValue = value);
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

    Converter createCollectionConverter(Type listElement, ParameterizedCollection parameterized,
            Consumer<Object> consumer) {
        if (listElement.isPrimitive()) {
            return buildPrimitiveConverter(listElement, parameterized.getActualType(), consumer);
        }
        LogicalTypeAnnotation logicalType = listElement.getLogicalTypeAnnotation();
        if (logicalType != null) {
            if (listType().equals(logicalType) && parameterized.isCollection()) {
                var parameterizedList = parameterized.getParametizedAsCollection();
                return new CarpetListConverter(listElement.asGroupType(), parameterizedList, consumer);
            }
            if (mapType().equals(logicalType) && parameterized.isMap()) {
                var parameterizedMap = parameterized.getParametizedAsMap();
                return new CarpetMapConverter(listElement.asGroupType(), parameterizedMap, consumer);
            }
            if (logicalType instanceof VariantLogicalTypeAnnotation) {
                return new VariantConverter(listElement.asGroupType(), consumer);
            }
        }
        GroupType groupType = listElement.asGroupType();
        Class<?> listType = parameterized.getActualType();
        return new CarpetGroupConverter(groupType, listType, consumer);
    }

    class CarpetMapConverter extends GroupConverter {

        private final Consumer<Object> groupConsumer;
        private final Converter converter;
        private final MapHolder mapHolder;

        CarpetMapConverter(GroupType schema, ParameterizedMap parameterized, Consumer<Object> groupConsumer) {
            this.groupConsumer = groupConsumer;
            this.mapHolder = new MapHolder(mapFactory(parameterized.getMapType()));
            List<Type> fields = schema.getFields();
            if (fields.size() > 1) {
                throw new RecordTypeConversionException(schema.getName() + " MAP can not have more than one field");
            }
            GroupType mapChild = fields.get(0).asGroupType();
            this.converter = new CarpetMapIntermediateConverter(parameterized, mapChild, mapHolder);
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
            groupConsumer.accept(mapHolder.getMap());
        }

    }

    class CarpetMapIntermediateConverter extends GroupConverter {

        private final Converter converterValue;
        private final Converter converterKey;
        private final MapHolder mapHolder;
        private Object elementValue;
        private Object elementKey;

        CarpetMapIntermediateConverter(ParameterizedMap parameterized, GroupType schema, MapHolder mapHolder) {
            this.mapHolder = mapHolder;

            List<Type> fields = schema.getFields();
            if (fields.size() != 2) {
                throw new RecordTypeConversionException(schema.getName() + " MAP child element must have two fields");
            }

            // Key
            Type mapKeyType = fields.get(0);
            Class<?> mapKeyActualType = parameterized.getKeyActualType();
            if (mapKeyType.isPrimitive()) {
                converterKey = buildPrimitiveConverter(mapKeyType, mapKeyActualType, this::consumeKey);
            } else {
                converterKey = new CarpetGroupConverter(mapKeyType.asGroupType(), mapKeyActualType, this::consumeKey);
            }

            // Value
            Type mapValueType = fields.get(1);
            if (mapValueType.isPrimitive()) {
                converterValue = buildPrimitiveConverter(mapValueType, parameterized.getValueActualType(),
                        this::consumeValue);
                return;
            }
            LogicalTypeAnnotation logicalType = mapValueType.getLogicalTypeAnnotation();
            if (logicalType == listType() && parameterized.valueIsCollection()) {
                var parameterizedList = parameterized.getValueTypeAsCollection();
                converterValue = new CarpetListConverter(mapValueType.asGroupType(), parameterizedList,
                        this::consumeValue);
                return;
            }
            if (logicalType == mapType() && parameterized.valueIsMap()) {
                var parameterizedMap = parameterized.getValueTypeAsMap();
                converterValue = new CarpetMapConverter(mapValueType.asGroupType(), parameterizedMap,
                        this::consumeValue);
                return;
            }
            if (logicalType instanceof VariantLogicalTypeAnnotation) {
                converterValue = new VariantConverter(mapValueType.asGroupType(),
                        this::consumeValue);
                return;
            }
            Class<?> mapValueActualType = parameterized.getValueActualType();
            converterValue = new CarpetGroupConverter(mapValueType.asGroupType(), mapValueActualType,
                    this::consumeValue);
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

    /**
     *
     * Supports reading 1-level collection structure. The field has repeated
     * repetition, but is not declared with a list LogicalType.
     *
     * Examples:
     *
     * Repeated list integer: List<Integer>
     *
     * <pre>
     * repeated int32 sizes
     * </pre>
     *
     * Repeated list of status record: List<Status>
     *
     * <pre>
     * repeated group status {
     *   optional binary id (STRING);
     *   required boolean active;
     * }
     * </pre>
     *
     */
    Converter createSingleLevelConverter(Type parquetField, ConstructorParams constructor,
            int index, RecordComponent recordComponent) {
        var parameterized = getParameterizedCollection(recordComponent);
        var collectionFactory = collectionFactory(parameterized.getCollectionType());

        Consumer<Object> consumer = v -> {
            Object currentCollection = constructor.get(index);
            if (currentCollection == null) {
                currentCollection = collectionFactory.get();
                constructor.set(index, currentCollection);
            }
            ((Collection) currentCollection).add(v);
        };

        if (parquetField.isPrimitive()) {
            return buildPrimitiveConverter(parquetField, parameterized.getActualType(), consumer);
        }
        var asGroupType = parquetField.asGroupType();
        if (parameterized.isMap()) {
            return new CarpetMapConverter(asGroupType, parameterized.getParametizedAsMap(), consumer);
        }
        if (parameterized.getActualJavaType().isVariant()) {
            return new VariantConverter(parquetField.asGroupType(), consumer);
        }
        var actualCollectionType = parameterized.getActualType();
        if (actualCollectionType.isRecord()) {
            return new CarpetGroupConverter(asGroupType, actualCollectionType, consumer);
        }
        throw new RecordTypeConversionException("Unexpected single level collection schema");
    }

}
