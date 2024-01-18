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
import static com.jerolba.carpet.impl.read.SingleLevelConverterFactory.createSingleLevelConverter;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.lang.reflect.RecordComponent;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class CarpetGroupConverter extends GroupConverter {

    private final Converter[] converters;
    private final ConstructorParams constructor;
    private final Consumer<Object> groupConsumer;

    public CarpetGroupConverter(GroupType schema, Class<?> groupClass, Consumer<Object> groupConsumer) {
        this.groupConsumer = groupConsumer;
        this.constructor = new ConstructorParams(groupClass);

        GroupFieldsMapper mapper = new GroupFieldsMapper(groupClass);
        converters = new Converter[schema.getFields().size()];
        int cont = 0;
        for (var schemaField : schema.getFields()) {
            String name = schemaField.getName();
            var recordComponent = mapper.getRecordComponent(name);
            if (recordComponent == null) {
                throw new RecordTypeConversionException(
                        groupClass.getName() + " doesn't have an attribute called " + name);
            }
            converters[cont++] = converterFor(schemaField, constructor, mapper.getIndex(name), recordComponent);
        }
    }

    public static Converter converterFor(Type schemaField, ConstructorParams constructor, int index,
            RecordComponent recordComponent) {

        if (schemaField.isRepetition(Repetition.REPEATED)) {
            return createSingleLevelConverter(schemaField, constructor, index, recordComponent);
        }
        if (schemaField.isPrimitive()) {
            var factory = new PrimitiveConverterFactory(constructor, index, recordComponent);
            return factory.buildConverters(schemaField);
        }
        GroupType asGroupType = schemaField.asGroupType();
        LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
        if (listType().equals(logicalType)) {
            var parameterized = getParameterizedCollection(recordComponent);
            return new CarpetListConverter(asGroupType, parameterized, value -> constructor.c[index] = value);
        }
        if (mapType().equals(logicalType)) {
            var parameterized = getParameterizedMap(recordComponent);
            return new CarpetMapConverter(asGroupType, parameterized, value -> constructor.c[index] = value);
        }
        if (Map.class.isAssignableFrom(recordComponent.getType())) {
            return new CarpetGroupAsMapConverter(recordComponent.getType(), asGroupType,
                    value -> constructor.c[index] = value);
        }
        return new CarpetGroupConverter(asGroupType, recordComponent.getType(), value -> constructor.c[index] = value);
    }

    public Object getCurrentRecord() {
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