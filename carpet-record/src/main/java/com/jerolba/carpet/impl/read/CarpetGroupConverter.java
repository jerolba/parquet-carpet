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
import static com.jerolba.carpet.impl.read.PrimitiveConverterFactory.buildPrimitiveConverters;
import static com.jerolba.carpet.impl.read.SingleLevelConverterFactory.createSingleLevelConverter;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class CarpetGroupConverter extends GroupConverter {

    private final Converter[] converters;
    private final ConstructorParams constructor;
    private final Consumer<Object> groupConsumer;

    public CarpetGroupConverter(GroupType requestedSchema, Class<?> groupClass, Consumer<Object> groupConsumer) {
        this.groupConsumer = groupConsumer;
        this.constructor = new ConstructorParams(groupClass);

        GroupFieldsMapper mapper = new GroupFieldsMapper(groupClass);

        converters = new Converter[requestedSchema.getFields().size()];
        int cont = 0;
        for (var schemaField : requestedSchema.getFields()) {
            String name = schemaField.getName();
            int index = mapper.getIndex(name);
            var recordComponent = mapper.getRecordComponent(name);
            if (recordComponent == null) {
                throw new RecordTypeConversionException(
                        groupClass.getName() + " doesn't have an attribute called " + name);
            }
            if (schemaField.isRepetition(Repetition.REPEATED)) {
                converters[cont] = createSingleLevelConverter(schemaField, constructor, index, recordComponent);
            } else if (schemaField.isPrimitive()) {
                converters[cont] = buildPrimitiveConverters(schemaField, constructor, index, recordComponent);
            } else {
                GroupType asGroupType = schemaField.asGroupType();
                LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
                if (listType().equals(logicalType)) {
                    var parameterized = getParameterizedCollection(recordComponent);
                    converters[cont] = new CarpetListConverter(asGroupType, parameterized,
                            value -> constructor.c[index] = value);
                } else if (mapType().equals(logicalType)) {
                    var parameterized = getParameterizedMap(recordComponent);
                    converters[cont] = new CarpetMapConverter(asGroupType, parameterized,
                            value -> constructor.c[index] = value);
                } else {
                    Class<?> childClass = recordComponent.getType();
                    CarpetGroupConverter converter = new CarpetGroupConverter(asGroupType, childClass,
                            value -> constructor.c[index] = value);
                    converters[cont] = converter;
                }
            }
            cont++;
        }
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