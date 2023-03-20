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

import static com.jerolba.carpet.impl.read.PrimitiveGenericConverterFactory.buildPrimitiveGenericConverter;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.util.List;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedMap;

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
            converterKey = buildPrimitiveGenericConverter(mapKeyType, mapKeyActualType, this::consumeKey);
        } else {
            converterKey = new CarpetGroupConverter(mapKeyType.asGroupType(), mapKeyActualType, this::consumeKey);
        }

        // Value
        Type mapValueType = fields.get(1);
        if (mapValueType.isPrimitive()) {
            converterValue = buildPrimitiveGenericConverter(mapValueType, parameterized.getValueActualType(),
                    this::consumeValue);
            return;
        }
        LogicalTypeAnnotation logicalType = mapValueType.getLogicalTypeAnnotation();
        if (logicalType == listType() && parameterized.valueIsCollection()) {
            var parameterizedValue = parameterized.getValueTypeAsCollection();
            converterValue = new CarpetListConverter(mapValueType.asGroupType(), parameterizedValue,
                    this::consumeValue);
            return;
        }
        if (logicalType == mapType() && parameterized.valueIsMap()) {
            var parameterizedValue = parameterized.getValueTypeAsMap();
            converterValue = new CarpetMapConverter(mapValueType.asGroupType(), parameterizedValue,
                    this::consumeValue);
            return;
        }
        Class<?> mapValueActualType = parameterized.getValueActualType();
        converterValue = new CarpetGroupConverter(mapValueType.asGroupType(), mapValueActualType, this::consumeValue);
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