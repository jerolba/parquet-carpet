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

import static com.jerolba.carpet.impl.read.PrimitiveGenericConverterFactory.buildPrimitiveGenericConverters;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.util.List;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;

class CarpetListIntermediateConverter extends GroupConverter {

    private final Converter converter;
    private final ListHolder listHolder;
    private Object elementValue;

    CarpetListIntermediateConverter(Type rootListType, ParameterizedCollection parameterized, ListHolder listHolder) {
        this.listHolder = listHolder;

        var requestedSchema = rootListType.asGroupType();
        List<Type> fields = requestedSchema.getFields();
        if (fields.size() > 1) {
            throw new RecordTypeConversionException(
                    requestedSchema.getName() + " LIST child element can not have more than one field");
        }
        Consumer<Object> consumer = this::accept;
        Type listElement = fields.get(0);
        converter = createCollectionConverter(listElement, parameterized, consumer);
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
        listHolder.add(elementValue);
    }

    public void accept(Object value) {
        elementValue = value;
    }

    public static Converter createCollectionConverter(Type listElement, ParameterizedCollection parameterized,
            Consumer<Object> consumer) {
        if (listElement.isPrimitive()) {
            return buildPrimitiveGenericConverters(listElement, parameterized.getActualType(), consumer);
        }
        LogicalTypeAnnotation logicalType = listElement.getLogicalTypeAnnotation();
        if (logicalType == listType() && parameterized.isCollection()) {
            var parameterizedList = parameterized.getParametizedAsCollection();
            return new CarpetListConverter(listElement.asGroupType(), parameterizedList, consumer);
        }
        if (logicalType == mapType() && parameterized.isMap()) {
            var parameterizedMap = parameterized.getParametizedAsMap();
            return new CarpetMapConverter(listElement.asGroupType(), parameterizedMap, consumer);

        }
        GroupType groupType = listElement.asGroupType();
        Class<?> listType = parameterized.getActualType();
        return new CarpetGroupConverter(groupType, listType, consumer);
    }

}