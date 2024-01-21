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
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.impl.ParameterizedCollection;

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

    public static Converter createCollectionConverter(Type listElement, ParameterizedCollection parameterized,
            Consumer<Object> consumer) {
        if (listElement.isPrimitive()) {
            return buildPrimitiveGenericConverter(listElement, parameterized.getActualType(), consumer);
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
        }
        GroupType groupType = listElement.asGroupType();
        Class<?> listType = parameterized.getActualType();
        return new CarpetGroupConverter(groupType, listType, consumer);
    }

}