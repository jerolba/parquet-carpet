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
import static com.jerolba.carpet.impl.read.PrimitiveGenericConverterFactory.buildPrimitiveGenericConverters;
import static com.jerolba.carpet.impl.read.ReadReflection.collectionFactory;

import java.lang.reflect.RecordComponent;
import java.util.Collection;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

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
class SingleLevelConverterFactory {

    public static Converter createSingleLevelConverter(Type parquetField, ConstructorParams constructor,
            int index, RecordComponent recordComponent) {
        var parameterized = getParameterizedCollection(recordComponent);
        var collectionFactory = collectionFactory(parameterized.getCollectionType());

        Consumer<Object> consumer = v -> {
            if (constructor.c[index] == null) {
                constructor.c[index] = collectionFactory.get();
            }
            ((Collection) constructor.c[index]).add(v);
        };

        if (parquetField.isPrimitive()) {
            return buildPrimitiveGenericConverters(parquetField, parameterized.getActualType(), consumer);
        }
        var asGroupType = parquetField.asGroupType();
        if (parameterized.isMap()) {
            return new CarpetMapConverter(asGroupType, parameterized.getParametizedAsMap(), consumer);
        }
        var actualCollectionType = parameterized.getActualType();
        if (actualCollectionType.isRecord()) {
            return new CarpetGroupConverter(asGroupType, actualCollectionType, consumer);
        }
        throw new RecordTypeConversionException("Unexpected single level collection schema");
    }

}
