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

import static com.jerolba.carpet.impl.read.LogicalTypeConverters.buildFromLogicalTypeConverter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.read.converter.BooleanGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToByteGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToIntegerGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToLongGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToShortGenericConverter;

class PrimitiveGenericConverterFactory {

    public static Converter buildPrimitiveGenericConverter(Type parquetField, Class<?> genericJavaType,
            Consumer<Object> consumer) {
        JavaType javaType = new JavaType(genericJavaType);

        Converter fromLogicalType = buildFromLogicalTypeConverter(javaType, parquetField, consumer);
        if (fromLogicalType != null) {
            return fromLogicalType;
        }
        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        var converter = switch (type) {
        case INT32, INT64 -> genericBuildFromIntConverter(consumer, javaType);
        case FLOAT, DOUBLE -> genericBuildFromDecimalConverter(consumer, javaType);
        case BOOLEAN -> genericBuildFromBooleanConverter(consumer, javaType);
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
        if (converter == null) {
            throw new RecordTypeConversionException(
                    genericJavaType.getTypeName() + " not compatible with " + parquetField.getName() + " collection");
        }
        return converter;
    }

    private static Converter genericBuildFromIntConverter(Consumer<Object> consumer, JavaType type) {
        if (type.isInteger()) {
            return new ToIntegerGenericConverter(consumer);
        }
        if (type.isLong()) {
            return new ToLongGenericConverter(consumer);
        }
        if (type.isShort()) {
            return new ToShortGenericConverter(consumer);
        }
        if (type.isByte()) {
            return new ToByteGenericConverter(consumer);
        }
        if (type.isDouble()) {
            return new ToDoubleGenericConverter(consumer);
        }
        if (type.isFloat()) {
            return new ToFloatGenericConverter(consumer);
        }
        return null;
    }

    private static Converter genericBuildFromDecimalConverter(Consumer<Object> consumer, JavaType type) {
        if (type.isFloat()) {
            return new ToFloatGenericConverter(consumer);
        }
        if (type.isDouble()) {
            return new ToDoubleGenericConverter(consumer);
        }
        return null;
    }

    private static Converter genericBuildFromBooleanConverter(Consumer<Object> consumer, JavaType type) {
        if (type.isBoolean()) {
            return new BooleanGenericConverter(consumer);
        }
        return null;
    }

}
