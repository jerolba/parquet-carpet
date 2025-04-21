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
import com.jerolba.carpet.impl.read.converter.BooleanConverter;
import com.jerolba.carpet.impl.read.converter.BinaryConverter;
import com.jerolba.carpet.impl.read.converter.ToByteConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatConverter;
import com.jerolba.carpet.impl.read.converter.ToIntegerConverter;
import com.jerolba.carpet.impl.read.converter.ToLongConverter;
import com.jerolba.carpet.impl.read.converter.ToShortConverter;

class PrimitiveConverterFactory {

    public static Converter buildPrimitiveConverter(Type parquetField, Class<?> genericJavaType,
            Consumer<Object> consumer) {

        JavaType javaType = new JavaType(genericJavaType);
        Converter fromLogicalType = buildFromLogicalTypeConverter(javaType, parquetField, consumer);
        if (fromLogicalType != null) {
            return fromLogicalType;
        }
        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        var converter = switch (type) {
        case INT32, INT64 -> buildFromIntConverter(consumer, javaType);
        case FLOAT, DOUBLE -> buildFromDecimalConverter(consumer, javaType);
        case BOOLEAN -> buildFromBooleanConverter(consumer, javaType);
        case BINARY, FIXED_LEN_BYTE_ARRAY -> buildFromBinaryConverter(consumer, javaType);
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
        if (converter == null) {
            throw new RecordTypeConversionException(
                    genericJavaType.getTypeName() + " not compatible with " + parquetField.getName());
        }
        return converter;
    }

    private static Converter buildFromIntConverter(Consumer<Object> consumer, JavaType type) {
        if (type.isInteger()) {
            return new ToIntegerConverter(consumer);
        }
        if (type.isLong()) {
            return new ToLongConverter(consumer);
        }
        if (type.isShort()) {
            return new ToShortConverter(consumer);
        }
        if (type.isByte()) {
            return new ToByteConverter(consumer);
        }
        if (type.isDouble()) {
            return new ToDoubleConverter(consumer);
        }
        if (type.isFloat()) {
            return new ToFloatConverter(consumer);
        }
        return null;
    }

    private static Converter buildFromDecimalConverter(Consumer<Object> consumer, JavaType type) {
        if (type.isFloat()) {
            return new ToFloatConverter(consumer);
        }
        if (type.isDouble()) {
            return new ToDoubleConverter(consumer);
        }
        return null;
    }

    private static Converter buildFromBooleanConverter(Consumer<Object> consumer, JavaType type) {
        if (type.isBoolean()) {
            return new BooleanConverter(consumer);
        }
        return null;
    }

    private static Converter buildFromBinaryConverter(Consumer<Object> consumer, JavaType type) {
        if (type.isBinary()) {
            return new BinaryConverter(consumer);
        }
        return null;
    }

}
