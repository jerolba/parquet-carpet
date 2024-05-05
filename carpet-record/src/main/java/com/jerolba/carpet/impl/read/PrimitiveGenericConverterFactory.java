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

import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.read.converter.BooleanGenericConverter;
import com.jerolba.carpet.impl.read.converter.DecimalConverter;
import com.jerolba.carpet.impl.read.converter.EnumConverter;
import com.jerolba.carpet.impl.read.converter.InstantConverter;
import com.jerolba.carpet.impl.read.converter.LocalDateConverter;
import com.jerolba.carpet.impl.read.converter.LocalDateTimeConverter;
import com.jerolba.carpet.impl.read.converter.LocalTimeConverter;
import com.jerolba.carpet.impl.read.converter.StringConverter;
import com.jerolba.carpet.impl.read.converter.ToByteGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToIntegerGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToLongGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToShortGenericConverter;
import com.jerolba.carpet.impl.read.converter.UuidToStringConverter;
import com.jerolba.carpet.impl.read.converter.UuidToUuidConverter;

class PrimitiveGenericConverterFactory {

    public static Converter buildPrimitiveGenericConverter(Type parquetField, Class<?> genericJavaType,
            Consumer<Object> consumer) {
        JavaType genericType = new JavaType(genericJavaType);
        Converter fromLogicalType = buildFromLogicalTypeConverter(consumer, genericType, parquetField);
        if (fromLogicalType != null) {
            return fromLogicalType;
        }
        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        return switch (type) {
        case INT32, INT64 -> genericBuildFromIntConverter(consumer, genericType, parquetField);
        case FLOAT, DOUBLE -> genericBuildFromDecimalConverter(consumer, genericType, parquetField);
        case BOOLEAN -> genericBuildFromBooleanConverter(consumer, genericType, parquetField);
        case BINARY -> genericBuildFromBinaryConverter(consumer, genericType, parquetField);
        case FIXED_LEN_BYTE_ARRAY -> genericBuildFromByteArrayConverter(consumer, genericType, parquetField);
        case INT96 -> throw new RecordTypeConversionException(type + " deserialization not supported");
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
    }

    private static Converter genericBuildFromIntConverter(Consumer<Object> consumer, JavaType type, Type schemaType) {
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

        LogicalTypeAnnotation logicalTypeAnnotation = schemaType.getLogicalTypeAnnotation();
        PrimitiveTypeName primitive = schemaType.asPrimitiveType().getPrimitiveTypeName();
        if (primitive == PrimitiveTypeName.INT32 && logicalTypeAnnotation.equals(dateType()) && type.isLocalDate()) {
            return new LocalDateConverter(consumer);
        }
        if ((primitive == PrimitiveTypeName.INT32 || primitive == PrimitiveTypeName.INT64) && type.isLocalTime()
                && logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation time) {
            return new LocalTimeConverter(consumer, time.getUnit());
        }
        if (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation timeStamp) {
            if (type.isLocalDateTime()) {
                return new LocalDateTimeConverter(consumer, timeStamp.getUnit());
            } else if (type.isInstant()) {
                return new InstantConverter(consumer, timeStamp.getUnit());
            }
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " collection");
    }

    private static Converter genericBuildFromDecimalConverter(Consumer<Object> consumer, JavaType type,
            Type schemaType) {
        if (type.isFloat()) {
            return new ToFloatGenericConverter(consumer);
        }
        if (type.isDouble()) {
            return new ToDoubleGenericConverter(consumer);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " collection");
    }

    private static Converter genericBuildFromBooleanConverter(Consumer<Object> consumer, JavaType type,
            Type schemaType) {
        if (type.isBoolean()) {
            return new BooleanGenericConverter(consumer);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " collection");
    }

    private static Converter genericBuildFromBinaryConverter(Consumer<Object> consumer, JavaType type,
            Type schemaType) {
        var logicalType = schemaType.getLogicalTypeAnnotation();
        if (stringType().equals(logicalType)) {
            if (type.isString()) {
                return new StringConverter(consumer);
            }
            if (type.isEnum()) {
                return new EnumConverter(consumer, type.getJavaType());
            }
            throw new RecordTypeConversionException(type.getTypeName() + " not compatible with String field");
        }
        if (enumType().equals(logicalType)) {
            if (type.isString()) {
                return new StringConverter(consumer);
            }
            return new EnumConverter(consumer, type.getJavaType());
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " field");
    }

    private static Converter genericBuildFromByteArrayConverter(Consumer<Object> consumer, JavaType type,
            Type schemaType) {
        if (!uuidType().equals(schemaType.getLogicalTypeAnnotation())) {
            throw new RecordTypeConversionException(schemaType + " deserialization not supported");
        }
        if (type.isString()) {
            return new UuidToStringConverter(consumer);
        }
        if (type.isUuid()) {
            return new UuidToUuidConverter(consumer);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " field");
    }

    private static Converter buildFromLogicalTypeConverter(Consumer<Object> consumer, JavaType type,
            Type schemaType) {
        var logicalTypeAnnotation = schemaType.getLogicalTypeAnnotation();
        var primitiveType = schemaType.asPrimitiveType();
        if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimalType) {
            return new DecimalConverter(consumer, primitiveType.getPrimitiveTypeName(), decimalType.getScale());
        }
        return null;
    }

}
