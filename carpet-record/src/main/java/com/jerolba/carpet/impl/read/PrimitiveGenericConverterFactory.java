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

import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.read.converter.BooleanGenericConverter;
import com.jerolba.carpet.impl.read.converter.EnumGenericConverter;
import com.jerolba.carpet.impl.read.converter.StringGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToByteGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToIntegerGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToLongGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToShortGenericConverter;
import com.jerolba.carpet.impl.read.converter.UuidToStringGenericConverter;
import com.jerolba.carpet.impl.read.converter.UuidToUuidGenericConverter;

class PrimitiveGenericConverterFactory {

    public static Converter buildPrimitiveGenericConverters(Type parquetField, Class<?> genericJavaType,
            Consumer<Object> listConsumer) {
        JavaType genericType = new JavaType(genericJavaType);
        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        return switch (type) {
        case INT32, INT64 -> genericBuildFromIntConverter(listConsumer, genericType, parquetField);
        case FLOAT, DOUBLE -> genericBuildFromDecimalConverter(listConsumer, genericType, parquetField);
        case BOOLEAN -> genericBuildFromBooleanConverter(listConsumer, genericType, parquetField);
        case BINARY -> genericBuildFromBinaryConverter(listConsumer, genericType, parquetField);
        case FIXED_LEN_BYTE_ARRAY -> genericBuildFromByteArrayConverter(listConsumer, genericType, parquetField);
        case INT96 -> throw new RecordTypeConversionException(type + " deserialization not supported");
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
    }

    private static Converter genericBuildFromIntConverter(Consumer<Object> listConsumer, JavaType type,
            Type schemaType) {
        if (type.isInteger()) {
            return new ToIntegerGenericConverter(listConsumer);
        }
        if (type.isLong()) {
            return new ToLongGenericConverter(listConsumer);
        }
        if (type.isShort()) {
            return new ToShortGenericConverter(listConsumer);
        }
        if (type.isByte()) {
            return new ToByteGenericConverter(listConsumer);
        }
        if (type.isDouble()) {
            return new ToDoubleGenericConverter(listConsumer);
        }
        if (type.isFloat()) {
            return new ToFloatGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " collection");
    }

    private static Converter genericBuildFromDecimalConverter(Consumer<Object> listConsumer, JavaType type,
            Type schemaType) {
        if (type.isFloat()) {
            return new ToFloatGenericConverter(listConsumer);
        }
        if (type.isDouble()) {
            return new ToDoubleGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " collection");
    }

    private static Converter genericBuildFromBooleanConverter(Consumer<Object> listConsumer, JavaType type,
            Type schemaType) {
        if (type.isBoolean()) {
            return new BooleanGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " collection");
    }

    private static Converter genericBuildFromBinaryConverter(Consumer<Object> listConsumer, JavaType type,
            Type schemaType) {
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        if (logicalType.equals(stringType())) {
            if (type.isString()) {
                return new StringGenericConverter(listConsumer);
            }
            if (type.isEnum()) {
                return new EnumGenericConverter(listConsumer, type.getJavaType());
            }
            throw new RecordTypeConversionException(type.getTypeName() + " not compatible with String field");
        }
        if (logicalType.equals(enumType())) {
            if (type.isString()) {
                return new StringGenericConverter(listConsumer);
            }
            return new EnumGenericConverter(listConsumer, type.getJavaType());
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " field");
    }

    private static Converter genericBuildFromByteArrayConverter(Consumer<Object> listConsumer, JavaType type,
            Type schemaType) {
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        if (!logicalType.equals(uuidType())) {
            throw new RecordTypeConversionException(schemaType + " deserialization not supported");
        }
        if (type.isString()) {
            return new UuidToStringGenericConverter(listConsumer);
        }
        if (type.isUuid()) {
            return new UuidToUuidGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + schemaType.getName() + " field");
    }

}
