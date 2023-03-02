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

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.converter.BooleanGenericConverter;
import com.jerolba.carpet.impl.read.converter.EnumGenericConverter;
import com.jerolba.carpet.impl.read.converter.StringGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToByteGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToIntegerGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToLongGenericConverter;
import com.jerolba.carpet.impl.read.converter.ToShortGenericConverter;

class PrimitiveGenericConverterFactory {

    public static Converter buildPrimitiveGenericConverters(Type parquetField, Class<?> genericType,
            Consumer<Object> listConsumer) {
        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        return switch (type) {
        case INT32, INT64 -> genericBuildFromIntConverter(listConsumer, genericType);
        case FLOAT, DOUBLE -> genericBuildFromDecimalConverter(listConsumer, genericType);
        case BOOLEAN -> genericBuildFromBooleanConverter(listConsumer, genericType);
        case BINARY -> genericBuildFromBinaryConverter(listConsumer, genericType, parquetField);
        case INT96, FIXED_LEN_BYTE_ARRAY -> throw new RecordTypeConversionException(
                type + " deserialization not supported");
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
    }

    public static Converter genericBuildFromIntConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new ToIntegerGenericConverter(listConsumer);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new ToLongGenericConverter(listConsumer);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new ToShortGenericConverter(listConsumer);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new ToByteGenericConverter(listConsumer);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new ToDoubleGenericConverter(listConsumer);
        }
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new ToFloatGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromDecimalConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new ToFloatGenericConverter(listConsumer);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new ToDoubleGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromBooleanConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromBinaryConverter(Consumer<Object> listConsumer, Class<?> type,
            Type schemaType) {
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        String typeName = type.getName();
        if (logicalType.equals(stringType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringGenericConverter(listConsumer);
            }
            if (type.isEnum()) {
                return new EnumGenericConverter(listConsumer, type);
            }
            throw new RecordTypeConversionException(typeName + " not compatible with String field");
        }
        if (logicalType.equals(enumType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringGenericConverter(listConsumer);
            }
            return new EnumGenericConverter(listConsumer, type);

        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + schemaType.getName() + " field");
    }

}
