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

import java.lang.reflect.RecordComponent;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;
import com.jerolba.carpet.impl.read.converter.BooleanConverter;
import com.jerolba.carpet.impl.read.converter.EnumConverter;
import com.jerolba.carpet.impl.read.converter.StringConverter;
import com.jerolba.carpet.impl.read.converter.ToByteConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatConverter;
import com.jerolba.carpet.impl.read.converter.ToIntegerConverter;
import com.jerolba.carpet.impl.read.converter.ToLongConverter;
import com.jerolba.carpet.impl.read.converter.ToShortConverter;
import com.jerolba.carpet.impl.read.converter.UuidToStringConverter;
import com.jerolba.carpet.impl.read.converter.UuidToUuidConverter;

class PrimitiveConverterFactory {

    public static Converter buildPrimitiveConverters(Type parquetField, ConstructorParams constructor,
            int index, RecordComponent recordComponent) {

        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        return switch (type) {
        case INT32, INT64 -> buildFromIntConverter(constructor, index, recordComponent);
        case FLOAT, DOUBLE -> buildFromDecimalConverter(constructor, index, recordComponent);
        case BOOLEAN -> buildFromBooleanConverter(constructor, index, recordComponent);
        case BINARY -> buildFromBinaryConverter(constructor, index, recordComponent, parquetField);
        case FIXED_LEN_BYTE_ARRAY -> buildFromByteArrayConverter(constructor, index, recordComponent, parquetField);
        case INT96 -> throw new RecordTypeConversionException(type + " deserialization not supported");
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
    }

    public static Converter buildFromIntConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        JavaType type = new JavaType(recordComponent);
        if (type.isInteger()) {
            return new ToIntegerConverter(constructor, index);
        }
        if (type.isLong()) {
            return new ToLongConverter(constructor, index);
        }
        if (type.isShort()) {
            return new ToShortConverter(constructor, index);
        }
        if (type.isByte()) {
            return new ToByteConverter(constructor, index);
        }
        if (type.isDouble()) {
            return new ToDoubleConverter(constructor, index);
        }
        if (type.isFloat()) {
            return new ToFloatConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromDecimalConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        JavaType type = new JavaType(recordComponent);
        if (type.isFloat()) {
            return new ToFloatConverter(constructor, index);
        }
        if (type.isDouble()) {
            return new ToDoubleConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromBooleanConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        JavaType type = new JavaType(recordComponent);
        if (type.isBoolean()) {
            return new BooleanConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromBinaryConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent, Type schemaType) {
        JavaType type = new JavaType(recordComponent);
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        if (logicalType.equals(stringType())) {
            if (type.isString()) {
                return new StringConverter(constructor, index);
            }
            if (type.isEnum()) {
                return new EnumConverter(constructor, index, recordComponent.getType());
            }
            throw new RecordTypeConversionException(type.getTypeName() + " not compatible with String field");
        }
        if (logicalType.equals(enumType())) {
            if (type.isString()) {
                return new StringConverter(constructor, index);
            }
            return new EnumConverter(constructor, index, recordComponent.getType());
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + recordComponent.getName() + " field");
    }

    private static Converter buildFromByteArrayConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent, Type parquetField) {

        LogicalTypeAnnotation logicalType = parquetField.getLogicalTypeAnnotation();
        if (!logicalType.equals(uuidType())) {
            throw new RecordTypeConversionException(parquetField + " deserialization not supported");
        }
        JavaType type = new JavaType(recordComponent);
        if (type.isString()) {
            return new UuidToStringConverter(constructor, index);
        }
        if (type.isUuid()) {
            return new UuidToUuidConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                parquetField + " deserialization not supported for type " + type.getTypeName());
    }

}
