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

    private final RecordComponent recordComponent;
    private final ConstructorParams constructor;
    private final JavaType type;
    private final int index;

    PrimitiveConverterFactory(ConstructorParams constructor, int index, RecordComponent recordComponent) {
        this.recordComponent = recordComponent;
        this.constructor = constructor;
        this.index = index;
        this.type = new JavaType(recordComponent);
    }

    public Converter buildConverters(Type parquetField) {
        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        return switch (type) {
        case INT32, INT64 -> buildFromIntConverter();
        case FLOAT, DOUBLE -> buildFromDecimalConverter();
        case BOOLEAN -> buildFromBooleanConverter();
        case BINARY -> buildFromBinaryConverter(parquetField);
        case FIXED_LEN_BYTE_ARRAY -> buildFromByteArrayConverter(parquetField);
        case INT96 -> throw new RecordTypeConversionException(type + " deserialization not supported");
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
    }

    private Converter buildFromIntConverter() {
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

    private Converter buildFromDecimalConverter() {
        if (type.isFloat()) {
            return new ToFloatConverter(constructor, index);
        }
        if (type.isDouble()) {
            return new ToDoubleConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + recordComponent.getName() + " field");
    }

    private Converter buildFromBooleanConverter() {
        if (type.isBoolean()) {
            return new BooleanConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + recordComponent.getName() + " field");
    }

    private Converter buildFromBinaryConverter(Type parquetField) {
        LogicalTypeAnnotation logicalType = parquetField.getLogicalTypeAnnotation();
        if (stringType().equals(logicalType)) {
            if (type.isString()) {
                return new StringConverter(obj -> constructor.set(index, obj));
            }
            if (type.isEnum()) {
                return new EnumConverter(obj -> constructor.set(index, obj), recordComponent.getType());
            }
            throw new RecordTypeConversionException(type.getTypeName() + " not compatible with String field");
        }
        if (enumType().equals(logicalType)) {
            if (type.isString()) {
                return new StringConverter(obj -> constructor.set(index, obj));
            }
            return new EnumConverter(obj -> constructor.set(index, obj), recordComponent.getType());
        }
        throw new RecordTypeConversionException(
                type.getTypeName() + " not compatible with " + recordComponent.getName() + " field");
    }

    private Converter buildFromByteArrayConverter(Type parquetField) {
        if (!uuidType().equals(parquetField.getLogicalTypeAnnotation())) {
            throw new RecordTypeConversionException(parquetField + " deserialization not supported");
        }
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
