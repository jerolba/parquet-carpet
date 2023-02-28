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

import java.lang.reflect.RecordComponent;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;
import com.jerolba.carpet.impl.read.converter.BooleanConverter;
import com.jerolba.carpet.impl.read.converter.EnumConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToByteConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToIntegerConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToLongConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToShortConverter;
import com.jerolba.carpet.impl.read.converter.StringConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatConverter;

class PrimitiveConverterFactory {

    public static Converter buildPrimitiveConverters(Type parquetField, ConstructorParams constructor,
            int index, RecordComponent recordComponent) {

        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        return switch (type) {
        case INT32, INT64 -> buildFromIntConverter(constructor, index, recordComponent);
        case FLOAT, DOUBLE -> buildFromDecimalConverter(constructor, index, recordComponent);
        case BOOLEAN -> buildFromBooleanConverter(constructor, index, recordComponent);
        case BINARY -> buildFromBinaryConverter(constructor, index, recordComponent, parquetField);
        case INT96, FIXED_LEN_BYTE_ARRAY -> throw new RecordTypeConversionException(
                type + " deserialization not supported");
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
    }

    public static Converter buildFromIntConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new FromIntToIntegerConverter(constructor, index);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new FromIntToLongConverter(constructor, index);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new FromIntToShortConverter(constructor, index);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromIntToByteConverter(constructor, index);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new ToDoubleConverter(constructor, index);
        }
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new ToFloatConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromDecimalConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new ToFloatConverter(constructor, index);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new ToDoubleConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromBooleanConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanConverter(constructor, index);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

    public static Converter buildFromBinaryConverter(ConstructorParams constructor, int index,
            RecordComponent recordComponent, Type schemaType) {
        Class<?> type = recordComponent.getType();
        String typeName = type.getName();
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        if (logicalType.equals(stringType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringConverter(constructor, index);
            }
            throw new RecordTypeConversionException(typeName + " not compatible with String field");
        }
        if (logicalType.equals(enumType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringConverter(constructor, index);
            }
            return new EnumConverter(constructor, index, recordComponent.getType());
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + recordComponent.getName() + " field");
    }

}
