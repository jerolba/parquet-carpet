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

import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeographyLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeometryLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.read.converter.BinaryConverter;
import com.jerolba.carpet.impl.read.converter.DecimalConverter;
import com.jerolba.carpet.impl.read.converter.EnumConverter;
import com.jerolba.carpet.impl.read.converter.GeometryConverter;
import com.jerolba.carpet.impl.read.converter.InstantConverter;
import com.jerolba.carpet.impl.read.converter.LocalDateConverter;
import com.jerolba.carpet.impl.read.converter.LocalDateTimeConverter;
import com.jerolba.carpet.impl.read.converter.LocalTimeConverter;
import com.jerolba.carpet.impl.read.converter.StringConverter;
import com.jerolba.carpet.impl.read.converter.ToByteConverter;
import com.jerolba.carpet.impl.read.converter.ToShortConverter;
import com.jerolba.carpet.impl.read.converter.UuidToStringConverter;
import com.jerolba.carpet.impl.read.converter.UuidToUuidConverter;

class LogicalTypeConverters {

    /**
     * Build a converter from the logical type annotation of a Parquet field.
     *
     * Creates a converter only if the logical type annotation is not null.
     * Otherwise, returns null.
     *
     * @param type         The Java type to convert to.
     * @param parquetField The Parquet field to convert from.
     * @param consumer     The consumer to receive the converted value.
     * @return A converter that converts the Parquet field to the Java type, or null
     *         if no conversion is possible.
     */
    public static Converter buildFromLogicalTypeConverter(JavaType type, Type parquetField, Consumer<Object> consumer) {
        var logicalTypeAnnotation = parquetField.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation == null) {
            return null;
        }
        var primitiveTypeName = parquetField.asPrimitiveType().getPrimitiveTypeName();
        if (logicalTypeAnnotation.equals(stringType())) {
            return converterForStringOrEnumType(type, consumer);
        } else if (logicalTypeAnnotation.equals(enumType())) {
            return converterForStringOrEnumType(type, consumer);
        } else if (logicalTypeAnnotation instanceof IntLogicalTypeAnnotation intType) {
            return converterForIntType(type, consumer, intType);
        } else if (logicalTypeAnnotation.equals(dateType())) {
            return converterForDateType(type, consumer, primitiveTypeName);
        } else if (logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation time) {
            return converterForTimestampType(type, consumer, primitiveTypeName, time);
        } else if (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation timeStamp) {
            return converterForTimestampType(type, consumer, primitiveTypeName, timeStamp);
        } else if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimalType) {
            return converterForDecimalType(type, consumer, primitiveTypeName, decimalType);
        } else if (logicalTypeAnnotation.equals(uuidType())) {
            return converterForUuidType(type, consumer, primitiveTypeName);
        } else if (logicalTypeAnnotation.equals(jsonType())) {
            return converterForJsonType(type, consumer);
        } else if (logicalTypeAnnotation.equals(bsonType())) {
            return converterForBsonType(type, consumer);
        } else if (logicalTypeAnnotation instanceof GeometryLogicalTypeAnnotation && type.isGeometry()) {
            return converterForGeometry(consumer);
        } else if (logicalTypeAnnotation instanceof GeographyLogicalTypeAnnotation && type.isGeometry()) {
            return converterForGeometry(consumer);
        }
        return null;
    }

    private static Converter converterForGeometry(Consumer<Object> consumer) {
        return new GeometryConverter(consumer);
    }

    private static Converter converterForStringOrEnumType(JavaType type, Consumer<Object> consumer) {
        if (type == null || type.isString()) {
            return new StringConverter(consumer);
        }
        if (type.isBinary()) {
            return new BinaryConverter(consumer);
        }
        if (type.isEnum()) {
            return new EnumConverter(consumer, type.getJavaType());
        }
        return null;
    }

    private static Converter converterForJsonType(JavaType type, Consumer<Object> consumer) {
        if (type == null || type.isString()) {
            return new StringConverter(consumer);
        }
        if (type.isBinary()) {
            return new BinaryConverter(consumer);
        }
        return null;
    }

    private static Converter converterForBsonType(JavaType type, Consumer<Object> consumer) {
        if (type == null || type.isBinary()) {
            return new BinaryConverter(consumer);
        }
        return null;
    }

    private static Converter converterForIntType(JavaType type, Consumer<Object> consumer,
            IntLogicalTypeAnnotation intType) {
        if (intType.getBitWidth() == 8 && (type == null || type.isByte())) {
            return new ToByteConverter(consumer);
        }
        if (intType.getBitWidth() == 16 && (type == null || type.isShort())) {
            return new ToShortConverter(consumer);
        }
        return null;
    }

    private static Converter converterForUuidType(JavaType type, Consumer<Object> consumer,
            PrimitiveTypeName primitiveTypeName) {
        if (primitiveTypeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            if (type == null || type.isUuid()) {
                return new UuidToUuidConverter(consumer);
            }
            if (type.isString()) {
                return new UuidToStringConverter(consumer);
            }
        }
        return null;
    }

    private static Converter converterForDateType(JavaType type, Consumer<Object> consumer,
            PrimitiveTypeName primitiveTypeName) {
        if (primitiveTypeName == PrimitiveTypeName.INT32) {
            if (type == null || type.isLocalDate()) {
                return new LocalDateConverter(consumer);
            }
        }
        return null;
    }

    private static Converter converterForTimestampType(JavaType type, Consumer<Object> consumer,
            PrimitiveTypeName primitiveTypeName, TimeLogicalTypeAnnotation time) {
        if (primitiveTypeName == PrimitiveTypeName.INT32 || primitiveTypeName == PrimitiveTypeName.INT64) {
            if (type == null || type.isLocalTime()) {
                return new LocalTimeConverter(consumer, time.getUnit());
            }
        }
        return null;
    }

    private static Converter converterForTimestampType(JavaType type, Consumer<Object> consumer,
            PrimitiveTypeName primitiveTypeName, TimestampLogicalTypeAnnotation timeStamp) {
        if (primitiveTypeName == PrimitiveTypeName.INT64) {
            if (type == null) {
                if (timeStamp.isAdjustedToUTC()) {
                    return new InstantConverter(consumer, timeStamp.getUnit());
                } else {
                    return new LocalDateTimeConverter(consumer, timeStamp.getUnit());
                }
            }
            if (type.isLocalDateTime()) {
                return new LocalDateTimeConverter(consumer, timeStamp.getUnit());
            } else if (type.isInstant()) {
                return new InstantConverter(consumer, timeStamp.getUnit());
            }
        }
        return null;
    }

    private static Converter converterForDecimalType(JavaType type, Consumer<Object> consumer,
            PrimitiveTypeName typeName, DecimalLogicalTypeAnnotation decimalType) {
        if (type == null || type.isBigDecimal()) {
            return new DecimalConverter(consumer, typeName, decimalType.getScale());
        }
        return null;
    }

}
