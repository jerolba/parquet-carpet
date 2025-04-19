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
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.read.converter.BinaryConverter;
import com.jerolba.carpet.impl.read.converter.DecimalConverter;
import com.jerolba.carpet.impl.read.converter.EnumConverter;
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

    public static Converter buildFromLogicalTypeConverter(JavaType type, Type parquetField,
            Consumer<Object> consumer) {
        var logicalTypeAnnotation = parquetField.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation == null) {
            return null;
        }
        var primitiveType = parquetField.asPrimitiveType();
        var primitiveTypeName = parquetField.asPrimitiveType().getPrimitiveTypeName();

        if (logicalTypeAnnotation.equals(stringType())) {
            if (type == null || type.isString()) {
                return new StringConverter(consumer);
            }
            if (type.isBinary()) {
                return new BinaryConverter(consumer);
            }
            if (type.isEnum()) {
                return new EnumConverter(consumer, type.getJavaType());
            }
        }

        if (logicalTypeAnnotation.equals(enumType())) {
            if (type == null || type.isString()) {
                return new StringConverter(consumer);
            }
            if (type.isEnum()) {
                return new EnumConverter(consumer, type.getJavaType());
            }
        }

        if (logicalTypeAnnotation instanceof IntLogicalTypeAnnotation intType) {
            if (intType.getBitWidth() == 8 && (type == null || type.isByte())) {
                return new ToByteConverter(consumer);
            }
            if (intType.getBitWidth() == 16 && (type == null || type.isShort())) {
                return new ToShortConverter(consumer);
            }
        }

        if (logicalTypeAnnotation.equals(uuidType())
                && primitiveTypeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            if (type == null || type.isUuid()) {
                return new UuidToUuidConverter(consumer);
            }
            if (type.isString()) {
                return new UuidToStringConverter(consumer);
            }
        }

        if (logicalTypeAnnotation.equals(dateType())
                && primitiveTypeName == PrimitiveTypeName.INT32
                && (type == null || type.isLocalDate())) {
            return new LocalDateConverter(consumer);
        }

        if (logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation time
                && (primitiveTypeName == PrimitiveTypeName.INT32 || primitiveTypeName == PrimitiveTypeName.INT64)
                && (type == null || type.isLocalTime())) {
            return new LocalTimeConverter(consumer, time.getUnit());
        }

        if (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation timeStamp
                && primitiveTypeName == PrimitiveTypeName.INT64) {
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

        if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimalType
                && (type == null || type.isBigDecimal())) {
            return new DecimalConverter(consumer, primitiveType.getPrimitiveTypeName(), decimalType.getScale());
        }

        return null;
    }

}
