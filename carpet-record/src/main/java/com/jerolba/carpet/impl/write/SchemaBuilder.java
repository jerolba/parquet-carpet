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
package com.jerolba.carpet.impl.write;

import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.Types.primitive;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.TimeUnit;

class SchemaBuilder {

    static PrimitiveType buildLocalTimeType(Repetition repetition, String name, TimeUnit timeUnit,
            boolean isAdjustedToUTC) {
        LogicalTypeAnnotation timeType = timeType(isAdjustedToUTC, toParquetTimeUnit(timeUnit));
        var typeName = switch (timeUnit) {
        case MILLIS -> PrimitiveTypeName.INT32;
        case MICROS, NANOS -> PrimitiveTypeName.INT64;
        };
        return primitive(typeName, repetition).as(timeType).named(name);
    }

    static PrimitiveType buildLocalDateType(Repetition repetition, String name) {
        return primitive(PrimitiveTypeName.INT32, repetition).as(dateType()).named(name);
    }

    static PrimitiveType buildLocalDateTimeType(Repetition repetition, String name, TimeUnit timeUnit) {
        var timeStampType = timestampType(false, toParquetTimeUnit(timeUnit));
        return primitive(PrimitiveTypeName.INT64, repetition).as(timeStampType).named(name);
    }

    static PrimitiveType buildInstantType(Repetition repetition, String name, TimeUnit timeUnit) {
        var timeStampType = timestampType(true, toParquetTimeUnit(timeUnit));
        return primitive(PrimitiveTypeName.INT64, repetition).as(timeStampType).named(name);
    }

    static Type buildDecimalTypeItem(Repetition repetition, String name, DecimalConfig decimalConfig) {
        if (decimalConfig == null || decimalConfig.scale() == null || decimalConfig.precision() == null) {
            throw new RecordTypeConversionException("If BigDecimal is used, a Default Decimal configuration "
                    + "must be provided in the setup of CarpetWriter builder, or BigDecimal fields must be "
                    + "annotated with @PrecisionScale");
        }
        var decimalType = decimalType(decimalConfig.scale(), decimalConfig.precision());
        if (decimalConfig.precision() <= 9) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(decimalType).named(name);
        }
        if (decimalConfig.precision() <= 18) {
            return primitive(PrimitiveTypeName.INT64, repetition).as(decimalType).named(name);
        }
        return primitive(PrimitiveTypeName.BINARY, repetition).as(decimalType).named(name);
    }

    static Type buildUuidType(Repetition repetition, String name) {
        return primitive(FIXED_LEN_BYTE_ARRAY, repetition).as(uuidType())
                .length(UUIDLogicalTypeAnnotation.BYTES).named(name);
    }

    static LogicalTypeAnnotation.TimeUnit toParquetTimeUnit(TimeUnit timeUnit) {
        return switch (timeUnit) {
        case MILLIS -> LogicalTypeAnnotation.TimeUnit.MILLIS;
        case MICROS -> LogicalTypeAnnotation.TimeUnit.MICROS;
        case NANOS -> LogicalTypeAnnotation.TimeUnit.NANOS;
        };
    }

}
