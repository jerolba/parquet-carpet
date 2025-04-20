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

import com.jerolba.carpet.model.BigDecimalType;
import com.jerolba.carpet.model.BinaryType;
import com.jerolba.carpet.model.BinaryType.BinaryLogicalType;
import com.jerolba.carpet.model.BooleanType;
import com.jerolba.carpet.model.ByteType;
import com.jerolba.carpet.model.DoubleType;
import com.jerolba.carpet.model.EnumType;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.FloatType;
import com.jerolba.carpet.model.InstantType;
import com.jerolba.carpet.model.IntegerType;
import com.jerolba.carpet.model.ListType;
import com.jerolba.carpet.model.LocalDateTimeType;
import com.jerolba.carpet.model.LocalDateType;
import com.jerolba.carpet.model.LocalTimeType;
import com.jerolba.carpet.model.LongType;
import com.jerolba.carpet.model.MapType;
import com.jerolba.carpet.model.SetType;
import com.jerolba.carpet.model.ShortType;
import com.jerolba.carpet.model.StringType;
import com.jerolba.carpet.model.StringType.StringLogicalType;
import com.jerolba.carpet.model.UuidType;
import com.jerolba.carpet.model.WriteRecordModelType;

class FieldTypeInspect {

    private final FieldType fieldType;

    public FieldTypeInspect(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public boolean isInteger() {
        return fieldType instanceof IntegerType;
    }

    public boolean isLong() {
        return fieldType instanceof LongType;
    }

    public boolean isShort() {
        return fieldType instanceof ShortType;
    }

    public boolean isByte() {
        return fieldType instanceof ByteType;
    }

    public boolean isDouble() {
        return fieldType instanceof DoubleType;
    }

    public boolean isFloat() {
        return fieldType instanceof FloatType;
    }

    public boolean isBoolean() {
        return fieldType instanceof BooleanType;
    }

    public boolean isString() {
        return fieldType instanceof StringType;
    }

    public boolean isBinary() {
        return fieldType instanceof BinaryType;
    }
	
    public boolean isEnum() {
        return fieldType instanceof EnumType;
    }

    public boolean isUuid() {
        return fieldType instanceof UuidType;
    }

    public boolean isBigDecimal() {
        return fieldType instanceof BigDecimalType;
    }

    public boolean isLocalDate() {
        return fieldType instanceof LocalDateType;
    }

    public boolean isLocalTime() {
        return fieldType instanceof LocalTimeType;
    }

    public boolean isLocalDateTime() {
        return fieldType instanceof LocalDateTimeType;
    }

    public boolean isInstant() {
        return fieldType instanceof InstantType;
    }

    public boolean isRecord() {
        return fieldType instanceof WriteRecordModelType;
    }

    public boolean isCollection() {
        return fieldType instanceof ListType || fieldType instanceof SetType;
    }

    public boolean isMap() {
        return fieldType instanceof MapType;
    }

    public BinaryLogicalType binaryLogicalType() {
        if (fieldType instanceof BinaryType binary) {
            return binary.logicalType();
        } else {
            throw new IllegalStateException("Field type is not a binary type");
        }
    }

    public StringLogicalType stringLogicalType() {
        if (fieldType instanceof StringType string) {
            return string.logicalType();
        } else {
            throw new IllegalStateException("Field type is not a string type");
        }
    }
}
