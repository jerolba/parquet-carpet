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

import static com.jerolba.carpet.impl.read.LogicalTypeConverters.buildFromLogicalTypeConverter;

import java.lang.reflect.RecordComponent;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;
import com.jerolba.carpet.impl.read.converter.BooleanConverter;
import com.jerolba.carpet.impl.read.converter.ToByteConverter;
import com.jerolba.carpet.impl.read.converter.ToDoubleConverter;
import com.jerolba.carpet.impl.read.converter.ToFloatConverter;
import com.jerolba.carpet.impl.read.converter.ToIntegerConverter;
import com.jerolba.carpet.impl.read.converter.ToLongConverter;
import com.jerolba.carpet.impl.read.converter.ToShortConverter;

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

        Converter fromLogicalType = buildFromLogicalTypeConverter(type, parquetField,
                obj -> constructor.set(index, obj));
        if (fromLogicalType != null) {
            return fromLogicalType;
        }
        PrimitiveTypeName primitiveType = parquetField.asPrimitiveType().getPrimitiveTypeName();
        Converter converter = switch (primitiveType) {
        case INT32, INT64 -> buildFromIntConverter();
        case FLOAT, DOUBLE -> buildFromDecimalConverter();
        case BOOLEAN -> buildFromBooleanConverter();
        default -> throw new RecordTypeConversionException(primitiveType + " deserialization not supported");
        };
        if (converter == null) {
            throw new RecordTypeConversionException(
                    this.type.getTypeName() + " not compatible with " + recordComponent.getName() + " field");
        }
        return converter;
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
        return null;
    }

    private Converter buildFromDecimalConverter() {
        if (type.isFloat()) {
            return new ToFloatConverter(constructor, index);
        }
        if (type.isDouble()) {
            return new ToDoubleConverter(constructor, index);
        }
        return null;
    }

    private Converter buildFromBooleanConverter() {
        if (type.isBoolean()) {
            return new BooleanConverter(constructor, index);
        }
        return null;
    }

}
