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

import static com.jerolba.carpet.impl.write.BigDecimalWrite.buildDecimalConfig;
import static com.jerolba.carpet.impl.write.GeometryWrite.geometryCosumer;
import static com.jerolba.carpet.impl.write.TimeWrite.instantCosumer;
import static com.jerolba.carpet.impl.write.TimeWrite.localDateTimeConsumer;
import static com.jerolba.carpet.impl.write.TimeWrite.localTimeConsumer;
import static com.jerolba.carpet.impl.write.UuidWrite.uuidToBinary;
import static com.jerolba.carpet.impl.write.VariantWriter.simpleVariantConsumer;

import java.time.LocalDate;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.model.BigDecimalType;
import com.jerolba.carpet.model.EnumType;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.ToBooleanFunction;
import com.jerolba.carpet.model.ToByteFunction;
import com.jerolba.carpet.model.ToFloatFunction;
import com.jerolba.carpet.model.ToShortFunction;
import com.jerolba.carpet.model.WriteRecordModelType;
import com.jerolba.carpet.model.WriteRecordModelType.PrimitiveJavaFieldInfo;

class ModelFieldsWriter {

    public static BiConsumer<RecordConsumer, Object> buildSimpleElementConsumer(FieldType fieldType,
            RecordConsumer recordConsumer, CarpetWriteConfiguration carpetConfiguration) {

        var type = new FieldTypeInspect(fieldType);
        if (type.isInteger()) {
            return (consumer, v) -> consumer.addInteger((Integer) v);
        }
        if (type.isString()) {
            return (consumer, v) -> consumer.addBinary(Binary.fromString((String) v));
        }
        if (type.isBoolean()) {
            return (consumer, v) -> consumer.addBoolean((Boolean) v);
        }
        if (type.isLong()) {
            return (consumer, v) -> consumer.addLong((Long) v);
        }
        if (type.isDouble()) {
            return (consumer, v) -> consumer.addDouble((Double) v);
        }
        if (type.isFloat()) {
            return (consumer, v) -> consumer.addFloat((Float) v);
        }
        if (type.isShort() || type.isByte()) {
            return (consumer, v) -> consumer.addInteger(((Number) v).intValue());
        }
        if (type.isBinary() || type.isBinaryGeospatial()) {
            return (consumer, v) -> consumer.addBinary((Binary) v);
        }
        if (fieldType instanceof EnumType enumType) {
            EnumsValues enumValues = new EnumsValues(enumType.enumClass());
            return (consumer, v) -> consumer.addBinary(enumValues.getValue(v));
        }
        if (type.isUuid()) {
            return (consumer, v) -> consumer.addBinary(uuidToBinary(v));
        }
        if (type.isLocalDate()) {
            return (consumer, v) -> consumer.addInteger((int) ((LocalDate) v).toEpochDay());
        }
        if (type.isLocalTime()) {
            return localTimeConsumer(carpetConfiguration.defaultTimeUnit());
        }
        if (type.isLocalDateTime()) {
            return localDateTimeConsumer(carpetConfiguration.defaultTimeUnit());
        }
        if (type.isInstant()) {
            return instantCosumer(carpetConfiguration.defaultTimeUnit());
        }
        if (type.isBigDecimal()) {
            var bigDecimalType = (BigDecimalType) fieldType;
            var config = buildDecimalConfig(bigDecimalType.precision(), bigDecimalType.scale(),
                    bigDecimalType.roundingMode(), carpetConfiguration.decimalConfig());
            return new BigDecimalWrite(config)::write;
        }
        if (type.isJtsGeometry()) {
            return geometryCosumer();
        }
        if (type.isVariant()) {
            return simpleVariantConsumer();
        }
        if (fieldType instanceof WriteRecordModelType<?> recordType) {
            var recordWriter = new WriteRecordModelWriter(recordConsumer, recordType, carpetConfiguration);
            return (consumer, v) -> {
                consumer.startGroup();
                recordWriter.write(v);
                consumer.endGroup();
            };
        }
        return null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Consumer<Object> buildPrimitiveJavaConsumer(PrimitiveJavaFieldInfo<?> primitiveField,
            int idx, RecordConsumer recordConsumer) {
        Object fieldAccessor = primitiveField.accessor();
        String parquetFieldName = primitiveField.parquetFieldName();
        var javaType = new FieldTypeInspect(primitiveField.fieldType());
        if (javaType.isBoolean()) {
            var accessor = (ToBooleanFunction) fieldAccessor;
            return new PrimitiveFieldWriter(recordConsumer, parquetFieldName, idx,
                    (rc, obj) -> rc.addBoolean(accessor.applyAsBoolean(obj)));
        } else if (javaType.isByte()) {
            var accessor = (ToByteFunction<Object>) fieldAccessor;
            return new PrimitiveFieldWriter(recordConsumer, parquetFieldName, idx,
                    (rc, obj) -> rc.addInteger(accessor.applyAsByte(obj)));
        } else if (javaType.isShort()) {
            var accessor = (ToShortFunction<Object>) fieldAccessor;
            return new PrimitiveFieldWriter(recordConsumer, parquetFieldName, idx,
                    (rc, obj) -> rc.addInteger(accessor.applyAsShort(obj)));
        } else if (javaType.isInteger()) {
            var accessor = (ToIntFunction<Object>) fieldAccessor;
            return new PrimitiveFieldWriter(recordConsumer, parquetFieldName, idx,
                    (rc, obj) -> rc.addInteger(accessor.applyAsInt(obj)));
        } else if (javaType.isLong()) {
            var accessor = (ToLongFunction<Object>) fieldAccessor;
            return new PrimitiveFieldWriter(recordConsumer, parquetFieldName, idx,
                    (rc, obj) -> rc.addLong(accessor.applyAsLong(obj)));
        } else if (javaType.isFloat()) {
            var accessor = (ToFloatFunction<Object>) fieldAccessor;
            return new PrimitiveFieldWriter(recordConsumer, parquetFieldName, idx,
                    (rc, obj) -> rc.addFloat(accessor.applyAsFloat(obj)));
        } else if (javaType.isDouble()) {
            var accessor = (ToDoubleFunction<Object>) fieldAccessor;
            return new PrimitiveFieldWriter(recordConsumer, parquetFieldName, idx,
                    (rc, obj) -> rc.addDouble(accessor.applyAsDouble(obj)));
        }
        return null;
    }

}
